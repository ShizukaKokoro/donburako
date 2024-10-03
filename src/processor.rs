//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::node::Edge;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::WorkflowBuilder;
use log::{debug, info};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task::{spawn, spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;

/// プロセッサーエラー
#[derive(Debug, Error, PartialEq)]
pub enum ProcessorError {
    /// オペレーターエラー
    #[error("Operator error")]
    OperatorError(#[from] crate::operator::OperatorError),

    /// ワークフローの開始に失敗
    #[error("Failed to start workflow")]
    FailedToStartWorkflow,
}

/// ハンドラ管理
struct Handlers<T> {
    handles: Vec<Option<JoinHandle<Result<T, ProcessorError>>>>,
    retains: VecDeque<usize>,
}
impl<T> Handlers<T> {
    fn new(n: usize) -> Self {
        let mut handles: Vec<Option<JoinHandle<Result<T, ProcessorError>>>> = Vec::with_capacity(n);
        let mut retains = VecDeque::new();
        for i in 0..n {
            retains.push_back(i);
            handles.push(None);
        }
        Self { handles, retains }
    }

    fn push(&mut self, handle: JoinHandle<Result<T, ProcessorError>>) {
        if let Some(retain) = self.retains.pop_front() {
            self.handles[retain] = Some(handle);
        }
    }

    fn iter(
        &mut self,
    ) -> impl Iterator<Item = (usize, &mut JoinHandle<Result<T, ProcessorError>>)> {
        self.handles
            .iter_mut()
            .enumerate()
            .filter(|(_, handle)| handle.is_some())
            .map(|(key, handle)| (key, handle.as_mut().unwrap()))
    }

    fn remove(&mut self, key: usize) {
        self.retains.push_back(key);
        self.handles[key] = None;
    }

    fn is_full(&self) -> bool {
        self.retains.is_empty()
    }

    fn is_running(&self) -> bool {
        self.retains.len() != self.handles.len()
    }
}

/// プロセッサービルダー
#[derive(Default)]
pub struct ProcessorBuilder {
    workflow: Vec<WorkflowBuilder>,
    check_time: Duration,
    loop_wait_time: Duration,
}
impl ProcessorBuilder {
    /// ワークフローの追加
    ///
    /// # Arguments
    ///
    /// * `wf` - ワークフロービルダー
    pub fn add_workflow(self, wf: WorkflowBuilder) -> Self {
        let mut wfs = self.workflow;
        wfs.push(wf);
        Self {
            workflow: wfs,
            check_time: Duration::from_millis(10),
            loop_wait_time: Duration::from_millis(1),
        }
    }

    /// チェック時間の設定
    ///
    /// # Arguments
    ///
    /// * `time` - チェック時間
    pub fn set_check_time_millis(self, time: u64) -> Self {
        Self {
            check_time: Duration::from_millis(time),
            ..self
        }
    }

    /// ループ待機時間の設定
    ///
    /// # Arguments
    ///
    /// * `time` - ループ待機時間
    pub fn set_loop_wait_time_millis(self, time: u64) -> Self {
        Self {
            loop_wait_time: Duration::from_millis(time),
            ..self
        }
    }

    /// ビルド
    pub fn build(self, n: usize) -> Result<Processor, ProcessorError> {
        debug!("Start building processor");
        let mut handlers: Handlers<()> = Handlers::new(n);
        let op = Operator::new(self.workflow);
        let op_clone = op.clone();
        debug!("End setting up processor: capacity={}", n);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handle = spawn(async move {
            loop {
                if cancel_clone.is_cancelled() {
                    break;
                }

                if op.has_executable_node().await {
                    debug!("Get nodes enabled to run");
                }
                while !handlers.is_full() {
                    let handle = if let Some((node, exec_id)) = op.get_next_node().await {
                        let op_clone = op.clone();
                        if node.is_blocking() {
                            spawn(async move {
                                node.run(&op_clone, exec_id).await;
                                Ok(())
                            })
                        } else {
                            let rt_handle = Handle::current();
                            spawn_blocking(move || {
                                rt_handle.block_on(async {
                                    node.run(&op_clone, exec_id).await;
                                });
                                Ok(())
                            })
                        }
                    } else {
                        break;
                    };
                    handlers.push(handle);
                }

                let mut finished = Vec::new();
                if handlers.is_running() {
                    debug!("Check running tasks");
                }
                for (key, handle) in handlers.iter() {
                    tokio::select! {
                            // タスクが終了した場合
                            _ = handle => {
                                debug!("Task is done(at {} / {})", key, n);
                                finished.push(key);
                            }
                            // タスクが終了していない場合
                            _ = tokio::time::sleep(self.check_time) => {}
                    }
                }
                for key in finished {
                    handlers.remove(key);
                }
                tokio::time::sleep(self.loop_wait_time).await;
            }
            Ok(())
        });

        Ok(Processor {
            op: op_clone,
            handle,
            cancel,
        })
    }
}

/// プロセッサー
pub struct Processor {
    op: Operator,
    handle: JoinHandle<Result<(), ProcessorError>>,
    cancel: CancellationToken,
}
impl Processor {
    /// ワークフローの開始
    ///
    /// # Arguments
    ///
    /// * `index` - ワークフローのインデックス
    pub async fn start(&self, index: usize) -> ExecutorId {
        info!("Start workflow: {:?}", index);
        let id = ExecutorId::new();
        self.op.start_workflow(id, index).await;
        id
    }

    /// データを設定
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    /// * `data` - データ
    pub async fn store<T: 'static + Send + Sync>(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), ProcessorError> {
        self.op.add_new_container(edge, exec_id, data).await?;
        Ok(())
    }

    /// プロセッサーの停止
    pub fn stop(&self) {
        info!("Stop processor");
        self.cancel.cancel();
    }

    /// プロセッサーの待機
    pub async fn wait(self) -> Result<(), ProcessorError> {
        info!("Wait processor");
        self.handle.await.unwrap()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_handlers() {
        let mut handlers = Handlers::new(3);
        let handle0 = spawn(async { Ok(()) });
        let handle1 = spawn(async { Ok(()) });
        let handle2 = spawn(async { Ok(()) });

        handlers.push(handle0);
        handlers.push(handle1);
        handlers.push(handle2);

        assert_eq!(handlers.handles.len(), 3);

        handlers.remove(0);
        handlers.remove(1);
        handlers.remove(2);

        assert_eq!(handlers.handles.len(), 3);
    }

    #[tokio::test]
    async fn test_handlers_iter_filtered() {
        let mut handlers = Handlers::new(3);
        let handle0 = spawn(async { Ok(0) });
        let handle1 = spawn(async { Ok(1) });
        let handle2 = spawn(async { Ok(2) });

        handlers.push(handle0);
        handlers.push(handle1);
        handlers.push(handle2);
        handlers.remove(1);

        let mut iter_key = Vec::new();
        for (key, _) in handlers.iter() {
            iter_key.push(key);
        }

        assert_eq!(iter_key, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_handlers_remove() {
        let mut handlers = Handlers::new(3);
        let handle0 = spawn(async { Ok(0) });
        let handle1 = spawn(async { Ok(1) });
        let handle2 = spawn(async { Ok(2) });
        let handle3 = spawn(async { Ok(3) });

        handlers.push(handle0);
        handlers.push(handle1);
        handlers.push(handle2);
        handlers.remove(1);
        handlers.push(handle3);

        let mut iter_index = Vec::new();

        for (key, handle) in handlers.iter() {
            tokio::select! {
                    res = handle => {
                        iter_index.push((key,res.unwrap().unwrap()));
                }
            }
        }
        assert_eq!(iter_index, vec![(0, 0), (1, 3), (2, 2)]);

        handlers.remove(0);
        handlers.remove(1);
        handlers.remove(2);
        assert!(handlers.handles[0].is_none());
        assert!(handlers.handles[1].is_none());
        assert!(handlers.handles[2].is_none());
        assert_eq!(handlers.retains.len(), 3);
    }
}
