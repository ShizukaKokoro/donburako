//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::operator::{ExecutorId, Operator};
use crate::workflow::WorkflowBuilder;
use log::{debug, info};
use std::collections::VecDeque;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::{spawn, spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;

/// プロセッサーエラー
#[derive(Debug, Error, PartialEq)]
pub enum ProcessorError {
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
}

/// 開始メッセージ
pub struct StartMessage {
    index: usize,
    exec_id: ExecutorId,
}
impl StartMessage {
    fn new(index: usize) -> (Self, ExecutorId) {
        let exec_id = ExecutorId::new();
        (Self { index, exec_id }, exec_id)
    }
}

/// プロセッサービルダー
#[derive(Default)]
pub struct ProcessorBuilder {
    workflow: Vec<WorkflowBuilder>,
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
        Self { workflow: wfs }
    }

    /// ビルド
    pub fn build(self, n: usize) -> Result<Processor, ProcessorError> {
        debug!("Start building processor");
        let mut handlers: Handlers<()> = Handlers::new(n);
        let op = Operator::new(self.workflow);
        let op_clone = op.clone();
        debug!("End setting up processor: capacity={}", n);

        let (tx, mut rx): (mpsc::Sender<StartMessage>, mpsc::Receiver<StartMessage>) =
            mpsc::channel(16);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handle = spawn(async move {
            loop {
                if cancel_clone.is_cancelled() {
                    break;
                }

                while let Ok(message) = rx.try_recv() {
                    debug!(
                        "Start workflow: {:?} with id: {:?}",
                        message.index, message.exec_id
                    );
                    op.start_workflow(message.exec_id, message.index).await;
                }

                debug!("Get nodes enabled to run");
                todo!(); // TODO: ワークフローの次に実行可能なノードを取得し、タスクのスレッドを handles に追加する

                let mut finished = Vec::new();
                debug!("Check running tasks");
                for (key, handle) in handlers.iter() {
                    tokio::select! {
                            // タスクが終了した場合
                            res = handle => {
                                debug!("Task is done(at {} / {})", key, n);
                                todo!(); // TODO: タスクの終了処理を行う
                                finished.push(key);
                            }
                            // タスクが終了していない場合
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {}
                    }
                }
                for key in finished {
                    handlers.remove(key);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
            Ok(())
        });

        Ok(Processor {
            op: op_clone,
            handle,
            tx,
            cancel,
        })
    }
}

/// プロセッサー
pub struct Processor {
    op: Operator,
    handle: JoinHandle<Result<(), ProcessorError>>,
    tx: mpsc::Sender<StartMessage>,
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
        let (message, id) = StartMessage::new(index);
        self.tx.send(message).await.unwrap();
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
