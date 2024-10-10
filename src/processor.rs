//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::edge::Edge;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::{WorkflowBuilder, WorkflowId};
use log::{debug, info, trace};
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task::{spawn, spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;

/// プロセッサーエラー
#[derive(Debug, Error, PartialEq)]
pub enum ProcessorError {
    /// コンテナエラー
    #[error("Container error")]
    ContainerError(#[from] crate::container::ContainerError),

    /// オペレーターエラー
    #[error("Operator error")]
    OperatorError(#[from] crate::operator::OperatorError),

    /// エッジの数が不正
    #[error("Some node has invalid edge count")]
    InvalidEdgeCount,

    /// まだ終了していないエッジを取得しようとした
    #[error("Not finished edge")]
    NotFinishedEdge,
}

type Handler<T> = (JoinHandle<Result<T, ProcessorError>>, ExecutorId);

/// ハンドラ管理
struct Handlers<T> {
    handles: Vec<Option<Handler<T>>>,
    retains: VecDeque<usize>,
}
impl<T> Handlers<T> {
    fn new(n: usize) -> Self {
        let mut handles: Vec<Option<Handler<T>>> = Vec::with_capacity(n);
        let mut retains = VecDeque::new();
        for i in 0..n {
            retains.push_back(i);
            handles.push(None);
        }
        Self { handles, retains }
    }

    fn push(&mut self, handle: JoinHandle<Result<T, ProcessorError>>, exec_id: ExecutorId) {
        if let Some(retain) = self.retains.pop_front() {
            self.handles[retain] = Some((handle, exec_id));
            #[cfg(feature = "dev")]
            debug!(
                "{:?} tasks are running",
                self.handles.len() - self.retains.len()
            );
        }
    }

    fn iter(
        &mut self,
    ) -> impl Iterator<
        Item = (
            usize,
            &mut (JoinHandle<Result<T, ProcessorError>>, ExecutorId),
        ),
    > {
        self.handles
            .iter_mut()
            .enumerate()
            .filter(|(_, handle)| handle.is_some())
            .map(|(key, handle)| (key, handle.as_mut().unwrap()))
    }

    fn remove(&mut self, key: usize) {
        self.retains.push_back(key);
        self.handles[key] = None;
        #[cfg(feature = "dev")]
        debug!(
            "{:?} tasks are running",
            self.handles.len() - self.retains.len()
        );
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
        let mut handlers = Handlers::new(n);
        let op = Operator::new(self.workflow);
        #[cfg(feature = "dev")]
        let mut time: std::collections::HashMap<
            (WorkflowId, ExecutorId),
            std::time::Instant,
        > = std::collections::HashMap::new();
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
                    trace!("Get nodes enabled to run");
                }
                while !handlers.is_full() {
                    let (handle, exec_id) = if let Some((node, exec_id)) = op.get_next_node().await
                    {
                        #[cfg(feature = "dev")]
                        {
                            let wf_id = op.get_wf_id(exec_id).await.unwrap();
                            let key = (wf_id, exec_id);
                            let _ = time.entry(key).or_insert_with(std::time::Instant::now);
                        }
                        let op_clone = op.clone();
                        debug!("Task is started: {:?}", node.name());
                        if node.is_blocking() {
                            let rt_handle = Handle::current();
                            (
                                spawn_blocking(move || {
                                    rt_handle.block_on(async {
                                        node.run(&op_clone, exec_id).await;
                                    });
                                    Ok(node.name())
                                }),
                                exec_id,
                            )
                        } else {
                            #[cfg(not(feature = "dev"))]
                            {
                                (
                                    spawn(async move {
                                        node.run(&op_clone, exec_id).await;
                                        Ok(node.name())
                                    }),
                                    exec_id,
                                )
                            }
                            #[cfg(feature = "dev")]
                            (
                                tokio::task::Builder::new()
                                    .name(node.name())
                                    .spawn(async move {
                                        node.run(&op_clone, exec_id).await;
                                        Ok(node.name())
                                    })
                                    .unwrap(),
                                exec_id,
                            )
                        }
                    } else {
                        break;
                    };
                    handlers.push(handle, exec_id);
                }

                let mut finished = Vec::new();
                if handlers.is_running() {
                    trace!("Check running tasks");
                }
                for (key, (handle, exec_id)) in handlers.iter() {
                    let is_finished = op.is_finished(*exec_id).await;
                    if is_finished {
                        #[cfg(feature = "dev")]
                        {
                            let wf_id = op.get_wf_id(*exec_id).await.unwrap();
                            if let Some(inst) = time.remove(&(wf_id, *exec_id)) {
                                log::info!(
                                    "{:?}({:?}) is finished in {:?}",
                                    wf_id,
                                    exec_id,
                                    inst.elapsed()
                                );
                            }
                        }
                        let res = handle.await.unwrap().unwrap();
                        debug!("Task is finished: {:?}", res);
                        if op.check_all_containers_taken(*exec_id).await {
                            op.finish_workflow_by_execute_id(*exec_id).await;
                        }
                        finished.push(key);
                    }
                }
                for key in finished {
                    handlers.remove(key);
                }
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
    /// * `wf_id` - ワークフローID
    pub async fn start(&self, wf_id: WorkflowId) -> ExecutorId {
        info!("Start workflow: {:?}", wf_id);
        let id = ExecutorId::new();
        self.op.start_workflow(id, wf_id, None).await;
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

    /// データの取得
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    pub async fn take<T: 'static + Send + Sync>(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
    ) -> Result<T, ProcessorError> {
        let mut cons = self.op.get_container(&vec![edge], exec_id).await;
        if cons.len() == 1 {
            let mut con = cons.pop_front().unwrap();
            assert_eq!(cons.len(), 0);
            Ok(con.take()?)
        } else {
            Err(ProcessorError::NotFinishedEdge)
        }
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

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id);
        handlers.push(handle1, exec_id);
        handlers.push(handle2, exec_id);

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

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id);
        handlers.push(handle1, exec_id);
        handlers.push(handle2, exec_id);
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

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id);
        handlers.push(handle1, exec_id);
        handlers.push(handle2, exec_id);
        handlers.remove(1);
        handlers.push(handle3, exec_id);

        let mut iter_index = Vec::new();

        for (key, (handle, _)) in handlers.iter() {
            let res = handle.await.unwrap().unwrap();
            iter_index.push((key, res));
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
