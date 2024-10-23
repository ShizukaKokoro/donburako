//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::edge::Edge;
use crate::node::NodeError;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::{WorkflowBuilder, WorkflowId};
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Mutex};
use tokio::task::{spawn, spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

/// プロセッサーエラー
#[derive(Debug, Error)]
pub enum ProcessorError {
    /// コンテナエラー
    #[error("Container error")]
    ContainerError(#[from] crate::container::ContainerError),

    /// オペレーターエラー
    #[error("Operator error")]
    OperatorError(#[from] crate::operator::OperatorError),

    /// Join エラー
    #[error("Join error")]
    JoinError(#[from] tokio::task::JoinError),

    /// エッジの数が不正
    #[error("Some node has invalid edge count")]
    InvalidEdgeCount,

    /// まだ終了していないエッジを取得しようとした
    #[error("Not finished edge")]
    NotFinishedEdge,
}

type Handler<T> = (JoinHandle<T>, ExecutorId, oneshot::Receiver<()>);

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

    fn push(&mut self, handle: JoinHandle<T>, exec_id: ExecutorId, node_rx: oneshot::Receiver<()>) {
        if let Some(retain) = self.retains.pop_front() {
            self.handles[retain] = Some((handle, exec_id, node_rx));
            #[cfg(feature = "dev")]
            debug!(
                "{:?} tasks are running",
                self.handles.len() - self.retains.len()
            );
        }
    }

    fn iter(&mut self) -> impl Iterator<Item = (usize, &mut Handler<T>)> {
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
    workflow: Vec<(WorkflowId, WorkflowBuilder)>,
}
impl ProcessorBuilder {
    /// ワークフローの追加
    ///
    /// # Arguments
    ///
    /// * `wf` - ワークフロービルダー
    pub fn add_workflow(self, wf_id: WorkflowId, wf: WorkflowBuilder) -> Self {
        let mut wfs = self.workflow;
        wfs.push((wf_id, wf));
        Self { workflow: wfs }
    }

    /// ビルド
    pub fn build(self, n: usize) -> Processor {
        debug!("Start building processor");
        let mut handlers: Handlers<Result<&str, NodeError>> = Handlers::new(n);
        let op = Arc::new(Mutex::new(Operator::new(self.workflow)));
        let op_clone = op.clone();
        debug!("End setting up processor: capacity={}", n);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let shutdown_token = CancellationToken::new();
        let shutdown_clone = shutdown_token.clone();
        let handle = spawn(async move {
            let mut flag = true;
            loop {
                if cancel_clone.is_cancelled() || !flag {
                    break;
                }
                if shutdown_clone.is_cancelled() && op.lock().await.check_all_finished().await {
                    flag = false;
                }

                let mut op_lock = op.lock().await;
                if op_lock.has_executable_node().await {
                    trace!("Get nodes enabled to run");
                }
                while !handlers.is_full() {
                    let (handle, exec_id, node_rx) = if let Some((node, exec_id)) =
                        op_lock.get_next_node().await
                    {
                        #[cfg(feature = "dev")]
                        op_lock.start_timer(exec_id).await;
                        let op_clone = op.clone();
                        debug!("Task is started: {:?}", node.name());
                        let (node_tx, node_rx) = oneshot::channel();
                        if node.is_blocking() {
                            let rt_handle = Handle::current();
                            (
                                spawn_blocking(move || {
                                    rt_handle
                                        .block_on(async { node.run(op_clone, exec_id).await })?;
                                    let _ = node_tx.send(());
                                    Ok(node.name())
                                }),
                                exec_id,
                                node_rx,
                            )
                        } else {
                            #[cfg(not(feature = "dev"))]
                            {
                                (
                                    spawn(async move {
                                        node.run(op_clone, exec_id).await?;
                                        let _ = node_tx.send(());
                                        Ok(node.name())
                                    }),
                                    exec_id,
                                    node_rx,
                                )
                            }
                            #[cfg(feature = "dev")]
                            (
                                tokio::task::Builder::new()
                                    .name(node.name())
                                    .spawn(async move {
                                        node.run(op_clone, exec_id).await?;
                                        let _ = node_tx.send(());
                                        Ok(node.name())
                                    })
                                    .unwrap(),
                                exec_id,
                                node_rx,
                            )
                        }
                    } else {
                        break;
                    };
                    handlers.push(handle, exec_id, node_rx);
                }
                drop(op_lock);

                let mut finished = Vec::new();
                if handlers.is_running() {
                    trace!("Check running tasks");
                }
                for (key, (handle, exec_id, node_rx)) in handlers.iter() {
                    if op.lock().await.is_finished(*exec_id).await {
                        finished.push(key);
                        let mut op_lock = op.lock().await;
                        #[cfg(feature = "dev")]
                        op_lock.stop_timer(*exec_id).await;
                        if op_lock.check_all_containers_taken(*exec_id).await {
                            op_lock.finish_workflow_by_execute_id(*exec_id).await;
                        }
                        drop(op_lock);
                    } else if node_rx.try_recv().is_ok() {
                        let result = handle.await?;
                        match result {
                            Ok(name) => {
                                debug!("Task is finished: {:?}", name);
                            }
                            Err(e) => {
                                warn!("Task is failed: {:?}", e);
                            }
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

        Processor {
            op: op_clone,
            handle,
            cancel,
            shutdown_token,
        }
    }
}

/// プロセッサー
pub struct Processor {
    op: Arc<Mutex<Operator>>,
    handle: JoinHandle<Result<(), ProcessorError>>,
    cancel: CancellationToken,
    shutdown_token: CancellationToken,
}
impl Processor {
    /// ワークフローの開始
    ///
    /// # Arguments
    ///
    /// * `wf_id` - ワークフローID
    pub async fn start(&self, wf_id: WorkflowId) -> (ExecutorId, oneshot::Receiver<()>) {
        let id = ExecutorId::new();
        let wf_rx = self.op.lock().await.start_workflow(id, wf_id).await;
        (id, wf_rx)
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
        self.op
            .lock()
            .await
            .add_new_container(edge, exec_id, data)
            .await?;
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
        let mut cons = self.op.lock().await.get_container(&[edge], exec_id).await?;
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

    /// プロセッサーのシャットダウン
    pub fn shutdown(&self) {
        info!("Shutdown processor");
        self.shutdown_token.cancel();
    }

    /// プロセッサーの待機
    pub async fn wait(self) -> Result<(), ProcessorError> {
        info!("Wait processor");
        self.handle.await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_handlers() {
        let mut handlers = Handlers::new(3);
        let handle0 = spawn(async {});
        let handle1 = spawn(async {});
        let handle2 = spawn(async {});
        let (_, node_rx0) = oneshot::channel();
        let (_, node_rx1) = oneshot::channel();
        let (_, node_rx2) = oneshot::channel();

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id, node_rx0);
        handlers.push(handle1, exec_id, node_rx1);
        handlers.push(handle2, exec_id, node_rx2);

        assert_eq!(handlers.handles.len(), 3);

        handlers.remove(0);
        handlers.remove(1);
        handlers.remove(2);

        assert_eq!(handlers.handles.len(), 3);
    }

    #[tokio::test]
    async fn test_handlers_iter_filtered() {
        let mut handlers = Handlers::new(3);
        let handle0 = spawn(async { 0 });
        let handle1 = spawn(async { 1 });
        let handle2 = spawn(async { 2 });
        let (_, node_rx0) = oneshot::channel();
        let (_, node_rx1) = oneshot::channel();
        let (_, node_rx2) = oneshot::channel();

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id, node_rx0);
        handlers.push(handle1, exec_id, node_rx1);
        handlers.push(handle2, exec_id, node_rx2);
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
        let handle0 = spawn(async { 0 });
        let handle1 = spawn(async { 1 });
        let handle2 = spawn(async { 2 });
        let handle3 = spawn(async { 3 });
        let (_, node_rx0) = oneshot::channel();
        let (_, node_rx1) = oneshot::channel();
        let (_, node_rx2) = oneshot::channel();
        let (_, node_rx3) = oneshot::channel();

        let exec_id = ExecutorId::new();
        handlers.push(handle0, exec_id, node_rx0);
        handlers.push(handle1, exec_id, node_rx1);
        handlers.push(handle2, exec_id, node_rx2);
        handlers.remove(1);
        handlers.push(handle3, exec_id, node_rx3);

        let mut iter_index = Vec::new();

        for (key, (handle, _, _)) in handlers.iter() {
            let res = handle.await.unwrap();
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
