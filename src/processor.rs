//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::channel::{executor_channel, WorkflowTx};
use crate::edge::Edge;
use crate::node::NodeError;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::{WorkflowBuilder, WorkflowId};
use std::{collections::VecDeque, sync::Arc};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::task::{spawn, spawn_blocking, JoinError, JoinHandle};
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

type Handler<T> = (JoinHandle<T>, ExecutorId);

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

    fn push(&mut self, handle: JoinHandle<T>, exec_id: ExecutorId) {
        let retain = self.retains.pop_front().unwrap();
        self.handles[retain] = Some((handle, exec_id));
        #[cfg(feature = "dev")]
        debug!(
            "{:?} tasks are running",
            self.handles.len() - self.retains.len()
        );
    }

    fn remove(&mut self, key: usize) {
        self.retains.push_back(key);
        let _ = self.handles[key].take().unwrap();
        #[cfg(feature = "dev")]
        debug!(
            "{:?} tasks are running",
            self.handles.len() - self.retains.len()
        );
    }

    fn has_retain(&self) -> bool {
        !self.retains.is_empty()
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
        let (exec_tx, mut exec_rx) = executor_channel(n);
        let op = Arc::new(Mutex::new(Operator::new(exec_tx.clone(), self.workflow)));
        let op_clone = op.clone();
        let mut handlers: Handlers<Result<_, NodeError>> = Handlers::new(n);
        debug!("End setting up processor: capacity={}", n);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let shutdown_token = CancellationToken::new();
        let shutdown_clone = shutdown_token.clone();
        let handle = spawn(async move {
            while let Some(message) = exec_rx.recv().await {
                println!("message: {:?}", message);
                if handlers.has_retain() {
                    if let Some((node, exec_id)) = op.lock().await.next_node() {
                        let tx_clone = exec_tx.clone();
                        let op_clone = op.clone();
                        let handle = if node.is_blocking() {
                            let rt_handle = Handle::current();
                            spawn_blocking(move || {
                                rt_handle.block_on(async {
                                    let result = node.run(op_clone, exec_id).await;
                                    let _ = tx_clone.send(()).await;
                                    result
                                })?;
                                Ok(node.name())
                            })
                        } else {
                            spawn(async move {
                                node.run(op_clone, exec_id).await?;
                                let _ = tx_clone.send(()).await;
                                Ok(node.name())
                            })
                        };
                        handlers.push(handle, exec_id);
                    }
                } else {
                    warn!("No retain");
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
    /// * `wf_tx` - ワークフローの送信チャンネル
    ///
    /// # Returns
    ///
    /// 実行ID
    pub async fn start(&self, wf_id: WorkflowId, wf_tx: WorkflowTx) -> ExecutorId {
        info!("Start workflow: {:?}", wf_id);
        self.op.lock().await.start_workflow(wf_id, wf_tx).await
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
