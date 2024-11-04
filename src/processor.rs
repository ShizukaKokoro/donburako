//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::channel::{executor_channel, ExecutorMessage, WorkflowTx};
use crate::edge::Edge;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::{WorkflowBuilder, WorkflowId};
use std::sync::Arc;
use thiserror::Error;
use tokio::select;
use tokio::sync::Mutex;
use tokio::task::{spawn, JoinHandle};
use tokio::time::{sleep, Duration};
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

/// プロセッサービルダー
pub struct ProcessorBuilder {
    workflow: Vec<(WorkflowId, WorkflowBuilder)>,
    capacity: usize,
}
impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self {
            workflow: Vec::new(),
            capacity: 100,
        }
    }
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
        Self {
            workflow: wfs,
            ..self
        }
    }

    /// キューの容量の設定
    ///
    /// # Arguments
    ///
    /// * `capacity` - キューの容量
    pub fn set_capacity(self, capacity: usize) -> Self {
        Self { capacity, ..self }
    }

    /// ビルド
    ///
    /// # Arguments
    ///
    /// * `handler_num` - 一度に実行されるハンドラーの数
    #[tracing::instrument(skip(self))]
    pub fn build(self, handler_num: usize) -> Processor {
        debug!("Start building processor");
        let (exec_tx, mut exec_rx) = executor_channel(self.capacity);
        let op = Arc::new(Mutex::new(Operator::new(
            exec_tx.clone(),
            self.workflow,
            handler_num,
        )));
        let op_clone = op.clone();
        debug!("End setting up processor",);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let shutdown_token = CancellationToken::new();
        let shutdown_clone = shutdown_token.clone();
        let handle = spawn(async move {
            #[cfg(feature = "dev")]
            let mut start = std::time::Instant::now();
            while let Some(message) = select! {
            message = exec_rx.recv() => message,
            _ = cancel_clone.cancelled() => None,
            _ = sleep(Duration::from_millis(100)) => Some(ExecutorMessage::Check),
            } {
                #[cfg(feature = "dev")]
                {
                    trace!("Start loop: {:?}", start.elapsed());
                    let cnt = op.lock().await.running_tasks();
                    trace!("{} tasks is running", cnt);
                }
                trace!("Receive message: {:?}", message);
                match message {
                    ExecutorMessage::Done(key) => {
                        if op.lock().await.finish_node(key).await
                            && shutdown_clone.is_cancelled()
                            && op.lock().await.is_all_finished()
                        {
                            break;
                        }
                    }
                    ExecutorMessage::Start => {}
                    ExecutorMessage::Update => {}
                    ExecutorMessage::Check => {
                        if op.lock().await.check_handles().await
                            && shutdown_clone.is_cancelled()
                            && op.lock().await.is_all_finished()
                        {
                            break;
                        }
                    }
                }
                #[cfg(feature = "dev")]
                trace!("Processing : {:?}", start.elapsed());
                let op_clone = op.clone();
                op.lock().await.process(&op_clone).await;
                #[cfg(feature = "dev")]
                {
                    trace!("Elapsed time: {:?}", start.elapsed());
                    start = std::time::Instant::now();
                }
            }
            info!("Finish processor");
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
        self.op.lock().await.send_update();
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
        self.handle.await?
    }
}
