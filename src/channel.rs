//! チャンネルモジュール
//!
//! 実行状態を各自管理するためのチャンネル。終了を通知する。

use crate::operator::ExecutorId;
use tokio::sync::mpsc::{
    channel,
    error::{SendError, TrySendError},
    Receiver, Sender,
};
use tracing::trace;

/// ワークフローのメッセージ
#[derive(Debug)]
pub enum WfMessage {
    /// 正常終了
    Done(ExecutorId),

    /// 異常終了
    Error(ExecutorId),
}

/// ワークフローの送信側
#[derive(Debug, Clone)]
pub struct WorkflowTx {
    tx: Sender<WfMessage>,
}
impl WorkflowTx {
    /// 送信
    #[tracing::instrument(skip(self))]
    pub async fn send(&self, message: WfMessage) -> Result<(), SendError<WfMessage>> {
        self.tx.send(message).await
    }
}
#[cfg(feature = "dev")]
impl Drop for WorkflowTx {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        trace!("Drop WorkflowTx");
    }
}

/// ワークフローの受信側
#[derive(Debug)]
pub struct WorkflowRx {
    rx: Receiver<WfMessage>,
}
impl WorkflowRx {
    /// 受信待ち
    ///
    /// 全ての [`WorkflowTx`] がドロップすると、`None` が返る。
    #[tracing::instrument(skip(self))]
    pub async fn recv(&mut self) -> Option<WfMessage> {
        trace!("Wait received message");
        let result = self.rx.recv().await;
        trace!("Received message: {:?}", result);
        result
    }
}
#[cfg(feature = "dev")]
impl Drop for WorkflowRx {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        trace!("Drop WorkflowRx");
    }
}

/// ワークフローチャンネルの作成
pub fn workflow_channel(n: usize) -> (WorkflowTx, WorkflowRx) {
    let (tx, rx) = channel(n);
    (WorkflowTx { tx }, WorkflowRx { rx })
}

#[derive(Debug)]
pub(crate) enum ExecutorMessage {
    /// ワークフローの実行開始
    Start,
    /// コンテナの更新
    Update,
    /// ノードの実行完了
    ///
    /// # Arguments
    ///
    /// * `key` - ハンドラーのキー
    Done(usize),
    /// 定期的な確認
    Check,
}

/// ノードの送信側
#[derive(Debug, Clone)]
pub(crate) struct ExecutorTx {
    tx: Sender<ExecutorMessage>,
}
impl ExecutorTx {
    /// 送信
    #[tracing::instrument(skip(self))]
    pub fn send(&self, message: ExecutorMessage) -> Result<(), TrySendError<ExecutorMessage>> {
        #[cfg(feature = "dev")]
        let start = std::time::Instant::now();
        trace!(
            "Send message: {:?} (capacity: {})",
            message,
            self.tx.capacity()
        );
        let result = match self.tx.try_send(message) {
            Ok(_) => Ok(()),
            Err(TrySendError::Closed(message)) => Err(TrySendError::Closed(message)),
            Err(TrySendError::Full(message)) => {
                trace!(
                    "Failed to send message because the channel is full: {:?}",
                    message
                );
                Ok(())
            }
        };
        #[cfg(feature = "dev")]
        trace!(
            "Send message: {:?} (elapsed: {:?})",
            result,
            start.elapsed()
        );
        result
    }
}
#[cfg(feature = "dev")]
impl Drop for ExecutorTx {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        trace!("Drop ExecutorTx");
    }
}

/// ノードの受信側
#[derive(Debug)]
pub(crate) struct ExecutorRx {
    rx: Receiver<ExecutorMessage>,
}
impl ExecutorRx {
    /// 受信待ち
    ///
    /// 全ての [`ExecutorTx`] がドロップすると、`None` が返る。
    #[tracing::instrument(skip(self))]
    pub async fn recv(&mut self) -> Option<ExecutorMessage> {
        #[cfg(feature = "dev")]
        let start = std::time::Instant::now();
        let result = self.rx.recv().await;
        #[cfg(feature = "dev")]
        trace!(
            "Receive message: {:?} (elapsed: {:?})",
            result,
            start.elapsed()
        );
        result
    }
}
#[cfg(feature = "dev")]
impl Drop for ExecutorRx {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        trace!("Drop ExecutorRx");
    }
}

/// ノードチャンネルの作成
pub(crate) fn executor_channel(n: usize) -> (ExecutorTx, ExecutorRx) {
    let (tx, rx) = channel(n);
    (ExecutorTx { tx }, ExecutorRx { rx })
}
