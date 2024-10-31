//! チャンネルモジュール
//!
//! 実行状態を各自管理するためのチャンネル。終了を通知する。

use crate::operator::ExecutorId;
use tokio::sync::mpsc::{channel, error::SendError, Receiver, Sender};

type WfMessage = ExecutorId;

/// ワークフローの送信側
#[derive(Debug, Clone)]
pub struct WorkflowTx {
    tx: Sender<WfMessage>,
}
impl WorkflowTx {
    /// 送信
    pub async fn send(&self, message: WfMessage) -> Result<(), SendError<WfMessage>> {
        self.tx.send(message).await
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
    pub async fn recv(&mut self) -> Option<WfMessage> {
        self.rx.recv().await
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
}

/// ノードの送信側
#[derive(Debug, Clone)]
pub(crate) struct ExecutorTx {
    tx: Sender<ExecutorMessage>,
}
impl ExecutorTx {
    /// 送信
    pub async fn send(&self, message: ExecutorMessage) -> Result<(), SendError<ExecutorMessage>> {
        self.tx.send(message).await
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
    pub async fn recv(&mut self) -> Option<ExecutorMessage> {
        self.rx.recv().await
    }
}

/// ノードチャンネルの作成
pub(crate) fn executor_channel(n: usize) -> (ExecutorTx, ExecutorRx) {
    let (tx, rx) = channel(n);
    (ExecutorTx { tx }, ExecutorRx { rx })
}
