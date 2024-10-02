//! プロセッサーモジュール
//!
//! ワークフローを保持し、コンテナを移動させる。

use crate::container::Container;
use crate::node::Edge;
use crate::workflow::{Workflow, WorkflowBuilder};
use log::{debug, info};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::rc::Rc;
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
        let workflow = {
            let mut workflow: Vec<Workflow> = Vec::new();
            for builder in self.workflow {
                workflow.push(builder.build());
            }
            workflow
        };
        let mut handles: Vec<Option<JoinHandle<Result<(), ProcessorError>>>> =
            Vec::with_capacity(n);
        for _ in 0..n {
            handles.push(None);
        }
        let mut retains = {
            let mut retains = VecDeque::new();
            for i in 0..n {
                retains.push_back(i);
            }
            retains
        };
        let mut cons: HashMap<Rc<Edge>, Container> = HashMap::new();
        debug!("End setting up processor: capacity={}", n);

        let (tx, mut rx): (mpsc::Sender<usize>, mpsc::Receiver<usize>) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handle = spawn(async move {
            // NOTE: ハンドル内で外側から情報を流し込み、端まで到達したコンテナのデータを取り出す仕組みがない。
            loop {
                if cancel_clone.is_cancelled() {
                    break;
                }

                while let Ok(wf_id) = rx.try_recv() {
                    debug!("Start workflow: {:?}", wf_id);
                    todo!(); // TODO: ワークフローをスレッド外から開始する
                }

                debug!("Get nodes enabled to run");
                todo!(); // TODO: ワークフローの次に実行可能なノードを取得し、タスクのスレッドを handles に追加する

                debug!("Check running tasks");
                for (key, item) in handles.iter_mut().enumerate().take(n) {
                    if let Some(handle) = item {
                        tokio::select! {
                            // タスクが終了した場合
                            res = handle => {
                                debug!("Task is done(at {} / {})", key, n);
                                retains.push_back(key);
                                todo!(); // TODO: タスクの終了処理を行う
                                *item= None; // タスクハンドルをクリア
                            }
                            // タスクが終了していない場合
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {}
                        }
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
            Ok(())
        });

        Ok(Processor { handle, tx, cancel })
    }
}

/// プロセッサー
pub struct Processor {
    handle: JoinHandle<Result<(), ProcessorError>>,
    tx: mpsc::Sender<usize>,
    cancel: CancellationToken,
}
impl Processor {
    /// ワークフローの開始
    ///
    /// # Arguments
    ///
    /// * `wf_id` - ワークフローID
    pub async fn start(&self, wf_id: usize) {
        info!("Start workflow: {:?}", wf_id);
        self.tx.send(wf_id).await.unwrap();
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
