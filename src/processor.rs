//! プロセッサーモジュール
//!
//! タスクの実行やレジストリの管理を行うプロセッサーを実装するモジュール。

use crate::registry::Registry;
use crate::workflow::{WorkflowBuilder, WorkflowError, WorkflowID};
use log::{debug, info};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::{spawn, task};
use tokio_util::sync::CancellationToken;

/// プロセッサーエラー
#[derive(Error, Debug)]
pub enum ProcessorError {
    /// ワークフローエラー
    #[error("workflow error: {0}")]
    WorkflowError(#[from] WorkflowError),

    /// ワークフローの実行開始エラー
    #[error("failed to start workflow")]
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
        let (workflow, wf_ids) = {
            let mut workflow = HashMap::new();
            let mut wf_ids = Vec::new();
            for builder in self.workflow {
                let id = WorkflowID::new();
                assert!(workflow.insert(id, Arc::new(builder.build()?)).is_none());
                wf_ids.push(id);
            }
            (Arc::new(workflow), wf_ids)
        };

        let (tx, mut rx): (mpsc::Sender<WorkflowID>, mpsc::Receiver<WorkflowID>) =
            mpsc::channel(16);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handle = spawn(async move {
            let mut rgs: VecDeque<Arc<Mutex<Registry>>> = VecDeque::new();
            let mut handles = Vec::with_capacity(n);
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
            debug!("End setting up processor: capacity={}", n);

            loop {
                if cancel_clone.is_cancelled() {
                    break;
                }

                while let Ok(wf_id) = rx.try_recv() {
                    debug!("Start workflow: {:?}", wf_id);
                    let rg = Arc::new(Mutex::new(Registry::new(wf_id)));
                    rgs.push_back(rg.clone());
                    if let Some(wf) = workflow.get(&wf_id) {
                        wf.start(rg.lock().await)?;
                    } else {
                        return Err(ProcessorError::FailedToStartWorkflow);
                    }
                }

                debug!("Get nodes enabled to run");
                for _ in 0..rgs.len() {
                    let rg = rgs.pop_front().unwrap();
                    let wf_id = rg.lock().await.wf_id();
                    if rg.lock().await.finished {
                        debug!("Workflow is finished: {:?}", wf_id);
                        continue;
                    }
                    let wf = workflow[&wf_id].clone();
                    let rg_clone = rg.clone();
                    if !retains.is_empty() {
                        if let Some((task_index, node)) = wf.get_next(rg.lock().await) {
                            let index = retains.pop_front().unwrap();
                            let handle = if !node.is_blocking() {
                                task::spawn(async move {
                                    node.run(&rg).await;
                                    task_index
                                })
                            } else {
                                let rt_handle = Handle::current();
                                task::spawn_blocking(move || {
                                    rt_handle.block_on(async { node.run(&rg).await });
                                    task_index
                                })
                            };
                            handles[index] = Some((handle, rg_clone.clone()));
                        }
                        rgs.push_back(rg_clone);
                    } else {
                        debug!("No capacity to run");
                        rgs.push_front(rg);
                        break;
                    }
                }

                debug!("Check running tasks");
                for (key, item) in handles.iter_mut().enumerate().take(n) {
                    if let Some((handle, rg)) = item {
                        tokio::select! {
                            // タスクが終了した場合
                            res = handle => {
                                debug!("Task is done(at {} / {})", key, n);
                                let task_index = res.unwrap(); // タスクが正常に終了したか確認
                                retains.push_back(key);
                                let rg = rg.lock().await;
                                let wf = workflow[&rg.wf_id()].clone();
                                wf.done(task_index, rg);
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

        Ok(Processor {
            wf_ids,
            handle,
            tx,
            cancel,
        })
    }
}

/// プロセッサー
pub struct Processor {
    wf_ids: Vec<WorkflowID>,
    handle: JoinHandle<Result<(), ProcessorError>>,
    tx: mpsc::Sender<WorkflowID>,
    cancel: CancellationToken,
}
impl Processor {
    /// ワークフローID
    pub fn wf_ids(&self) -> &Vec<WorkflowID> {
        &self.wf_ids
    }

    /// ワークフローの開始
    ///
    /// # Arguments
    ///
    /// * `wf_id` - ワークフローID
    pub async fn start(&self, wf_id: WorkflowID) {
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
