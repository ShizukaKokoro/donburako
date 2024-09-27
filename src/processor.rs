//! プロセッサーモジュール
//!
//! タスクの実行やレジストリの管理を行うプロセッサーを実装するモジュール。

use crate::registry::Registry;
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowID};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::{spawn, task};

/// プロセッサービルダー
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
    pub fn build(self) -> Processor {
        let (workflow, wf_ids) = {
            let mut workflow = HashMap::new();
            let mut wf_ids = Vec::new();
            for builder in self.workflow {
                let id = WorkflowID::new();
                workflow.insert(id, Arc::new(builder.build()));
                wf_ids.push(id);
            }
            (workflow, wf_ids)
        };
        Processor {
            wf_ids,
            workflow: Arc::new(workflow),
        }
    }
}

/// プロセッサー
pub struct Processor {
    wf_ids: Vec<WorkflowID>,
    workflow: Arc<HashMap<WorkflowID, Arc<Workflow>>>,
}
impl Processor {
    /// ワークフローID
    pub fn wf_ids(&self) -> &Vec<WorkflowID> {
        &self.wf_ids
    }

    /// プロセスの開始
    pub fn run(&self, n: usize) -> (JoinHandle<()>, mpsc::Sender<WorkflowID>) {
        let (tx, mut rx): (mpsc::Sender<WorkflowID>, mpsc::Receiver<WorkflowID>) =
            mpsc::channel(16);
        let workflow = self.workflow.clone();
        let handle = spawn(async move {
            let mut rgs: VecDeque<Option<(Arc<Mutex<Registry>>, WorkflowID)>> = VecDeque::new();
            let mut handles = Vec::with_capacity(n);
            let mut retains = {
                let mut retains = VecDeque::new();
                for i in 0..n {
                    retains.push_back(i);
                }
                retains
            };
            loop {
                while let Ok(wf_id) = rx.try_recv() {
                    let rg = Arc::new(Mutex::new(Registry::new(wf_id)));
                    rgs.push_back(Some((rg.clone(), wf_id)));
                    workflow[&wf_id].start(rg.lock().await);
                }

                rgs.push_back(None); // キューの最後をマーク
                while let Some(item) = rgs.pop_front() {
                    if item.is_none() {
                        break;
                    }
                    let (rg, wf_id) = item.unwrap();
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
                    }
                    rgs.push_back(Some((rg_clone, wf_id)));
                }

                for (key, item) in handles.iter_mut().enumerate().take(n) {
                    if let Some((handle, rg)) = item {
                        tokio::select! {
                            // タスクが終了した場合
                            res = handle => {
                                println!("Task {} has finished", key);
                                let task_index = res.unwrap(); // タスクが正常に終了したか確認
                                retains.push_back(key);
                                let rg = rg.lock().await;
                                let wf = workflow[&rg.wf_id()].clone();
                                wf.done(task_index, rg);
                                *item= None; // タスクハンドルをクリア
                            }
                            // タスクが終了していない場合
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                                println!("Task {} is still running", key);
                            }
                        }
                    }
                }
            }
        });
        (handle, tx)
    }
}
