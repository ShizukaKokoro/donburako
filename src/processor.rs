//! プロセッサーモジュール
//!
//! タスクの実行やレジストリの管理を行うプロセッサーを実装するモジュール。

use crate::registry::{Registry, RegistryID};
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowID};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

/// プロセッサー
pub struct Processor {
    workflow: Vec<WorkflowBuilder>,
}
impl Processor {
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
    pub fn build(self, n: usize) -> (JoinHandle<()>, Vec<WorkflowID>, mpsc::Sender<WorkflowID>) {
        let mut workflow = HashMap::new();
        let mut wf_ids = Vec::new();
        for wf in self.workflow {
            let id = WorkflowID::new();
            workflow.insert(id, wf.build());
            wf_ids.push(id);
        }

        let (tx, mut rx): (mpsc::Sender<WorkflowID>, mpsc::Receiver<WorkflowID>) =
            mpsc::channel(16);
        let handle = spawn(async move {
            let mut rgs: HashMap<RegistryID, Arc<Mutex<Registry>>> = HashMap::new();
            let mut handles: Vec<Option<JoinHandle<()>>> = Vec::with_capacity(n);
            let mut retains = {
                let mut retains = VecDeque::new();
                for i in 0..n {
                    retains.push_back(i);
                }
                retains
            };
            loop {
                while let Ok(wf_id) = rx.try_recv() {
                    let id = RegistryID::new();
                    let rg = Arc::new(Mutex::new(Registry::new()));
                    rgs.insert(id, rg.clone());
                    workflow[&wf_id].start(&rg).await;
                }

                //TODO: 次のノードを取得し、実行し、 handles にいれる。(retains を用いて空きスペースに入れる)

                for (key, item) in handles.iter_mut().enumerate().take(n) {
                    if let Some(handle) = item {
                        tokio::select! {
                            // タスクが終了した場合
                            res = handle => {
                                println!("Task {} has finished", key);
                                res.unwrap(); // タスクが正常に終了したか確認
                                *item= None; // タスクハンドルをクリア
                                retains.push_back(key);
                                //TODO: タスクの完了を通知
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
        (handle, wf_ids, tx)
    }
}
