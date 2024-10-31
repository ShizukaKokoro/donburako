//! オペレーターモジュール

use crate::channel::{ExecutorTx, WorkflowTx};
use crate::container::{Container, ContainerError, ContainerMap};
use crate::edge::Edge;
use crate::node::Node;
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;
use tokio::task::spawn_blocking;
use tracing::{debug, warn};
use uuid::Uuid;

/// オペレーターエラー
#[derive(Debug, Error, PartialEq)]
pub enum OperatorError {
    /// コンテナエラー
    #[error("Container error")]
    ContainerError(#[from] ContainerError),

    /// 終了した実行IDに対してコンテナを追加しようとした
    #[error("The executor ID({0:?}) has already finished(Cannot add a container)")]
    NotRunning(ExecutorId),
}

/// 実行ID
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ExecutorId(Uuid);
impl ExecutorId {
    /// 実行IDの生成
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
impl Default for ExecutorId {
    fn default() -> Self {
        Self::new()
    }
}

/// 実行可能なノードのキュー
#[derive(Debug, Default)]
struct ExecutableQueue {
    queue: VecDeque<(Arc<Node>, ExecutorId)>,
    set: HashSet<(Arc<Node>, ExecutorId)>,
}
impl ExecutableQueue {
    /// 新しい実行可能なノードのキューの生成
    fn push(&mut self, node: Arc<Node>, exec_id: ExecutorId) {
        if self.set.insert((node.clone(), exec_id)) {
            self.queue.push_back((node.clone(), exec_id));
        }
    }

    /// ノードの取得
    fn pop(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        if let Some(item) = self.queue.pop_front() {
            assert!(self.set.remove(&item));
            Some(item)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct StatusMap(HashMap<ExecutorId, (WorkflowId, WorkflowTx)>);
impl StatusMap {
    fn start(&mut self, exec_id: ExecutorId, wf_id: WorkflowId, tx: WorkflowTx) {
        let _ = self.0.insert(exec_id, (wf_id, tx));
    }
}

/// オペレーター
///
/// コンテナと実行IDごとのワークフローの状態を管理する。
#[derive(Debug)]
pub struct Operator {
    exec_tx: ExecutorTx,
    workflows: HashMap<WorkflowId, Workflow>,
    status: StatusMap,
    containers: ContainerMap,
    queue: ExecutableQueue,
}
impl Operator {
    /// 新しいオペレーターの生成
    ///
    /// # Arguments
    ///
    /// * `builders` - ワークフロービルダーのリスト
    pub(crate) fn new(exec_tx: ExecutorTx, builders: Vec<(WorkflowId, WorkflowBuilder)>) -> Self {
        let mut workflows = HashMap::new();
        for (id, builder) in builders {
            let _ = workflows.insert(id, builder.build());
        }
        Self {
            exec_tx,
            workflows,
            status: StatusMap(HashMap::new()),
            containers: ContainerMap::default(),
            queue: ExecutableQueue::default(),
        }
    }

    /// ワークフローIDから始点と終点のエッジを取得
    ///
    /// # Arguments
    ///
    /// * `wf_id` - ワークフローID
    ///
    /// # Returns
    ///
    /// 0: 始点のエッジ
    /// 1: 終点のエッジ
    pub fn get_start_end_edges(&self, wf_id: &WorkflowId) -> (Vec<Arc<Edge>>, Vec<Arc<Edge>>) {
        let wf = &self.workflows[wf_id];
        (wf.start_edges().clone(), wf.end_edges().clone())
    }

    /// ワークフローの実行開始
    ///
    /// # Arguments
    ///
    /// * `wf_id` - ワークフローID
    /// * `wf_tx` - ワークフローの送信チャンネル
    ///
    /// # Returns
    ///
    /// 実行ID
    pub async fn start_workflow(&mut self, wf_id: WorkflowId, wf_tx: WorkflowTx) -> ExecutorId {
        let exec_id = ExecutorId::new();
        #[cfg(feature = "dev")]
        tracing::info!("Start workflow: {:?}({:?})", wf_id, exec_id);
        self.status.start(exec_id, wf_id, wf_tx);
        self.containers.entry_by_exec_id(exec_id);
        for node in self.workflows[&wf_id].start_nodes() {
            self.queue.push(node.clone(), exec_id);
        }
        self.exec_tx.send(()).await.unwrap();
        exec_id
    }

    /// 次に実行するノードの取得
    pub(crate) fn next_node(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        self.queue.pop()
    }

    /// 新しいコンテナの追加
    ///
    /// ワークフローの入り口となるエッジに対して、新しいコンテナを追加する。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    /// * `data` - データ
    ///
    /// # Returns
    ///
    /// 成功した場合は Ok(())
    #[tracing::instrument(skip(self, data))]
    pub(crate) async fn add_new_container<T: 'static + Send + Sync>(
        &mut self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), OperatorError> {
        debug!("Add new container");
        self.containers
            .add_new_container(edge.clone(), exec_id, data)?;
        Ok(())
    }

    /// コンテナの取得
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// コンテナ
    #[tracing::instrument(skip(self))]
    pub async fn get_container(
        &mut self,
        edge: &[Arc<Edge>],
        exec_id: ExecutorId,
    ) -> Result<VecDeque<Container>, OperatorError> {
        if !self.containers.is_running(exec_id) {
            return Err(OperatorError::NotRunning(exec_id));
        }
        debug!("Get container");
        let mut containers = VecDeque::new();
        for e in edge {
            if let Some(container) = self.containers.get_container(e.clone(), exec_id) {
                containers.push_back(container);
            }
        }
        if containers.is_empty() {
            warn!("Container is empty");
        }
        Ok(containers)
    }

    /// 既存のコンテナの追加
    ///
    /// エッジの移動時に、既存のコンテナを追加する。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    /// * `container` - コンテナ
    ///
    /// # Returns
    ///
    /// 成功した場合は Ok(())
    #[tracing::instrument(skip(self, container))]
    pub async fn add_container(
        &mut self,
        edge: &[Arc<Edge>],
        exec_id: ExecutorId,
        container: VecDeque<Container>,
    ) -> Result<(), OperatorError> {
        debug!("Add container: {:?}", container);
        for (e, c) in edge.iter().zip(container) {
            self.containers.add_container(e.clone(), exec_id, c)?;
        }
        Ok(())
    }

    /// ワークフローの強制終了
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub async fn finish_workflow_by_execute_id(&mut self, exec_id: ExecutorId) {
        debug!("Finish workflow: {:?}", exec_id);
        self.containers.finish_containers(exec_id);
    }
}
