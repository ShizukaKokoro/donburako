//! オペレーターモジュール

use crate::channel::{ExecutorMessage, ExecutorTx, WorkflowTx};
use crate::container::{Container, ContainerError, ContainerMap};
use crate::edge::Edge;
use crate::node::Node;
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;
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
    #[tracing::instrument(skip(self))]
    fn push(&mut self, node: Arc<Node>, exec_id: ExecutorId) {
        debug!("Push node");
        if self.set.insert((node.clone(), exec_id)) {
            self.queue.push_back((node.clone(), exec_id));
        }
    }

    /// ノードの取得
    #[tracing::instrument(skip(self))]
    fn pop(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        if let Some(item) = self.queue.pop_front() {
            assert!(self.set.remove(&item));
            debug!("Pop node");
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

    fn get_workflow_id(&self, exec_id: &ExecutorId) -> Option<&WorkflowId> {
        self.0.get(exec_id).map(|(wf_id, _)| wf_id)
    }

    #[tracing::instrument(skip(self))]
    async fn end(&mut self, exec_id: ExecutorId) {
        if let Some((_, wf_tx)) = self.0.remove(&exec_id) {
            debug!("Send end message");
            let _ = wf_tx.send(exec_id).await;
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
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
        self.exec_tx.send(ExecutorMessage::Start).unwrap();
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
        if self.workflows[self.status.get_workflow_id(&exec_id).unwrap()].is_ignored(&edge) {
            return Ok(());
        }
        debug!("Add new container");
        self.containers
            .add_new_container(edge.clone(), exec_id, data)?;
        self.check_executable_nodes(&[edge], exec_id).await;
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
    /// * `edges` - エッジ
    /// * `exec_id` - 実行ID
    /// * `container` - コンテナ
    ///
    /// # Returns
    ///
    /// 成功した場合は Ok(())
    #[tracing::instrument(skip(self, container))]
    pub async fn add_container(
        &mut self,
        edges: &[Arc<Edge>],
        exec_id: ExecutorId,
        container: VecDeque<Container>,
    ) -> Result<(), OperatorError> {
        debug!("Add container: {:?}", container);
        let wf_id = self
            .status
            .get_workflow_id(&exec_id)
            .ok_or(OperatorError::NotRunning(exec_id))?;
        for (e, mut c) in edges.iter().zip(container) {
            if self.workflows[wf_id].is_ignored(e) {
                debug!("Ignore edge");
                c.take_anyway();
                continue;
            } else {
                self.containers.add_container(e.clone(), exec_id, c)?;
            }
        }
        self.check_executable_nodes(edges, exec_id).await;
        Ok(())
    }

    /// 実行可能なノードの精査
    ///
    /// 更新があったエッジに対して、実行可能なノードをキューに追加する。
    ///
    /// # Arguments
    ///
    /// * `edges` - エッジ
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub async fn check_executable_nodes(&mut self, edges: &[Arc<Edge>], exec_id: ExecutorId) {
        debug!("Check executable nodes");
        let mut flag = false;
        for e in edges {
            let wf_id = self.status.get_workflow_id(&exec_id).unwrap();
            if let Some(node) = self.workflows[wf_id].get_node(e) {
                if self.containers.is_ready(&node, exec_id) {
                    self.queue.push(node, exec_id);
                    flag = true;
                }
            }
        }
        if flag {
            self.exec_tx.send(ExecutorMessage::Update).unwrap();
        }
    }

    /// ワークフローの終了確認
    ///
    /// ワークフローが終了しているか確認する。
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 終了している場合は true
    #[tracing::instrument(skip(self))]
    pub(crate) async fn is_finished(&mut self, exec_id: ExecutorId) -> bool {
        debug!("Check finished");
        if let Some(wf_id) = self.status.get_workflow_id(&exec_id) {
            let edges = self.workflows[wf_id].end_edges();
            if self
                .containers
                .check_edges_exists_in_exec_id(exec_id, edges)
            {
                self.status.end(exec_id).await;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// ワークフローの強制終了
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub async fn finish_workflow_by_execute_id(&mut self, exec_id: ExecutorId) {
        debug!("Finish workflow");
        self.containers.finish_containers(exec_id);
        self.status.end(exec_id).await;
    }

    /// ワークフローが全て終了しているか確認
    pub fn is_all_finished(&self) -> bool {
        self.status.is_empty()
    }
}
