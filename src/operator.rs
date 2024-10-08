//! オペレーターモジュール

use crate::container::{Container, ContainerError, ContainerMap};
use crate::edge::Edge;
use crate::node::Node;
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// オペレーターエラー
#[derive(Debug, Error, PartialEq)]
pub enum OperatorError {
    /// コンテナエラー
    #[error("Container error")]
    ContainerError(#[from] ContainerError),

    /// ワークフローが開始されていない
    #[error("Workflow is not started")]
    NotStarted,
}

/// 実行ID
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ExecutorId(Uuid);
impl ExecutorId {
    /// 実行IDの生成
    pub fn new() -> Self {
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

/// 状態
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// 実行中
    Running(WorkflowId),
    /// 終了
    Finished(WorkflowId),
}

/// オペレーター
///
/// コンテナと実行IDごとのワークフローの状態を管理する。
#[derive(Debug, Clone)]
pub struct Operator {
    workflows: Arc<HashMap<WorkflowId, Workflow>>,
    containers: Arc<Mutex<ContainerMap>>,
    executors: Arc<Mutex<HashMap<ExecutorId, State>>>,
    queue: Arc<Mutex<ExecutableQueue>>,
}
impl Operator {
    /// 新しいオペレーターの生成
    ///
    /// # Arguments
    ///
    /// * `builders` - ワークフロービルダーのリスト
    pub(crate) fn new(builders: Vec<WorkflowBuilder>) -> Self {
        let mut workflows = HashMap::new();
        for builder in builders {
            let (workflow, id) = builder.build();
            let _ = workflows.insert(id, workflow);
        }
        Self {
            workflows: Arc::new(workflows),
            containers: Arc::new(Mutex::new(ContainerMap::default())),
            executors: Arc::new(Mutex::new(HashMap::new())),
            queue: Arc::new(Mutex::new(ExecutableQueue::default())),
        }
    }

    /// エッジからノードの実行可能性を確認し、実行可能な場合はキューに追加する
    async fn enqueue_node_if_executable(
        &self,
        edge: &Arc<Edge>,
        exec_id: ExecutorId,
    ) -> Result<(), OperatorError> {
        let mut exec = self.executors.lock().await;
        let wf_id = if let Some(state) = exec.get(&exec_id) {
            match state {
                State::Running(wf_id) => wf_id,
                State::Finished(wf_id) => wf_id,
            }
        } else {
            return Err(OperatorError::NotStarted);
        };
        if let Some(node) = self.workflows[wf_id].get_node(edge) {
            if self.check_node_executable(&node, exec_id).await {
                self.queue.lock().await.push(node, exec_id);
            }
        } else {
            self.check_finish(exec_id, &mut exec).await;
        }
        Ok(())
    }

    async fn check_finish(
        &self,
        exec_id: ExecutorId,
        exec: &mut MutexGuard<'_, HashMap<ExecutorId, State>>,
    ) {
        let wf_id = if let Some(State::Running(wf_id)) = exec.get(&exec_id) {
            for end in self.workflows[wf_id].end_edges() {
                if !self
                    .containers
                    .lock()
                    .await
                    .check_edge_exists(end.clone(), exec_id)
                {
                    return;
                }
            }
            *wf_id
        } else {
            return;
        };
        let _ = exec.insert(exec_id, State::Finished(wf_id));
    }

    #[cfg(test)]
    pub(crate) async fn is_finished(&self, exec_id: ExecutorId) -> bool {
        let exec = self.executors.lock().await;
        matches!(exec.get(&exec_id), Some(State::Finished(_)))
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
    pub(crate) async fn add_new_container<T: 'static + Send + Sync>(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), OperatorError> {
        self.containers
            .lock()
            .await
            .add_new_container(edge.clone(), exec_id, data)?;
        self.enqueue_node_if_executable(&edge, exec_id).await
    }

    /// ノードが実行できるか確認する
    ///
    /// ノードは全ての入力エッジに対して、コンテナが格納されることで実行可能となる。
    ///
    /// # Arguments
    ///
    /// * `node` - ノード
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 実行可能な場合は true、そうでない場合は false
    pub(crate) async fn check_node_executable(
        &self,
        node: &Arc<Node>,
        exec_id: ExecutorId,
    ) -> bool {
        self.containers
            .lock()
            .await
            .check_node_executable(node, exec_id)
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
    pub async fn get_container(&self, edge: Arc<Edge>, exec_id: ExecutorId) -> Option<Container> {
        self.containers.lock().await.get_container(edge, exec_id)
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
    pub async fn add_container(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        container: Container,
    ) -> Result<(), OperatorError> {
        self.containers
            .lock()
            .await
            .add_container(edge.clone(), exec_id, container)?;
        self.enqueue_node_if_executable(&edge, exec_id).await
    }

    /// ワークフローの実行開始
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    /// * `wf_id` - ワークフローID
    pub async fn start_workflow(&self, exec_id: ExecutorId, wf_id: WorkflowId) {
        let _ = self
            .executors
            .lock()
            .await
            .insert(exec_id, State::Running(wf_id));
    }

    /// ワークフローの実行終了の待機
    pub async fn wait_finish(&self, exec_id: ExecutorId, duration: u64) {
        loop {
            let exec = self.executors.lock().await;
            if let Some(State::Running(_)) = exec.get(&exec_id) {
            } else {
                break;
            }
            drop(exec);
            tokio::time::sleep(tokio::time::Duration::from_millis(duration)).await;
        }
    }

    /// 次に実行するノードを取得する
    pub(crate) async fn get_next_node(&self) -> Option<(Arc<Node>, ExecutorId)> {
        self.queue.lock().await.pop()
    }

    /// 実行可能なノードが存在するか確認する
    pub(crate) async fn has_executable_node(&self) -> bool {
        !self.queue.lock().await.queue.is_empty()
    }

    /// 実行IDからワークフローIDを取得
    pub async fn get_workflow_id(&self, exec_id: ExecutorId) -> Option<WorkflowId> {
        let exec = self.executors.lock().await;
        let state = exec.get(&exec_id)?;
        match state {
            State::Running(wf_id) => Some(*wf_id),
            State::Finished(wf_id) => Some(*wf_id),
        }
    }

    /// ワークフローIDから始点と終点のエッジを取得
    pub fn get_start_end_edges(&self, wf_id: &WorkflowId) -> (&Vec<Arc<Edge>>, &Vec<Arc<Edge>>) {
        let wf = &self.workflows[wf_id];
        (wf.start_edges(), wf.end_edges())
    }

    /// 特定の実行IDに対応するコンテナを全て終了処理する
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    pub async fn finish_containers(&self, exec_id: ExecutorId) {
        self.containers.lock().await.finish_containers(exec_id);
        let _ = self.executors.lock().await.remove(&exec_id).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::node::Choice;

    #[tokio::test]
    async fn test_operator_enqueue_node_if_executable() {
        let edge = Arc::new(Edge::new::<&str>());
        let node = Arc::new(Node::new_test(vec![edge.clone()], "node", Choice::All));
        let builder = WorkflowBuilder::new(WorkflowId::new("test"))
            .add_node(node.clone())
            .unwrap();
        let wf_id = builder.id();
        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, wf_id).await;
        op.add_new_container(edge.clone(), exec_id, "test")
            .await
            .unwrap();
        op.enqueue_node_if_executable(&edge, exec_id).await.unwrap();
        let queue = op.queue.lock().await;
        let expected = VecDeque::from(vec![(node, exec_id)]);
        assert_eq!(queue.queue, expected);
    }

    #[tokio::test]
    async fn test_operator_start_workflow() {
        let op = Operator::new(vec![]);
        let exec_id = ExecutorId::new();
        let wf_id = WorkflowId::new("test");
        op.start_workflow(exec_id, wf_id).await;
        let executors = op.executors.lock().await;
        assert_eq!(executors.get(&exec_id), Some(&State::Running(wf_id)));
    }

    #[tokio::test]
    async fn test_operator_get_next_node() {
        let edge = Arc::new(Edge::new::<&str>());
        let node = Arc::new(Node::new_test(vec![edge.clone()], "node", Choice::All));
        let builder = WorkflowBuilder::new(WorkflowId::new("test"))
            .add_node(node.clone())
            .unwrap();
        let wf_id = builder.id();
        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, wf_id).await;
        op.add_new_container(edge.clone(), exec_id, "test")
            .await
            .unwrap();
        op.enqueue_node_if_executable(&edge, exec_id).await.unwrap();
        let next = op.get_next_node().await.unwrap();
        assert_eq!(next, (node, exec_id));
    }

    #[tokio::test]
    async fn test_workflow_wait_finish() {
        let edge = Arc::new(Edge::new::<&str>());
        let mut node = Node::new(
            vec![edge.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut con = op
                        .get_container(self_.inputs()[0].clone(), exec_id)
                        .await
                        .unwrap();
                    let _: &str = con.take().unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store("test");
                    op.add_container(self_.outputs()[0].clone(), exec_id, con_clone)
                        .await
                        .unwrap();
                })
            }),
            false,
            "node",
            Choice::All,
        );
        let edge_to = node.add_output::<&str>();
        let builder = WorkflowBuilder::new(WorkflowId::new("test"))
            .add_node(Arc::new(node))
            .unwrap();
        let wf_id = builder.id();
        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, wf_id).await;
        op.add_new_container(edge, exec_id, "test").await.unwrap();
        let node = op.get_next_node().await.unwrap().0;
        let f = node.run(&op, exec_id);
        assert!(!op.is_finished(exec_id).await);
        assert!(op.get_container(edge_to.clone(), exec_id).await.is_none());
        f.await;
        op.wait_finish(exec_id, 10).await;
        assert!(op.get_container(edge_to, exec_id).await.is_some());
        assert!(op.is_finished(exec_id).await);
    }
}
