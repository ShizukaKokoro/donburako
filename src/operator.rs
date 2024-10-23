//! オペレーターモジュール

use crate::container::{Container, ContainerError, ContainerMap};
use crate::edge::Edge;
use crate::node::Node;
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};
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

/// 状態
#[derive(Debug)]
enum State {
    /// 実行中
    ///
    /// ワークフローIDと終了通知用のチャンネルと無視する残りのエッジ数
    Running(WorkflowId, oneshot::Sender<()>, usize),
    /// 終了
    ///
    /// ワークフローID
    Finished(WorkflowId),
    /// タイマー待ち
    ///
    /// ワークフローID
    #[cfg(feature = "dev")]
    WaitTimer(WorkflowId),
}

/// オペレーター
///
/// コンテナと実行IDごとのワークフローの状態を管理する。
#[derive(Debug)]
pub struct Operator {
    workflows: HashMap<WorkflowId, Workflow>,
    containers: ContainerMap,
    executors: HashMap<ExecutorId, State>,
    queue: ExecutableQueue,
    #[cfg(feature = "dev")]
    time: HashMap<(WorkflowId, ExecutorId), std::time::Instant>,
}
impl Operator {
    /// 新しいオペレーターの生成
    ///
    /// # Arguments
    ///
    /// * `builders` - ワークフロービルダーのリスト
    pub(crate) fn new(builders: Vec<(WorkflowId, WorkflowBuilder)>) -> Self {
        let mut workflows = HashMap::new();
        for (id, builder) in builders {
            let _ = workflows.insert(id, builder.build());
        }
        Self {
            workflows,
            containers: ContainerMap::default(),
            executors: HashMap::new(),
            queue: ExecutableQueue::default(),
            #[cfg(feature = "dev")]
            time: HashMap::new(),
        }
    }

    /// 実行IDからワークフローIDを取得
    ///
    /// 実行IDに対応するワークフローが存在しない場合は None を返す。
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// ワークフローID
    pub(crate) async fn get_wf_id(&self, exec_id: ExecutorId) -> Option<WorkflowId> {
        match self.executors.get(&exec_id) {
            Some(State::Running(wf_id, _, _)) => Some(*wf_id),
            Some(State::Finished(wf_id)) => Some(*wf_id),
            #[cfg(feature = "dev")]
            Some(State::WaitTimer(wf_id)) => Some(*wf_id),
            _ => None,
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
    /// * `exec_id` - 実行ID
    /// * `wf_id` - ワークフローID
    pub async fn start_workflow(
        &mut self,
        exec_id: ExecutorId,
        wf_id: WorkflowId,
    ) -> oneshot::Receiver<()> {
        #[cfg(feature = "dev")]
        tracing::info!("Start workflow: {:?}({:?})", wf_id, exec_id);
        self.containers.entry_by_exec_id(exec_id);
        let (wf_tx, wf_rx) = oneshot::channel();
        let ignore_cnt = self.workflows[&wf_id].ignore_edges().len();
        let _ = self
            .executors
            .insert(exec_id, State::Running(wf_id, wf_tx, ignore_cnt));
        for node in self.workflows[&wf_id].start_nodes() {
            self.queue.push(node.clone(), exec_id);
        }
        wf_rx
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
        self.enqueue_node_if_executable(&[edge], exec_id).await
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
        if self.is_finished(exec_id).await {
            trace!("Adding container at finished executor is ignored");
            for mut c in container {
                c.take_anyway();
            }
            return Err(OperatorError::NotRunning(exec_id));
        }
        debug!("Add container: {:?}", container);
        for (e, c) in edge.iter().zip(container) {
            self.containers.add_container(e.clone(), exec_id, c)?;
        }
        self.enqueue_node_if_executable(edge, exec_id).await
    }

    /// エッジからノードの実行可能性を確認し、実行可能な場合はキューに追加する
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 成功した場合は Ok(())
    #[tracing::instrument(skip(self))]
    async fn enqueue_node_if_executable(
        &mut self,
        edge: &[Arc<Edge>],
        exec_id: ExecutorId,
    ) -> Result<(), OperatorError> {
        let (wf_id, mut ignore_cnt) = if let Some(state) = self.executors.get(&exec_id) {
            match state {
                State::Running(wf_id, _, ignore_cnt) => (wf_id, *ignore_cnt),
                State::Finished(_) => return Ok(()),
                #[cfg(feature = "dev")]
                State::WaitTimer(_) => return Ok(()),
            }
        } else {
            return Ok(());
        };
        let mut finish = false;
        for e in edge {
            if let Some(node) = self.workflows[wf_id].get_node(e) {
                if self.containers.check_node_executable(&node, exec_id) {
                    self.queue.push(node, exec_id);
                }
            } else if self.workflows[wf_id].ignore_edges().contains(e) {
                ignore_cnt -= 1;
                if let Some(mut con) = self.containers.get_container(e.clone(), exec_id) {
                    con.take_anyway();
                }
                if ignore_cnt == 0 {
                    finish = true;
                }
            } else {
                finish = true;
            }
        }
        if finish {
            self.check_finish(exec_id);
        }
        Ok(())
    }

    /// 実行可能なノードが存在するか確認する
    ///
    /// # Returns
    ///
    /// 実行可能なノードが存在する場合は true、存在しない場合は false
    pub(crate) async fn has_executable_node(&self) -> bool {
        !self.queue.queue.is_empty()
    }

    /// 次に実行するノードを取得する
    ///
    /// 実行可能なノードが存在しない場合は None を返す。
    ///
    /// # Returns
    ///
    /// 実行可能なノードと実行ID
    pub(crate) async fn get_next_node(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        while let Some((node, exec_id)) = self.queue.pop() {
            if self.containers.check_node_executable(&node, exec_id) {
                return Some((node, exec_id));
            }
        }
        None
    }

    #[tracing::instrument(skip(self))]
    fn check_finish(&mut self, exec_id: ExecutorId) {
        let wf_id = if let Some(State::Running(wf_id, wf_tx, ignore_cnt)) =
            self.executors.remove(&exec_id)
        {
            for end in self.workflows[&wf_id].end_edges() {
                if self.workflows[&wf_id].ignore_edges().contains(end) {
                    continue;
                }
                if !self.containers.check_edge_exists(end.clone(), exec_id) {
                    assert!(self
                        .executors
                        .insert(exec_id, State::Running(wf_id, wf_tx, ignore_cnt))
                        .is_none());
                    return;
                }
            }
            let _ = wf_tx.send(());
            wf_id
        } else {
            return;
        };
        let _ = self.executors.insert(exec_id, State::Finished(wf_id));
    }

    /// ワークフローの終了確認
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// ワークフローが終了していない場合は false、終了している場合は true
    #[tracing::instrument(skip(self))]
    pub(crate) async fn is_finished(&self, exec_id: ExecutorId) -> bool {
        self.executors
            .get(&exec_id)
            .map_or(false, |state| !matches!(state, State::Running(_, _, _)))
    }

    /// 終了したワークフローから全てのコンテナがなくなっているか確認する
    ///
    /// 実行が終了した実行IDを指定すると false を返す。
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 全てのコンテナがなくなっている場合は true、残っている場合は false
    #[tracing::instrument(skip(self))]
    pub(crate) async fn check_all_containers_taken(&self, exec_id: ExecutorId) -> bool {
        if let Some(wf_id) = self.get_wf_id(exec_id).await {
            let (_, end) = self.get_start_end_edges(&wf_id);
            for edge in end {
                if self.containers.check_edge_exists(edge.clone(), exec_id) {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// 実行IDに対応するワークフローの終了処理
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub async fn finish_workflow_by_execute_id(&mut self, exec_id: ExecutorId) {
        debug!("Ending workflow");
        self.containers.finish_containers(exec_id);
        #[cfg(not(feature = "dev"))]
        let _ = self.executors.remove(&exec_id);
        #[cfg(feature = "dev")]
        {
            let wf_id = match self.executors.remove(&exec_id) {
                Some(State::Running(wf_id, wf_tx, _)) => {
                    let _ = wf_tx.send(());
                    wf_id
                }
                Some(State::Finished(wf_id)) => wf_id,
                Some(State::WaitTimer(wf_id)) => wf_id,
                None => return,
            };
            let _ = self.executors.insert(exec_id, State::WaitTimer(wf_id));
        }
    }

    /// 全てのワークフローが終了しているか確認する
    #[tracing::instrument(skip(self))]
    pub async fn check_all_finished(&self) -> bool {
        self.executors
            .iter()
            .all(|(_, state)| !matches!(state, State::Running(_, _, _)))
    }

    #[cfg(feature = "dev")]
    pub(crate) async fn start_timer(&mut self, exec_id: ExecutorId) {
        let wf_id = self.get_wf_id(exec_id).await.unwrap();
        let _ = self
            .time
            .insert((wf_id, exec_id), std::time::Instant::now());
    }

    #[cfg(feature = "dev")]
    pub(crate) async fn stop_timer(&mut self, exec_id: ExecutorId) {
        let wf_id = self.get_wf_id(exec_id).await.unwrap();
        if let Some(inst) = self.time.remove(&(wf_id, exec_id)) {
            tracing::info!(
                "{:?}({:?}) is finished in {:?}",
                wf_id,
                exec_id,
                inst.elapsed()
            );
        }
        let _ = self.executors.remove(&exec_id);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::node::Choice;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_operator_enqueue_node_if_executable() {
        let wf_id = WorkflowId::new("test");
        let edge = Arc::new(Edge::new::<&str>());
        let node = Arc::new(Node::new_test(
            vec![edge.clone()],
            vec![],
            "node",
            Choice::All,
        ));
        let builder = WorkflowBuilder::default().add_node(node.clone()).unwrap();
        let mut op = Operator::new(vec![(wf_id, builder)]);
        let exec_id = ExecutorId::default();
        let wf_rx = op.start_workflow(exec_id, wf_id).await;
        drop(wf_rx);
        op.add_new_container(edge.clone(), exec_id, "test")
            .await
            .unwrap();
        op.enqueue_node_if_executable(&[edge], exec_id)
            .await
            .unwrap();
        let queue = op.queue;
        let expected = VecDeque::from(vec![(node, exec_id)]);
        assert_eq!(queue.queue, expected);
    }

    #[tokio::test]
    async fn test_operator_start_workflow() {
        let wf_id = WorkflowId::new("test");
        let builder = WorkflowBuilder::default();
        let mut op = Operator::new(vec![(wf_id, builder)]);
        let exec_id = ExecutorId::default();
        let rx = op.start_workflow(exec_id, wf_id).await;
        drop(rx);
        let executors = op.executors;
        assert_eq!(
            match executors.get(&exec_id).unwrap() {
                State::Running(id, _, _) => id,
                _ => panic!(),
            },
            &wf_id
        );
    }

    #[tokio::test]
    async fn test_operator_get_next_node() {
        let wf_id = WorkflowId::new("test");
        let edge = Arc::new(Edge::new::<&str>());
        let node = Arc::new(Node::new_test(
            vec![edge.clone()],
            vec![],
            "node",
            Choice::All,
        ));
        let builder = WorkflowBuilder::default().add_node(node.clone()).unwrap();
        let mut op = Operator::new(vec![(wf_id, builder)]);
        let exec_id = ExecutorId::default();
        let wf_rx = op.start_workflow(exec_id, wf_id).await;
        drop(wf_rx);
        op.add_new_container(edge.clone(), exec_id, "test")
            .await
            .unwrap();
        op.enqueue_node_if_executable(&[edge], exec_id)
            .await
            .unwrap();
        let next = op.get_next_node().await.unwrap();
        assert_eq!(next, (node, exec_id));
    }

    #[tokio::test]
    async fn test_workflow_wait_finish() {
        let wf_id = WorkflowId::new("test");
        let edge = Arc::new(Edge::new::<&str>());
        let edge_to = Arc::new(Edge::new::<&str>());
        let node = Node::new(
            vec![edge.clone()],
            0,
            vec![edge_to.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut cons = op
                        .lock()
                        .await
                        .get_container(self_.inputs(), exec_id)
                        .await?;
                    let mut con = cons.pop_front().unwrap();
                    let _: &str = con.take().unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    let mut output_cons = VecDeque::new();
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store("test");
                    output_cons.push_back(con_clone);
                    op.lock()
                        .await
                        .add_container(self_.outputs(), exec_id, output_cons)
                        .await?;
                    Ok(())
                })
            }),
            false,
            "node",
            Choice::All,
        );
        let builder = WorkflowBuilder::default().add_node(Arc::new(node)).unwrap();
        let op = Arc::new(Mutex::new(Operator::new(vec![(wf_id, builder)])));
        let exec_id = ExecutorId::default();
        let wf_rx = op.lock().await.start_workflow(exec_id, wf_id).await;
        op.lock()
            .await
            .add_new_container(edge, exec_id, "test")
            .await
            .unwrap();
        let node = op.lock().await.get_next_node().await.unwrap().0;
        let f = node.run(op.clone(), exec_id);
        let is_finished = op.lock().await.is_finished(exec_id).await;
        assert!(!is_finished);
        assert!(op
            .lock()
            .await
            .get_container(&[edge_to.clone()], exec_id)
            .await
            .unwrap()
            .is_empty());
        assert!(f.await.is_ok());
        wf_rx.await.unwrap();
        assert_eq!(
            op.lock()
                .await
                .get_container(&[edge_to], exec_id)
                .await
                .unwrap()
                .len(),
            1
        );
        let is_finished = op.lock().await.is_finished(exec_id).await;
        assert!(is_finished);
    }
}
