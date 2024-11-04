//! オペレーターモジュール

use crate::channel::{ExecutorMessage, ExecutorTx, WfMessage, WorkflowTx};
use crate::container::{Container, ContainerError, ContainerMap};
use crate::edge::Edge;
use crate::node::{Node, NodeError};
use crate::workflow::{Workflow, WorkflowBuilder, WorkflowId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task::{spawn, spawn_blocking, JoinHandle};
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
    #[tracing::instrument(skip(self))]
    fn push(&mut self, node: Arc<Node>, exec_id: ExecutorId) -> bool {
        trace!("Push node");
        if self.set.insert((node.clone(), exec_id)) {
            self.queue.push_back((node.clone(), exec_id));
            true
        } else {
            false
        }
    }

    /// ノードの取得
    #[tracing::instrument(skip(self))]
    fn pop(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        if let Some(item) = self.queue.pop_front() {
            assert!(self.set.remove(&item));
            trace!("Pop node");
            Some(item)
        } else {
            None
        }
    }

    /// キューが空か確認
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
impl Drop for ExecutableQueue {
    fn drop(&mut self) {
        assert!(self.queue.is_empty());
        assert!(self.set.is_empty());
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
    async fn end(&mut self, exec_id: ExecutorId, is_safe: bool) {
        if let Some((_, wf_tx)) = self.0.remove(&exec_id) {
            debug!("Send end message");
            let _ = wf_tx
                .send(if is_safe {
                    WfMessage::Done(exec_id)
                } else {
                    WfMessage::Error(exec_id)
                })
                .await;
        }
    }

    fn is_running(&self, exec_id: &ExecutorId) -> bool {
        self.0.contains_key(exec_id)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl Drop for StatusMap {
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        assert!(self.0.is_empty());
    }
}

type Handler = (JoinHandle<Result<&'static str, NodeError>>, ExecutorId);

/// ハンドラ管理
#[derive(Debug)]
struct Handlers {
    handles: Vec<Option<Handler>>,
    retains: VecDeque<usize>,
}
impl Handlers {
    fn new(n: usize) -> Self {
        let mut handles = Vec::with_capacity(n);
        let mut retains = VecDeque::new();
        for i in 0..n {
            retains.push_back(i);
            handles.push(None);
        }
        assert_eq!(handles.len(), retains.len());
        Self { handles, retains }
    }

    #[tracing::instrument(skip(self, handle))]
    fn push(
        &mut self,
        key: usize,
        handle: JoinHandle<Result<&'static str, NodeError>>,
        exec_id: ExecutorId,
    ) {
        assert_eq!(self.retains.pop_front().unwrap(), key);
        self.handles[key] = Some((handle, exec_id));
        #[cfg(feature = "dev")]
        trace!(
            "{:?} tasks are running(push)",
            self.handles.len() - self.retains.len()
        );
    }

    #[tracing::instrument(skip(self, status))]
    async fn remove(
        &mut self,
        key: usize,
        status: &StatusMap,
    ) -> (ExecutorId, Option<&'static str>) {
        self.retains.push_back(key);
        let (handle, exec_id) = self.handles[key].take().unwrap();
        let mut result = None;
        // 実行中でない場合はノードの実行結果に関心がないため、結果を取得しない
        if status.is_running(&exec_id) {
            match handle.await.unwrap() {
                Ok(r) => result = Some(r),
                Err(e) => {
                    warn!("Finish node with error: {:?}", e);
                }
            };
        } else {
            let _ = handle.await;
        }
        #[cfg(feature = "dev")]
        trace!(
            "{:?} tasks are running(remove)",
            self.handles.len() - self.retains.len()
        );
        (exec_id, result)
    }

    #[tracing::instrument(skip(self))]
    fn has_retain(&mut self) -> Option<usize> {
        self.retains.front().cloned()
    }

    #[tracing::instrument(skip(self))]
    async fn check_handles(&mut self) -> HashSet<ExecutorId> {
        let mut finished = HashSet::new();
        for (key, handle) in self.handles.iter_mut().enumerate() {
            if let Some((h, exec_id)) = handle.take() {
                if h.is_finished() {
                    match h.await {
                        Ok(result) => {
                            assert!(handle.replace((spawn(async { result }), exec_id)).is_none());
                        }
                        Err(e) => {
                            warn!("Handle error: {:?} ({:?})", e, exec_id);
                            let _ = finished.insert(exec_id);
                            self.retains.push_back(key);
                            debug!("Done({})", key);
                        }
                    }
                } else {
                    assert!(handle.replace((h, exec_id)).is_none());
                }
            }
        }
        finished
    }
}
impl Drop for Handlers {
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        assert!(self.handles.iter().all(|h| h.is_none()));
        assert_eq!(self.handles.len(), self.retains.len());
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
    handlers: Handlers,
    containers: ContainerMap,
    queue: ExecutableQueue,
    #[cfg(feature = "dev")]
    timer: HashMap<ExecutorId, std::time::Instant>,
}
impl Operator {
    /// 新しいオペレーターの生成
    ///
    /// # Arguments
    ///
    /// * `exec_tx` - エグゼキューターの送信チャンネル
    /// * `builders` - ワークフロービルダーのリスト
    /// * `handler_num` - 一度に実行されるハンドラーの数
    pub(crate) fn new(
        exec_tx: ExecutorTx,
        builders: Vec<(WorkflowId, WorkflowBuilder)>,
        handler_num: usize,
    ) -> Self {
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
            handlers: Handlers::new(handler_num),
            #[cfg(feature = "dev")]
            timer: HashMap::new(),
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
    #[tracing::instrument(skip(self, wf_tx))]
    pub async fn start_workflow(&mut self, wf_id: WorkflowId, wf_tx: WorkflowTx) -> ExecutorId {
        let exec_id = ExecutorId::new();
        #[cfg(feature = "dev")]
        {
            tracing::info!("Start workflow: {:?}", exec_id);
            let _ = self.timer.insert(exec_id, std::time::Instant::now());
        }
        self.status.start(exec_id, wf_id, wf_tx);
        self.containers.entry_by_exec_id(exec_id);
        let mut flag = false;
        for node in self.workflows[&wf_id].start_nodes() {
            flag |= self.queue.push(node.clone(), exec_id);
        }
        if flag {
            self.exec_tx.send(ExecutorMessage::Start).unwrap();
        }
        exec_id
    }

    /// ノードの実行処理
    ///
    /// # Arguments
    ///
    /// * `exec_tx` - エグゼキューターの送信チャンネル
    /// * `op` - オペレーター
    #[tracing::instrument(skip(self))]
    pub(crate) async fn process(&mut self, op: &Arc<tokio::sync::Mutex<Self>>) {
        if let Some(key) = self.handlers.has_retain() {
            if let Some((node, exec_id)) = self.next_node() {
                let tx_clone = self.exec_tx.clone();
                let op_clone = op.clone();
                let handle = if node.is_blocking() {
                    let rt_handle = Handle::current();
                    spawn_blocking(move || {
                        rt_handle.block_on(async {
                            let result = node.run(op_clone, exec_id).await;
                            let _ = tx_clone.send(ExecutorMessage::Done(key));
                            result
                        })?;
                        Ok(node.name())
                    })
                } else {
                    spawn(async move {
                        let result = node.run(op_clone, exec_id).await;
                        let _ = tx_clone.send(ExecutorMessage::Done(key));
                        result?;
                        Ok(node.name())
                    })
                };
                self.handlers.push(key, handle, exec_id);
            }
        } else {
            debug!("No retain");
        }
    }

    /// 次に実行するノードの取得
    pub fn next_node(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        while !self.queue.is_empty() {
            if let Some((node, exec_id)) = self.queue.pop() {
                if !self.status.is_running(&exec_id) {
                    continue;
                }
                #[cfg(feature = "dev")]
                self.check_timer(&exec_id);
                return Some((node, exec_id));
            }
        }
        None
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
        trace!("Add new container");
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
        trace!("Get container");
        let mut containers = VecDeque::new();
        if edge.is_empty() {
            return Ok(containers);
        }
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
        let wf_id = {
            if let Some(wf_id) = self.status.get_workflow_id(&exec_id) {
                wf_id
            } else {
                for mut c in container {
                    c.take_anyway();
                }
                return Err(OperatorError::NotRunning(exec_id));
            }
        };
        trace!("Add container: {:?}", container);
        for (e, mut c) in edges.iter().zip(container) {
            if self.workflows[wf_id].is_ignored(e) {
                trace!("Ignore edge");
                self.containers.add_empty(e.clone(), exec_id);
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
    async fn check_executable_nodes(&mut self, edges: &[Arc<Edge>], exec_id: ExecutorId) {
        trace!("Check executable nodes");
        let mut flag = false;
        for e in edges {
            let wf_id = self.status.get_workflow_id(&exec_id).unwrap();
            if let Some(node) = self.workflows[wf_id].get_node(e) {
                if self.containers.is_ready(&node, exec_id) {
                    flag |= self.queue.push(node, exec_id);
                }
            }
        }
        if flag {
            self.exec_tx.send(ExecutorMessage::Update).unwrap();
            #[cfg(feature = "dev")]
            self.check_timer(&exec_id);
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
    async fn is_finished(&mut self, exec_id: ExecutorId) -> bool {
        trace!("Check finished");
        if let Some(wf_id) = self.status.get_workflow_id(&exec_id) {
            let edges = self.workflows[wf_id].end_edges();
            if self
                .containers
                .check_edges_exists_in_exec_id(exec_id, edges)
            {
                debug!("The workflow is finished");
                self.status.end(exec_id, true).await; // TODO: ほんとに true?
                true
            } else {
                false
            }
        } else {
            true
        }
    }

    /// ノードの終了プロセス
    ///
    /// ノードの終了処理を行う。
    ///
    /// # Arguments
    ///
    /// * `key` - ハンドラーのキー
    ///
    /// # Returns
    ///
    /// ワークフローが終了している場合は true
    #[tracing::instrument(skip(self))]
    pub(crate) async fn finish_node(&mut self, key: usize) -> bool {
        let (exec_id, result) = self.handlers.remove(key, &self.status).await;
        match result {
            Some(name) => {
                debug!("Finish node: {:?}", name);
            }
            None => {
                debug!("Finish node with error");
                self.status.end(exec_id, false).await;
            }
        }
        if self.is_finished(exec_id).await {
            #[cfg(feature = "dev")]
            self.stop_timer(&exec_id);
            debug!("Finish workflow: {:?}", exec_id);
            true
        } else {
            false
        }
    }

    /// ハンドルの定期確認
    ///
    /// ハンドルの状態を確認し、終了しているハンドルの実行を終了する。
    ///
    /// # Returns
    ///
    /// ワークフローが終了している場合は true
    #[tracing::instrument(skip(self))]
    pub(crate) async fn check_handles(&mut self) {
        for exec_id in self.handlers.check_handles().await {
            self.containers.finish_containers(exec_id);
            self.status.end(exec_id, false).await;
        }
    }

    /// ワークフローの強制終了
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub async fn finish_workflow_by_execute_id(&mut self, exec_id: ExecutorId, is_safe: bool) {
        debug!("Finish workflow");
        self.containers.finish_containers(exec_id);
        self.status.end(exec_id, is_safe).await;
    }

    /// ワークフローが全て終了しているか確認
    pub(crate) fn is_all_finished(&self) -> bool {
        #[cfg(feature = "dev")]
        {
            trace!("Check all finished {:?}", self.status);
        }
        self.status.is_empty()
    }

    pub(crate) fn send_update(&self) {
        let _ = self.exec_tx.send(ExecutorMessage::Update);
    }

    #[cfg(feature = "dev")]
    #[tracing::instrument(skip(self))]
    fn check_timer(&self, exec_id: &ExecutorId) {
        if let Some(start) = self.timer.get(exec_id) {
            tracing::trace!("{:?} is at {:?}", exec_id, start.elapsed());
        }
    }

    #[cfg(feature = "dev")]
    #[tracing::instrument(skip(self))]
    pub(crate) fn stop_timer(&mut self, exec_id: &ExecutorId) {
        if let Some(start) = self.timer.remove(exec_id) {
            let end = std::time::Instant::now();
            let duration = end - start;
            tracing::debug!("{:?} is finished in {:?}", exec_id, duration);
        }
    }

    #[cfg(feature = "dev")]
    #[tracing::instrument(skip(self))]
    pub(crate) fn running_tasks(&self) -> usize {
        self.handlers.handles.len() - self.handlers.retains.len()
    }
}
impl Drop for Operator {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        #[cfg(feature = "dev")]
        trace!("Drop operator");
    }
}
