//! オペレーターモジュール

use crate::container::{Container, ContainerError, ContainerMap};
use crate::node::{Edge, Node};
use crate::workflow::{Workflow, WorkflowBuilder};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

/// オペレーターエラー
#[derive(Debug, Error, PartialEq)]
pub enum OperatorError {
    /// コンテナエラー
    #[error("Container error")]
    ContainerError(#[from] ContainerError),
}

/// 実行ID
///
/// TODO: 後で Default トレイトを削除する。
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
pub struct ExecutorId(Uuid);
impl ExecutorId {
    /// 実行IDの生成
    ///
    /// TODO: 後でこの関数は隠蔽される。
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// 実行可能なノードのキュー
#[derive(Debug, Default)]
pub struct ExecutableQueue {
    queue: VecDeque<(Arc<Node>, ExecutorId)>,
    set: HashSet<(Arc<Node>, ExecutorId)>,
}
impl ExecutableQueue {
    /// 新しい実行可能なノードのキューの生成
    pub fn push(&mut self, node: Arc<Node>, exec_id: ExecutorId) {
        if self.set.insert((node.clone(), exec_id)) {
            self.queue.push_back((node.clone(), exec_id));
        }
    }

    /// ノードの取得
    pub fn pop(&mut self) -> Option<(Arc<Node>, ExecutorId)> {
        let item = self.queue.pop_front();
        assert!(self.set.remove(&item.clone().unwrap()));
        item
    }
}

/// 状態
#[derive(Debug, PartialEq, Eq)]
pub enum State {
    /// 実行中
    Running(usize),
    /// 終了
    Finished(usize),
}

/// オペレーター
///
/// コンテナと実行IDごとのワークフローの状態を管理する。
#[derive(Debug, Clone)]
pub struct Operator {
    workflows: Arc<Vec<Workflow>>,
    containers: Arc<Mutex<ContainerMap>>,
    executors: Arc<Mutex<HashMap<ExecutorId, State>>>,
}
impl Operator {
    /// 新しいオペレーターの生成
    ///
    /// # Arguments
    ///
    /// * `builders` - ワークフロービルダーのリスト
    pub fn new(builders: Vec<WorkflowBuilder>) -> Self {
        let workflows = builders
            .into_iter()
            .map(|builder| builder.build())
            .collect();
        Self {
            workflows: Arc::new(workflows),
            containers: Arc::new(Mutex::new(ContainerMap::default())),
            executors: Arc::new(Mutex::new(HashMap::new())),
        }
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
    pub async fn add_new_container<T: 'static + Send + Sync>(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), OperatorError> {
        Ok(self
            .containers
            .lock()
            .await
            .add_new_container(edge, exec_id, data)?)
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
    pub async fn check_node_executable(&self, node: &Arc<Node>, exec_id: ExecutorId) -> bool {
        self.containers
            .lock()
            .await
            .check_node_executable(node, exec_id)
    }

    /// 実行が可能なノードを取得する
    ///
    /// # Arguments
    ///
    /// * `node` - 終了したノード
    /// * `exec_id` - 実行ID
    /// * `wf` - ワークフロー
    ///
    /// # Returns
    ///
    /// 実行可能なノードの Vec
    pub async fn get_executable_nodes(
        &self,
        node: &Arc<Node>,
        exec_id: ExecutorId,
        index: usize,
    ) -> Vec<Arc<Node>> {
        self.containers.lock().await.get_executable_nodes(
            node,
            exec_id,
            self.workflows.get(index).unwrap(),
        )
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
        Ok(self
            .containers
            .lock()
            .await
            .add_container(edge, exec_id, container)?)
    }

    /// ワークフローの実行開始
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    /// * `index` - ワークフローのインデックス
    pub async fn start_workflow(&self, exec_id: ExecutorId, index: usize) {
        let _ = self
            .executors
            .lock()
            .await
            .insert(exec_id, State::Running(index));
    }

    /// ワークフローの取得
    ///
    /// TODO: この関数は後で削除する。
    pub fn get_workflow(&self, index: usize) -> &Workflow {
        self.workflows.get(index).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_operator_start_workflow() {
        let op = Operator::new(vec![]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, 0).await;
        let executors = op.executors.lock().await;
        assert_eq!(executors.get(&exec_id), Some(&State::Running(0)));
    }
}
