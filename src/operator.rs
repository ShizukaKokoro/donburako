//! オペレーターモジュール

use crate::container::{Container, ContainerError, ContainerMap};
use crate::node::{Edge, Node};
use crate::workflow::Workflow;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

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

/// オペレーター
///
/// コンテナと実行IDごとのワークフローの状態を管理する。
#[derive(Debug, Default, Clone)]
pub struct Operator {
    containers: Arc<Mutex<ContainerMap>>,
}
impl Operator {
    /// 新しいコンテナの追加
    ///
    /// ワークフローの入り口となるエッジに対して、新しいコンテナを追加する。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `data` - データ
    pub async fn add_new_container<T: 'static + Send + Sync>(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), ContainerError> {
        self.containers
            .lock()
            .await
            .add_new_container(edge, exec_id, data)
    }

    /// ノードが実行できるか確認する
    ///
    /// ノードは全ての入力エッジに対して、コンテナが格納されることで実行可能となる。
    ///
    /// # Arguments
    ///
    /// * `node` - ノード
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
    /// * `wf` - ワークフロー
    ///
    /// # Returns
    ///
    /// 実行可能なノードの Vec
    pub async fn get_executable_nodes(
        &self,
        node: &Arc<Node>,
        wf: &Workflow,
        exec_id: ExecutorId,
    ) -> Vec<Arc<Node>> {
        self.containers
            .lock()
            .await
            .get_executable_nodes(node, wf, exec_id)
    }

    /// コンテナの取得
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
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
    /// * `container` - コンテナ
    pub async fn add_container(
        &self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        container: Container,
    ) -> Result<(), ContainerError> {
        self.containers
            .lock()
            .await
            .add_container(edge, exec_id, container)
    }
}
