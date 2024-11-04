//! コンテナモジュール
//!
//! データを運ぶためのコンテナを提供する。
//! 中に入っているデータの型は、決まっておらず、任意の型を格納し、任意の型を取り出すことができる。
//! ただし、取り出すデータの型は入れたデータの型と一致している必要がある。

use crate::edge::Edge;
use crate::node::{Choice, Node};
use crate::operator::ExecutorId;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};

/// コンテナエラー
#[derive(Debug, Error, PartialEq)]
pub enum ContainerError {
    /// コンテナ内の型の不一致
    #[error("Type is mismatched with the stored data")]
    TypeMismatch,

    /// コンテナ内にデータが存在しない
    #[error("Data is not found in the container")]
    DataNotFound,

    /// コンテナ内にデータが存在するのに複製しようとした
    #[error("Data is found in the container when cloning")]
    CloningWithData,

    /// イテレーションの最大数を超えた
    #[error("The iteration exceeds the maximum number")]
    MaxIteration,
}

/// コンテナ
///
/// データを格納するためのコンテナ。
#[derive(Default)]
pub struct Container {
    data: Option<Box<dyn Any + 'static + Send + Sync>>,
    ty: Option<TypeId>,
}
impl Container {
    /// コンテナ内にデータが格納されているか確認する
    fn has_data(&self) -> bool {
        match (self.data.is_some(), self.ty.is_some()) {
            (true, true) => true,
            (false, false) => false,
            _ => unreachable!(),
        }
    }

    /// データを格納する
    ///
    /// # Arguments
    ///
    /// * `data` - 格納するデータ
    pub fn store<T: 'static + Send + Sync>(&mut self, data: T) {
        self.data = Some(Box::new(data));
        self.ty = Some(TypeId::of::<T>());
    }

    /// データを取り出す
    ///
    /// # Returns
    ///
    /// 格納されているデータ
    pub fn take<T: 'static + Send + Sync>(&mut self) -> Result<T, ContainerError> {
        if let Some(data) = self.data.take() {
            if let Some(ty) = self.ty {
                if ty == TypeId::of::<T>() {
                    if let Ok(data) = data.downcast::<T>() {
                        self.ty = None;
                        return Ok(*data);
                    }
                } else {
                    return Err(ContainerError::TypeMismatch);
                }
            }
        }
        Err(ContainerError::DataNotFound)
    }

    /// コンテナを複製する
    pub fn clone_container(&self) -> Result<Self, ContainerError> {
        if self.has_data() {
            Err(ContainerError::CloningWithData)
        } else {
            Ok(Self {
                data: None,
                ty: None,
            })
        }
    }

    /// データをとにかく取り出す
    pub(crate) fn take_anyway(&mut self) {
        let _ = self.data.take();
        let _ = self.ty.take();
    }
}
impl std::fmt::Debug for Container {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.has_data() {
            write!(f, "Container {{ data: Some(Any {{ .. }}) }}")
        } else {
            write!(f, "Container {{ data: None }}")
        }
    }
}
impl Drop for Container {
    #[tracing::instrument]
    fn drop(&mut self) {
        #[cfg(not(test))]
        if self.has_data() {
            tracing::error!("Drop Container");
            unreachable!("Container is dropped with data (type: {:?})", self.ty);
        }
    }
}

/// コンテナマップ
///
/// コンテナを管理するためのマップ。
/// 次に向かうべきエッジをキーにして、コンテナを格納する。
/// 外部からはエッジを参照して、コンテナを取り出すことができる。
#[derive(Default, Debug)]
pub(crate) struct ContainerMap(HashMap<ExecutorId, HashMap<Arc<Edge>, Option<Container>>>);
impl ContainerMap {
    /// 新しいコンテナマップを確保する
    #[tracing::instrument(skip(self))]
    pub(crate) fn entry_by_exec_id(&mut self, exec_id: ExecutorId) {
        debug!("Entry by exec_id");
        let _ = self.0.entry(exec_id).or_default();
    }

    /// コンテナマップがあるかどうか
    #[tracing::instrument(skip(self))]
    pub(crate) fn is_running(&self, exec_id: ExecutorId) -> bool {
        self.0.contains_key(&exec_id)
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
    pub(crate) fn add_new_container<T: 'static + Send + Sync>(
        &mut self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        data: T,
    ) -> Result<(), ContainerError> {
        if !edge.check_type::<T>() {
            return Err(ContainerError::TypeMismatch);
        }
        let mut container = Container::default();
        container.store(data);
        self.add_container(edge, exec_id, container)?;
        Ok(())
    }

    /// 実行準備ができているか確認する
    ///
    /// # Arguments
    ///
    /// * `node` - ノード
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 実行準備ができている場合は true、そうでない場合は false
    pub(crate) fn is_ready(&self, node: &Arc<Node>, exec_id: ExecutorId) -> bool {
        match node.choice() {
            Choice::All => node
                .inputs()
                .iter()
                .all(|e| self.check_edge_exists(e.clone(), exec_id)),
            Choice::Any => node
                .inputs()
                .iter()
                .any(|e| self.check_edge_exists(e.clone(), exec_id)),
        }
    }

    /// エッジに対応するコンテナが存在するか確認する
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    ///
    /// # Returns
    ///
    /// 存在する場合は true、そうでない場合は false
    fn check_edge_exists(&self, edge: Arc<Edge>, exec_id: ExecutorId) -> bool {
        self.0.contains_key(&exec_id) && self.0[&exec_id].get(&edge).unwrap_or(&None).is_some()
    }

    /// 実行IDに対応するエッジが存在するか確認する
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    /// * `edges` - エッジ
    ///
    /// # Returns
    ///
    /// 全て存在する場合は true、そうでない場合は false
    pub(crate) fn check_edges_exists_in_exec_id(
        &self,
        exec_id: ExecutorId,
        edges: &[Arc<Edge>],
    ) -> bool {
        if let Some(map) = self.0.get(&exec_id) {
            edges.iter().all(|e| map.contains_key(e))
        } else {
            false
        }
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
    pub(crate) fn get_container(
        &mut self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
    ) -> Option<Container> {
        self.0.get_mut(&exec_id)?.get_mut(&edge)?.take()
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
    #[tracing::instrument(skip(self))]
    pub(crate) fn add_container(
        &mut self,
        edge: Arc<Edge>,
        exec_id: ExecutorId,
        mut container: Container,
    ) -> Result<(), ContainerError> {
        if !container.has_data() {
            return Err(ContainerError::DataNotFound);
        }
        if let Some(map) = self.0.get_mut(&exec_id) {
            if map.contains_key(&edge) {
                debug!("Container is already exists");
                return Ok(());
            }
            assert!(map.insert(edge, Some(container)).is_none());
        } else {
            debug!("Container is dropped safely");
            container.take_anyway();
        }
        Ok(())
    }

    /// 空の枠の追加
    ///
    /// 無視されるエッジに対して、実行完了を示すための空の枠を追加する。
    /// 通常のコンテナだと、データを取得されないためデータが中に残ってしまうが、何も格納しないとノードが終了したのかどうかがわからない。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub(crate) fn add_empty(&mut self, edge: Arc<Edge>, exec_id: ExecutorId) {
        if let Some(map) = self.0.get_mut(&exec_id) {
            let _ = map.insert(edge, None);
        }
    }

    /// 特定の実行IDに対応するコンテナを全て終了処理する
    ///
    /// # Arguments
    ///
    /// * `exec_id` - 実行ID
    #[tracing::instrument(skip(self))]
    pub(crate) fn finish_containers(&mut self, exec_id: ExecutorId) {
        if let Some(con_map) = self.0.get_mut(&exec_id) {
            for (_, container) in con_map.iter_mut() {
                if let Some(mut container) = container.take() {
                    container.take_anyway();
                }
            }
            let _ = self.0.remove(&exec_id);
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn is_empty(&self) -> bool {
        self.0
            .values()
            .flat_map(|map| map.values())
            .all(|c| c.is_none())
    }

    /// 中身の入っているコンテナの数を取得する
    #[cfg(feature = "dev")]
    #[tracing::instrument(skip(self))]
    pub(crate) fn remaining_containers(&self) -> usize {
        self.0
            .values()
            .flat_map(|map| map.values())
            .filter(|c| c.is_some())
            .count()
    }
}
impl Drop for ContainerMap {
    fn drop(&mut self) {
        for (exec_id, con_map) in self.0.iter_mut() {
            for (edge, container) in con_map.iter_mut() {
                if let Some(mut container) = container.take() {
                    warn!(
                        "ContainerMap is dropped with data (edge: {:?}, exec_id: {:?})",
                        edge, exec_id
                    );
                    container.take_anyway();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_container_init() {
        let container = Container::default();

        assert!(container.data.is_none());
        assert!(container.ty.is_none());
    }

    #[test]
    fn test_container_store() {
        let mut container = Container::default();
        container.store(42);

        assert!(container.data.is_some());
        assert!(container.ty.is_some());
    }

    #[test]
    fn test_container_take() {
        let mut container = Container::default();
        container.store(42);

        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }

    #[test]
    fn test_container_has_map() {
        let mut container = Container::default();
        assert!(!container.has_data());

        container.store(42);
        assert!(container.has_data());

        let _ = container.take::<i32>();
        assert!(!container.has_data());
    }

    #[test]
    fn test_container_take_type_mismatch() {
        let mut container = Container::default();
        container.store(42);

        let result = container.take::<&str>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ContainerError::TypeMismatch);
    }

    #[test]
    fn test_container_take_data_not_found() {
        let mut container = Container::default();

        let result = container.take::<i32>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ContainerError::DataNotFound);
    }

    #[test]
    fn test_container_debug() {
        let mut container = Container::default();
        container.store(42);

        let debug = format!("{:?}", container);
        assert_eq!(debug, "Container { data: Some(Any { .. }) }");

        let container = Container::default();
        let debug = format!("{:?}", container);
        assert_eq!(debug, "Container { data: None }");
    }

    #[tokio::test]
    async fn test_container_map_add_new_container() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        let edge = Arc::new(Edge::new::<i32>());
        let result = map.add_new_container(edge.clone(), exec_id, 42);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_container_map_check_edge_exists() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        map.entry_by_exec_id(exec_id);
        let edge = Arc::new(Edge::new::<i32>());
        map.add_new_container(edge.clone(), exec_id, 42).unwrap();

        assert!(map.check_edge_exists(edge, exec_id));

        let edge = Arc::new(Edge::new::<&str>());
        assert!(!map.check_edge_exists(edge, exec_id));
    }

    #[tokio::test]
    async fn test_container_map_is_ready_all() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        map.entry_by_exec_id(exec_id);
        let edge0 = Arc::new(Edge::new::<i32>());
        let edge1 = Arc::new(Edge::new::<&str>());
        map.add_new_container(edge0.clone(), exec_id, 42).unwrap();

        let node = Arc::new(Node::new_test(
            vec![edge0.clone(), edge1.clone()],
            vec![],
            "node",
            Choice::All,
        ));
        assert!(!map.is_ready(&node, exec_id));

        map.add_new_container(edge1.clone(), exec_id, "42").unwrap();
        assert!(map.is_ready(&node, exec_id));
    }

    #[tokio::test]
    async fn test_container_map_is_ready_any() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        map.entry_by_exec_id(exec_id);
        let edge0 = Arc::new(Edge::new::<i32>());
        let edge1 = Arc::new(Edge::new::<&str>());
        map.add_new_container(edge0.clone(), exec_id, 42).unwrap();

        let node = Arc::new(Node::new_test(
            vec![edge0.clone(), edge1.clone()],
            vec![],
            "node",
            Choice::Any,
        ));
        assert!(map.is_ready(&node, exec_id));
    }

    #[tokio::test]
    async fn test_container_map_get_container() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        map.entry_by_exec_id(exec_id);
        let edge = Arc::new(Edge::new::<i32>());
        map.add_new_container(edge.clone(), exec_id, 42).unwrap();

        let container = map.get_container(edge, exec_id);
        assert!(container.is_some());
        let mut container = container.unwrap();
        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }

    #[tokio::test]
    async fn test_container_map_add_container() {
        let exec_id = ExecutorId::new();
        let mut map = ContainerMap::default();
        map.entry_by_exec_id(exec_id);
        let edge = Arc::new(Edge::new::<i32>());
        let mut container = Container::default();
        container.store(42);
        map.add_container(edge.clone(), exec_id, container).unwrap();

        let container = map.get_container(edge, exec_id);
        assert!(container.is_some());
        let mut container = container.unwrap();
        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }
}
