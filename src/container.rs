//! コンテナモジュール
//!
//! データを運ぶためのコンテナを提供する。
//! 中に入っているデータの型は、決まっておらず、任意の型を格納し、任意の型を取り出すことができる。
//! ただし、取り出すデータの型は入れたデータの型と一致している必要がある。

// TODO: コンテナが、他のワークフローの実行中のコンテナと競合しないように ID を紐づける
// TODO: 同じワークフロー内の異なるイテレーションで競合しないように、イテレーション番号を紐づける(0 から始まる usize)

use crate::node::{Edge, Node, NodeType};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
}

/// キャンセルスタック
#[derive(Default, Clone)]
pub struct CancelStack(Vec<CancellationToken>);
impl CancelStack {
    /// キャンセルトークンを追加する
    ///
    /// # Arguments
    ///
    /// * `token` - キャンセルトークン
    pub fn push(&mut self, token: CancellationToken) {
        self.0.push(token);
    }

    /// キャンセルを確認する
    pub fn check(&mut self) -> bool {
        if let Some(token) = self.0.last() {
            if token.is_cancelled() {
                let _ = self.0.pop();
                return true;
            }
        }
        false
    }

    /// キャンセル
    pub fn cancel(&mut self) {
        let token = self.0.pop();
        if let Some(token) = token {
            token.cancel();
        }
    }
}
impl std::fmt::Debug for CancelStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelStack")
            .field("len", &self.0.len())
            .finish()
    }
}

/// コンテナ
///
/// データを格納するためのコンテナ。
#[derive(Default)]
pub struct Container {
    data: Option<Box<dyn Any + 'static + Send + Sync>>,
    ty: Option<TypeId>,
    stack: CancelStack,
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
                stack: self.stack.clone(),
            })
        }
    }

    /// キャンセルトークンを追加する
    ///
    /// # Arguments
    ///
    /// * `token` - キャンセルトークン
    pub fn push_cancel_token(&mut self, token: CancellationToken) {
        self.stack.push(token);
    }

    /// キャンセルする
    pub fn cancel(&mut self) {
        self.stack.cancel();
    }

    /// キャンセルを確認する
    pub fn check_cancel(&mut self) -> bool {
        self.stack.check()
    }
}
impl std::fmt::Debug for Container {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Container")
            .field("data", &self.data)
            .field("stack", &self.stack)
            .finish()
    }
}
impl Drop for Container {
    fn drop(&mut self) {
        #[cfg(not(test))]
        if self.has_data() {
            unreachable!();
        }
    }
}

/// コンテナマップ
///
/// コンテナを管理するためのマップ。
/// 次に向かうべきエッジをキーにして、コンテナを格納する。
/// 外部からはエッジを参照して、コンテナを取り出すことができる。
///
/// 内部に状態をもつため、実行順に影響を受ける。
#[derive(Default, Debug, Clone)]
pub struct ContainerMap {
    // TODO: コンテナの ID を参照して、コンテナを貯められるようにする
    map: Arc<Mutex<HashMap<Rc<Edge>, Container>>>,
    // counter: AtomicUsize,
}
impl ContainerMap {
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
        edge: Rc<Edge>,
        data: T,
    ) -> Result<(), ContainerError> {
        if !edge.check_type::<T>() {
            return Err(ContainerError::TypeMismatch);
        }
        let mut container = Container::default();
        container.store(data);
        let _ = self.map.lock().await.insert(edge, container);
        Ok(())
    }

    /// エッジに対応するコンテナが存在するか確認する
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    ///
    /// # Returns
    ///
    /// 存在する場合は true、そうでない場合は false
    async fn check_edge_exists(&self, edge: &Rc<Edge>) -> bool {
        self.map.lock().await.contains_key(edge)
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
    pub async fn check_node_executable(&self, node: &Rc<Node>) -> bool {
        match node.kind() {
            NodeType::User(node) => {
                let mut result = true;
                for edge in node.inputs() {
                    if !self.check_edge_exists(edge).await {
                        result = false;
                        break;
                    }
                }
                result
            }
        }
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
    pub async fn get_container(&self, edge: &Rc<Edge>) -> Option<Container> {
        self.map.lock().await.remove(edge)
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
        edge: Rc<Edge>,
        container: Container,
    ) -> Result<(), ContainerError> {
        if !container.has_data() {
            return Err(ContainerError::DataNotFound);
        }
        let _ = self.map.lock().await.insert(edge, container);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Edge, Node, NodeType, UserNode};
    use pretty_assertions::assert_eq;

    #[test]
    fn test_cancel_stack_push() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());

        assert_eq!(stack.0.len(), 1);
    }

    #[test]
    fn test_cancel_stack_check() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());

        assert!(!stack.check());
    }

    #[test]
    fn test_cancel_stack_check_canceled() {
        let mut stack = CancelStack::default();
        let token = CancellationToken::new();
        stack.push(token.clone());
        token.cancel();

        assert!(stack.check());
        assert_eq!(stack.0.len(), 0);
    }

    #[test]
    fn test_cancel_stack_cancel() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());
        stack.cancel();

        assert_eq!(stack.0.len(), 0);
    }

    #[test]
    fn test_cancel_stack_cancel_empty() {
        let mut stack = CancelStack::default();
        stack.cancel();

        assert_eq!(stack.0.len(), 0);
    }

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
    fn test_container_clone_container() {
        let mut container = Container::default();
        let result = container.clone_container();
        assert!(result.is_ok());

        container.store(42);

        let result = container.clone_container();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ContainerError::CloningWithData);
    }

    #[test]
    fn test_container_debug() {
        let mut container = Container::default();
        container.store(42);

        let debug = format!("{:?}", container);
        assert_eq!(
            debug,
            "Container { data: Some(Any { .. }), stack: CancelStack { len: 0 } }"
        );

        let container = Container::default();
        let debug = format!("{:?}", container);
        assert_eq!(
            debug,
            "Container { data: None, stack: CancelStack { len: 0 } }"
        );
    }

    #[test]
    fn test_container_cancel() {
        let mut con0 = Container::default();
        con0.push_cancel_token(CancellationToken::new());
        let mut con1 = con0.clone_container().unwrap();
        assert_eq!(con1.stack.0.len(), 1);
        assert!(!con1.check_cancel());
        con0.cancel();
        assert_eq!(con0.stack.0.len(), 0);
        assert_eq!(con1.stack.0.len(), 1);
        assert!(con1.check_cancel());
    }

    #[tokio::test]
    async fn test_container_map_add_new_container() {
        let map = ContainerMap::default();
        let edge = Rc::new(Edge::new::<i32>());
        let result = map.add_new_container(edge.clone(), 42).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_container_map_check_edge_exists() {
        let map = ContainerMap::default();
        let edge = Rc::new(Edge::new::<i32>());
        map.add_new_container(edge.clone(), 42).await.unwrap();

        assert!(map.check_edge_exists(&edge).await);

        let edge = Rc::new(Edge::new::<&str>());
        assert!(!map.check_edge_exists(&edge).await);
    }

    #[tokio::test]
    async fn test_container_map_check_node_executable_user() {
        let map = ContainerMap::default();
        let edge0 = Rc::new(Edge::new::<i32>());
        let edge1 = Rc::new(Edge::new::<&str>());
        map.add_new_container(edge0.clone(), 42).await.unwrap();

        let node = Rc::new(Node::new(NodeType::User(UserNode::new_test(vec![
            edge0.clone(),
            edge1.clone(),
        ]))));
        assert!(!map.check_node_executable(&node).await);

        map.add_new_container(edge1.clone(), "42").await.unwrap();
        assert!(map.check_node_executable(&node).await);
    }

    #[tokio::test]
    async fn test_container_map_get_container() {
        let map = ContainerMap::default();
        let edge = Rc::new(Edge::new::<i32>());
        map.add_new_container(edge.clone(), 42).await.unwrap();

        let container = map.get_container(&edge).await;
        assert!(container.is_some());
        let mut container = container.unwrap();
        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }

    #[tokio::test]
    async fn test_container_map_add_container() {
        let map = ContainerMap::default();
        let edge = Rc::new(Edge::new::<i32>());
        let mut container = Container::default();
        container.store(42);
        map.add_container(edge.clone(), container).await.unwrap();

        let container = map.get_container(&edge).await;
        assert!(container.is_some());
        let mut container = container.unwrap();
        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }
}
