//! 関数ノードモジュール
//!
//! 関数を引数に取るノードを定義する。

use super::edge::Edge;
use super::*;
use crate::operator::{ExecutorId, Operator};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type BoxedFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

type AsyncFn =
    dyn for<'a> Fn(&'a UserNode, &'a Operator, ExecutorId) -> BoxedFuture<'a> + Send + Sync;

/// ユーザー定義ノード
///
/// ユーザーが定義した任意の処理を実行するノード。
/// その処理を一つの関数だとした時に、その関数が受け取る引数の数だけ入力エッジが必要になる。
/// また、その関数が返す値の数だけ出力エッジが必要になる。
/// 全ての入力エッジにデータが来るまで、ノードは実行されない。
pub struct UserNode {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    pub(super) func: Box<AsyncFn>,
    is_blocking: bool,
}
impl UserNode {
    /// ノードの生成
    pub fn new(inputs: Vec<Arc<Edge>>, func: Box<AsyncFn>, is_blocking: bool) -> Self {
        UserNode {
            inputs,
            outputs: Vec::new(),
            func,
            is_blocking,
        }
    }

    /// テスト用のノードの生成
    #[cfg(test)]
    pub fn new_test(inputs: Vec<Arc<Edge>>) -> Self {
        UserNode {
            inputs,
            outputs: Vec::new(),
            func: Box::new(|_, _, _| Box::pin(async {})),
            is_blocking: false,
        }
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Arc<Edge> {
        let edge = Arc::new(Edge::new::<T>());
        self.outputs.push(edge.clone());
        edge
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Vec<Arc<Edge>> {
        &self.outputs
    }

    /// ブロッキングノードかどうか
    pub fn is_blocking(&self) -> bool {
        self.is_blocking
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::User(self), name)
    }

    pub(super) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        (self.func)(self, op, exec_id).await;
    }
}
impl std::fmt::Debug for UserNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserNode")
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn test_connect_nodes() {
        let mut node1 = UserNode::new_test(vec![]);
        let edge = node1.add_output::<i32>();
        assert_eq!(node1.outputs.len(), 1);
        let node2 = UserNode::new_test(vec![edge.clone()]);
        assert_eq!(node2.inputs.len(), 1);
        assert_eq!(edge.ty, TypeId::of::<i32>());
        assert_eq!(edge, node2.inputs[0]);
    }

    #[tokio::test]
    async fn test_node_run() {
        let exec_id = ExecutorId::new();
        let node = UserNode::new_test(vec![]).to_node("node");
        let op = Operator::new(vec![]);
        node.run(&op, exec_id).await;
    }
}
