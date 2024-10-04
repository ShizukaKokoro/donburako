//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

use crate::operator::{ExecutorId, Operator};
use std::any::TypeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// ノードエラー
#[derive(Debug, Error, PartialEq)]
pub enum NodeError {
    /// すでに出力エッジが存在する
    #[error("Output edge already exists")]
    OutputEdgeExists,

    /// エッジの型が一致しない
    #[error("First choice node must have the same type in the input edges")]
    EdgeTypeMismatch,
}

/// ノードID
#[derive(Default, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(Uuid);
impl NodeId {
    /// ノードIDの生成
    pub fn new() -> Self {
        NodeId(Uuid::new_v4())
    }
}

/// ノード
///
/// NOTE: サイズが大きめ？
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    kind: NodeType,
    name: &'static str,
}
impl Node {
    /// ノードの生成
    pub fn new(kind: NodeType, name: &'static str) -> Self {
        Node {
            id: NodeId::new(),
            kind,
            name,
        }
    }

    /// ノードの実行
    ///
    /// ノードの種類に応じた処理を実行する。
    ///
    /// # Arguments
    ///
    /// * `op` - オペレーター
    pub async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        match &self.kind {
            NodeType::User(node) => {
                let func = &node.func;
                func(node, op, exec_id).await;
            }
            NodeType::FirstChoice(node) => {
                for edge in node.inputs() {
                    let con = op.get_container(edge.clone(), exec_id).await;
                    if con.is_some() {
                        let con = con.unwrap();
                        op.add_container(edge.clone(), exec_id, con).await.unwrap();
                        break;
                    }
                }
            }
        }
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> Vec<Arc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.inputs().clone(),
            NodeType::FirstChoice(node) => node.inputs().clone(),
        }
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> Vec<Arc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.outputs().clone(),
            NodeType::FirstChoice(node) => vec![node.outputs().clone()],
        }
    }

    /// ノードの種類の取得
    pub fn kind(&self) -> &NodeType {
        &self.kind
    }

    /// ブロッキングノードかどうか
    pub fn is_blocking(&self) -> bool {
        match &self.kind {
            NodeType::User(node) => node.is_blocking(),
            NodeType::FirstChoice(_) => false,
        }
    }

    /// ノードの名前の取得
    pub fn name(&self) -> &'static str {
        self.name
    }
}
impl std::cmp::PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl std::cmp::Eq for Node {}
impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// ノードの種類
#[derive(Debug)]
pub enum NodeType {
    /// ユーザー定義ノード
    User(UserNode),
    /// 最速ノード
    FirstChoice(FirstChoiceNode),
}

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
    func: Box<AsyncFn>,
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
}
impl std::fmt::Debug for UserNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserNode")
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .finish()
    }
}

/// 最速ノード
///
/// 複数の同じ型のエッジを受け取り、最も早く到着したデータを出力する。
#[derive(Debug)]
pub struct FirstChoiceNode {
    inputs: Vec<Arc<Edge>>,
    outputs: Option<Arc<Edge>>,
}
impl FirstChoiceNode {
    /// ノードの生成
    pub fn new(inputs: Vec<Arc<Edge>>) -> Result<Self, NodeError> {
        let ty = inputs[0].ty;
        if inputs.iter().any(|edge| edge.ty != ty) {
            return Err(NodeError::EdgeTypeMismatch);
        }
        Ok(FirstChoiceNode {
            inputs,
            outputs: None,
        })
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Result<Arc<Edge>, NodeError> {
        if self.outputs.is_some() {
            return Err(NodeError::OutputEdgeExists);
        }
        if !self.inputs[0].check_type::<T>() {
            return Err(NodeError::EdgeTypeMismatch);
        }
        let edge = Arc::new(Edge::new::<T>());
        self.outputs = Some(edge.clone());
        Ok(edge)
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Arc<Edge> {
        self.outputs.as_ref().unwrap()
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::FirstChoice(self), name)
    }
}

/// エッジ
///
/// ノード間を繋ぐデータの流れを表す。
/// エッジはデータの型を持ち、その型が異なるコンテナは通ることができない。
/// エッジは一意な ID を持ち、他のエッジと区別するために使われる。
///
/// ノードはエッジへの参照を持つが、エッジはノードへの参照を持たない。
/// エッジからノードへのマッピングは、ワークフローが行う。
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    ty: TypeId,
    id: Uuid,
}
impl Edge {
    /// エッジの生成
    pub fn new<T: 'static + Send + Sync>() -> Self {
        Edge {
            ty: TypeId::of::<T>(),
            id: Uuid::new_v4(),
        }
    }

    /// 型のチェック
    pub fn check_type<T: 'static + Send + Sync>(&self) -> bool {
        self.ty == TypeId::of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_first_choice_node_new() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        assert_eq!(node.inputs.len(), 2);
    }

    #[test]
    fn test_first_choice_node_new_error() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<f32>());
        let node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]);
        assert_eq!(node.err().unwrap(), NodeError::EdgeTypeMismatch);
    }

    #[test]
    fn test_first_choice_node_add_output() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let edge = node.add_output::<i32>().unwrap();
        assert_eq!(node.outputs.as_ref().unwrap(), &edge);
    }

    #[test]
    fn test_first_choice_node_add_output_error_exist() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let _ = node.add_output::<i32>().unwrap();
        let edge = node.add_output::<i32>();
        assert_eq!(edge.err().unwrap(), NodeError::OutputEdgeExists);
    }

    #[test]
    fn test_first_choice_node_add_output_error_type() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let edge = node.add_output::<f32>();
        assert_eq!(edge.err().unwrap(), NodeError::EdgeTypeMismatch);
    }
}
