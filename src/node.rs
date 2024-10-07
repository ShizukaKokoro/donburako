//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

mod branch;
pub mod edge;
mod func;
mod rec;

pub use self::branch::{FirstChoiceNode, IfNode};
pub use self::func::UserNode;
pub use self::rec::RecursiveNode;

use self::edge::Edge;
use crate::operator::{ExecutorId, Operator};
use crate::workflow::WorkflowId;
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
pub(crate) struct NodeId(Uuid);
impl NodeId {
    /// ノードIDの生成
    fn new() -> Self {
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
    fn new(kind: NodeType, name: &'static str) -> Self {
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
    pub(crate) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        match &self.kind {
            NodeType::User(node) => node.run(op, exec_id).await,
            NodeType::If(node) => node.run(op, exec_id).await,
            NodeType::FirstChoice(node) => node.run(op, exec_id).await,
            NodeType::Recursive(node) => node.run(op, exec_id).await,
        }
    }

    /// 入力エッジの取得
    pub(crate) fn inputs(&self) -> Vec<Arc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.inputs().clone(),
            NodeType::If(node) => vec![node.input().clone()],
            NodeType::FirstChoice(node) => node.inputs().clone(),
            NodeType::Recursive(node) => node.inputs().clone(),
        }
    }

    /// 出力エッジの取得
    pub(crate) fn outputs(&self) -> Vec<Arc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.outputs().clone(),
            NodeType::If(node) => vec![node.true_output().clone(), node.false_output().clone()],
            NodeType::FirstChoice(node) => vec![node.output().clone()],
            NodeType::Recursive(node) => node.outputs().clone(),
        }
    }

    /// ノードの種類の取得
    pub(crate) fn kind(&self) -> &NodeType {
        &self.kind
    }

    /// ブロッキングノードかどうか
    pub(crate) fn is_blocking(&self) -> bool {
        match &self.kind {
            NodeType::User(node) => node.is_blocking(),
            NodeType::If(_) => false,
            NodeType::FirstChoice(_) => false,
            NodeType::Recursive(_) => false,
        }
    }

    /// ノードの名前の取得
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    /// エッジの数が正しいかどうか
    pub(crate) fn is_edge_count_valid(&self, op: &Operator, wf_id: WorkflowId) -> bool {
        match &self.kind {
            NodeType::User(_) => true,
            NodeType::If(_) => true,
            NodeType::FirstChoice(_) => true,
            NodeType::Recursive(node) => node.is_edge_count_valid(op, wf_id),
        }
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
pub(crate) enum NodeType {
    /// ユーザー定義ノード
    User(UserNode),
    /// 分岐ノード
    If(IfNode),
    /// 最速ノード
    FirstChoice(FirstChoiceNode),
    /// 再帰ノード
    Recursive(RecursiveNode),
}
