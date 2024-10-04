//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

pub mod branch;
pub mod edge;
pub mod func;

use self::branch::FirstChoiceNode;
use self::edge::Edge;
use self::func::UserNode;
use crate::operator::{ExecutorId, Operator};
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
            NodeType::If(node) => {
                let mut con = op
                    .get_container(node.input().clone(), exec_id)
                    .await
                    .unwrap();
                if con.take::<bool>().unwrap() {
                    con.store(());
                    op.add_container(node.true_output().clone(), exec_id, con)
                        .await
                        .unwrap();
                } else {
                    con.store(());
                    op.add_container(node.false_output().clone(), exec_id, con)
                        .await
                        .unwrap();
                }
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
            NodeType::If(node) => vec![node.input().clone()],
            NodeType::FirstChoice(node) => node.inputs().clone(),
        }
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> Vec<Arc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.outputs().clone(),
            NodeType::If(node) => vec![node.true_output().clone(), node.false_output().clone()],
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
            NodeType::If(_) => false,
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
    /// 分岐ノード
    If(branch::IfNode),
    /// 最速ノード
    FirstChoice(FirstChoiceNode),
}
