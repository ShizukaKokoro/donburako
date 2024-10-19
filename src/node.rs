//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

use crate::edge::Edge;
use crate::operator::{ExecutorId, Operator};
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
pub(crate) struct NodeId(Uuid);
impl NodeId {
    /// ノードIDの生成
    fn new() -> Self {
        NodeId(Uuid::new_v4())
    }
}

/// エッジの判断方法
#[derive(Debug)]
pub enum Choice {
    /// すべてのエッジ
    All,

    /// 任意のエッジ
    Any,
}

type BoxedFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

type AsyncFn = dyn for<'a> Fn(&'a Node, &'a Operator, ExecutorId) -> BoxedFuture<'a> + Send + Sync;

/// ノードビルダートレイト
pub trait NodeBuilder {
    /// ノードビルダーの生成
    fn new() -> Self;

    /// 出力エッジの取得
    fn outputs(&self) -> &Vec<Arc<Edge>>;

    /// ノードのビルド
    ///
    /// # Arguments
    ///
    /// * `inputs` - 入力エッジ
    /// * `manage_cnt` - 管理エッジを持つかどうか
    ///
    /// # Returns
    ///
    /// ノードの生成結果。
    /// 入力のエッジの型が一致しない場合はエラーを返す。
    fn build(self, inputs: Vec<Arc<Edge>>, manage_cnt: usize) -> Result<Arc<Node>, NodeError>;
}

/// ノード設定
pub struct NodeConfig {
    id: Option<Uuid>,
    func: Box<AsyncFn>,
    is_blocking: bool,
    choice: Choice,
    name: &'static str,
}
impl NodeConfig {
    /// ノード設定の生成
    ///
    /// # Arguments
    ///
    /// * `func` - ノードの処理
    /// * `is_blocking` - ブロッキングノードかどうか
    /// * `choice` - エッジの判断方法
    /// * `name` - ノードの名前
    pub fn new(func: Box<AsyncFn>, is_blocking: bool, choice: Choice, name: &'static str) -> Self {
        NodeConfig {
            id: None,
            func,
            is_blocking,
            choice,
            name,
        }
    }

    /// IDの登録
    pub fn id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }
}

/// ノード
///
/// NOTE: サイズが大きめ？
pub struct Node {
    id: NodeId,
    inputs: Vec<Arc<Edge>>,
    manage_cnt: usize,
    outputs: Vec<Arc<Edge>>,
    func: Box<AsyncFn>,
    is_blocking: bool,
    choice: Choice,
    name: &'static str,
}
impl Node {
    /// ノードの生成
    ///
    /// # Arguments
    ///
    /// * `config` - ノード設定
    /// * `inputs` - 入力エッジ
    /// * `manage_cnt` - 管理エッジを持つかどうか
    /// * `outputs` - 出力エッジ
    pub fn new(
        config: NodeConfig,
        inputs: Vec<Arc<Edge>>,
        manage_cnt: usize,
        outputs: Vec<Arc<Edge>>,
    ) -> Self {
        let id = if let Some(id) = config.id {
            NodeId(id)
        } else {
            NodeId::new()
        };
        Node {
            id,
            inputs,
            manage_cnt,
            outputs,
            func: config.func,
            is_blocking: config.is_blocking,
            choice: config.choice,
            name: config.name,
        }
    }

    /// テスト用のノードの生成
    #[cfg(test)]
    pub fn new_test(
        inputs: Vec<Arc<Edge>>,
        outputs: Vec<Arc<Edge>>,
        name: &'static str,
        choice: Choice,
    ) -> Self {
        let config = NodeConfig {
            id: None,
            func: Box::new(|_, _, _| Box::pin(async {})),
            is_blocking: false,
            choice,
            name,
        };
        Self::new(config, inputs, 0, outputs)
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// 管理エッジの数の取得
    pub fn manage_cnt(&self) -> usize {
        self.manage_cnt
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Vec<Arc<Edge>> {
        &self.outputs
    }

    /// ブロッキングノードかどうか
    pub(super) fn is_blocking(&self) -> bool {
        self.is_blocking
    }

    pub(super) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        (self.func)(self, op, exec_id).await;
    }
    /// エッジの判断方法の取得
    pub(crate) fn choice(&self) -> &Choice {
        &self.choice
    }

    /// ノードの名前の取得
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
}
impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("inputs", &self.inputs)
            .field("manage_cnt", &self.manage_cnt)
            .field("outputs", &self.outputs)
            .field("is_blocking", &self.is_blocking)
            .field("choice", &self.choice)
            .finish()
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
