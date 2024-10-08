//! ノードモジュール
//!
<<<<<<< HEAD
//! タスクの実行を行うノードを定義するモジュール

use crate::edge::Edge;
use crate::registry::{Registry, RegistryError};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

type BoxedFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

type AsyncFn =
    dyn for<'a> Fn(&'a UserNode, &'a Arc<Mutex<Registry>>) -> BoxedFuture<'a> + Send + Sync;

/// ノードビルダー
///
/// ノードを構築するためのビルダー
pub enum NodeBuilder {
    /// ユーザーノード
    User(UserNodeBuilder),

    /// 任意の入力数を受け取るノード
    AnyInput(AnyInputNodeBuilder),

    /// 条件分岐ノード
    If(IfNodeBuilder),

    #[cfg(test)]
    Dummy(dummy::DummyNodeBuilder),
}

/// ユーザーノードビルダー
///
/// ユーザーが任意の処理を行うノードを構築するためのビルダー
pub struct UserNodeBuilder {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    func: Box<AsyncFn>,
    is_blocking: bool,
}
impl UserNodeBuilder {
    /// 新しいノードビルダーを生成する
    pub fn new(func: Box<AsyncFn>, is_blocking: bool) -> Self {
        Self {
            inputs: Vec::new(),
            outputs: Vec::new(),
            func,
            is_blocking,
        }
    }

    /// 入力エッジを追加する
    pub fn add_input(self, edge: Arc<Edge>) -> Self {
        let mut inputs = self.inputs;
        inputs.push(edge);
        Self { inputs, ..self }
    }

    /// 出力エッジを追加する
    pub fn add_output(self, edge: Arc<Edge>) -> Self {
        let mut outputs = self.outputs;
        outputs.push(edge);
        Self { outputs, ..self }
    }

    pub(crate) fn build(self) -> UserNode {
        UserNode {
            inputs: self.inputs,
            outputs: self.outputs,
            func: self.func,
            is_blocking: self.is_blocking,
        }
    }
}

/// 任意の入力数を受け取るノードビルダー
///
/// 設定した入力のうち、 count の入力がある場合にのみ実行される
pub struct AnyInputNodeBuilder {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    count: usize,
}
impl AnyInputNodeBuilder {
    /// 新しいノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `count` - 必要な入力の数(=出力の数)
    pub fn new(count: usize) -> Self {
        Self {
            inputs: Vec::new(),
            outputs: Vec::new(),
            count,
        }
    }

    /// 入力エッジを追加する
    pub fn add_input(self, edge: Arc<Edge>) -> Self {
        let mut inputs = self.inputs;
        inputs.push(edge);
        Self { inputs, ..self }
    }

    /// 出力エッジを追加する
    pub fn add_output(self, edge: Arc<Edge>) -> Self {
        let mut outputs = self.outputs;
        outputs.push(edge);
        Self { outputs, ..self }
    }

    pub(crate) fn build(self) -> AnyInputNode {
        AnyInputNode {
            inputs: self.inputs,
            outputs: self.outputs,
            count: self.count,
        }
    }
}

/// 条件分岐ノードビルダー
///
/// 条件分岐を行うノードを構築するためのビルダー
/// count が 1 の AnyInputNode と併用することで、合流できる
#[derive(Default)]
pub struct IfNodeBuilder {
    condition: Option<Arc<Edge>>,
    true_edge: Option<Arc<Edge>>,
    false_edge: Option<Arc<Edge>>,
}
impl IfNodeBuilder {
    /// 新しいノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `condition` - 条件を表すエッジ
    /// * `true_edge` - 条件が真の場合に遷移するエッジ
    /// * `false_edge` - 条件が偽の場合に遷移するエッジ
    pub fn new() -> Self {
        Self {
            condition: None,
            true_edge: None,
            false_edge: None,
        }
    }

    /// 条件を表すエッジを追加する
    pub fn add_condition(self, edge: Arc<Edge>) -> Self {
        if edge.check_type::<bool>() {
            panic!("Type mismatch: condition edge must be bool");
        }
        Self {
            condition: Some(edge),
            ..self
        }
    }

    /// 条件が真の場合に遷移するエッジを追加する
    pub fn add_true_edge(self, edge: Arc<Edge>) -> Self {
        if edge.check_type::<()>() {
            panic!("Type mismatch: true edge must be unit");
        }
        Self {
            true_edge: Some(edge),
            ..self
        }
    }

    /// 条件が偽の場合に遷移するエッジを追加する
    pub fn add_false_edge(self, edge: Arc<Edge>) -> Self {
        if edge.check_type::<()>() {
            panic!("Type mismatch: false edge must be unit");
        }
        Self {
            false_edge: Some(edge),
            ..self
        }
    }

    pub(crate) fn build(self) -> IfNode {
        IfNode {
            condition: self.condition.unwrap(),
            true_edge: self.true_edge.unwrap(),
            false_edge: self.false_edge.unwrap(),
        }
=======
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

mod branch;
pub mod edge;
mod func;
mod iter;
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
>>>>>>> refactor-iter_by_sub_workflow
    }
}

/// ノード
///
<<<<<<< HEAD
/// ワークフローのステップとして機能するノード
#[derive(Debug)]
pub(crate) enum Node {
    /// ユーザーノード
    User(UserNode),

    /// 任意の入力数を受け取るノード
    AnyInput(AnyInputNode),

    /// 条件分岐ノード
    If(IfNode),
}
impl Node {
    /// ブロッキングしているかどうかを取得する
    pub(crate) fn is_blocking(&self) -> bool {
        match self {
            Self::User(node) => node.is_blocking(),
            Self::AnyInput(_) => false,
            Self::If(_) => false,
        }
    }

    /// ノードを実行する
    pub(crate) async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        match self {
            Self::User(node) => node.run(registry).await,
            Self::AnyInput(node) => node.run(registry).await,
            Self::If(node) => node.run(registry).await,
        }
    }
}

/// ユーザーノード
///
/// ユーザーが任意の処理を行うノード
pub struct UserNode {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    func: Box<AsyncFn>,
    is_blocking: bool,
}
impl UserNode {
    /// ノードの入力エッジを取得する
    pub fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// ノードの出力エッジを取得する
    pub fn outputs(&self) -> &Vec<Arc<Edge>> {
        &self.outputs
    }

    /// ブロッキングしているかどうかを取得する
    fn is_blocking(&self) -> bool {
        self.is_blocking
    }

    /// ノードを実行する
    async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        (self.func)(self, registry).await;
    }
}
impl std::fmt::Debug for UserNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("inputs", &self.inputs)
            .field("output", &self.outputs)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct AnyInputNode {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    count: usize,
}
impl AnyInputNode {
    pub(crate) fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        let mut inputs = Vec::with_capacity(self.count);
        for input in &self.inputs {
            let data = registry
                .lock()
                .await
                .take::<Box<dyn Any + 'static + Send + Sync>>(input);
            if let Ok(data) = data {
                inputs.push(data);
            } else {
                match data.err().unwrap() {
                    RegistryError::DataNotFound => continue,
                    RegistryError::TypeMismatch => panic!("Cannot take data from edge"),
                }
            }
        }

        for (output, input) in self.outputs.iter().zip(inputs) {
            registry.lock().await.store(output, input).unwrap();
        }
    }
}

#[derive(Debug)]
pub(crate) struct IfNode {
    condition: Arc<Edge>,
    true_edge: Arc<Edge>,
    false_edge: Arc<Edge>,
}
impl IfNode {
    pub(crate) fn inputs(&self) -> Vec<Arc<Edge>> {
        vec![
            self.condition.clone(),
            self.true_edge.clone(),
            self.false_edge.clone(),
        ]
    }

    pub(crate) async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        let condition = registry.lock().await.take::<bool>(&self.condition).unwrap();
        let node = if condition {
            &self.true_edge
        } else {
            &self.false_edge
        };

        registry.lock().await.store(node, ()).unwrap();
    }
}

#[cfg(test)]
pub mod dummy {
    use super::*;

    #[derive(Default)]
    pub struct DummyNodeBuilder {
        inputs: Vec<Arc<Edge>>,
        outputs: Vec<Arc<Edge>>,
    }
    impl DummyNodeBuilder {
        pub fn new() -> Self {
            Self {
                inputs: Vec::new(),
                outputs: Vec::new(),
            }
        }

        pub fn add_input(self, edge: Arc<Edge>) -> Self {
            let mut inputs = self.inputs;
            inputs.push(edge);
            Self { inputs, ..self }
        }

        pub fn add_output(self, edge: Arc<Edge>) -> Self {
            let mut outputs = self.outputs;
            outputs.push(edge);
            Self { outputs, ..self }
        }

        pub fn build(self) -> UserNode {
            UserNode {
                inputs: self.inputs,
                outputs: self.outputs,
                func: Box::new(|self_: &UserNode, registry: &Arc<Mutex<Registry>>| {
                    Box::pin(async move {
                        // 引数の取得
                        let arg0: u16 = registry.lock().await.take(&self_.inputs[0]).unwrap();
                        let arg1: i32 = registry.lock().await.take(&self_.inputs[1]).unwrap();

                        // 処理
                        let result0 = format!("{} + {}", arg0, arg1);
                        let result1 = arg0 as i64 + arg1 as i64;

                        // 結果の登録
                        registry
                            .lock()
                            .await
                            .store(&self_.outputs[0], result0)
                            .unwrap();
                        registry
                            .lock()
                            .await
                            .store(&self_.outputs[1], result1)
                            .unwrap();
                    })
                }),
                is_blocking: false,
            }
        }
    }
}
=======
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
>>>>>>> refactor-iter_by_sub_workflow
