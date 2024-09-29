//! ノードモジュール
//!
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
impl NodeBuilder {
    /// 新しいノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `func` - ノードの処理を行う関数(非同期)
    pub fn new_user(func: Box<AsyncFn>, is_blocking: bool) -> Self {
        Self::User(UserNodeBuilder::new(func, is_blocking))
    }

    /// 新しい任意の入力数を受け取るノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `count` - 必要な入力の数(=出力の数)
    pub fn new_any_input(count: usize) -> Self {
        Self::AnyInput(AnyInputNodeBuilder::new(count))
    }

    /// 新しい条件分岐ノードビルダーを生成する
    pub fn new_if() -> Self {
        Self::If(IfNodeBuilder::new())
    }

    /// 入力エッジの追加
    pub fn add_input(self, edge: Arc<Edge>) -> Self {
        match self {
            Self::User(builder) => Self::User(builder.add_input(edge)),
            Self::AnyInput(builder) => Self::AnyInput(builder.add_input(edge)),
            Self::If(_) => panic!("Cannot add input to IfNode"),
            #[cfg(test)]
            Self::Dummy(builder) => Self::Dummy(builder.add_input(edge)),
        }
    }

    /// 出力エッジの追加
    pub fn add_output(self, edge: Arc<Edge>) -> Self {
        match self {
            Self::User(builder) => Self::User(builder.add_output(edge)),
            Self::AnyInput(builder) => Self::AnyInput(builder.add_output(edge)),
            Self::If(_) => panic!("Cannot add output to IfNode"),
            #[cfg(test)]
            Self::Dummy(builder) => Self::Dummy(builder.add_output(edge)),
        }
    }

    /// 条件を表すエッジの追加
    pub fn add_condition(self, edge: Arc<Edge>) -> Self {
        match self {
            Self::If(builder) => Self::If(builder.add_condition(edge)),
            _ => panic!("Cannot add condition to non-IfNode"),
        }
    }

    /// 条件が真の場合に遷移するエッジの追加
    pub fn add_true_edge(self, edge: Arc<Edge>) -> Self {
        match self {
            Self::If(builder) => Self::If(builder.add_true_edge(edge)),
            _ => panic!("Cannot add true edge to non-IfNode"),
        }
    }

    /// 条件が偽の場合に遷移するエッジの追加
    pub fn add_false_edge(self, edge: Arc<Edge>) -> Self {
        match self {
            Self::If(builder) => Self::If(builder.add_false_edge(edge)),
            _ => panic!("Cannot add false edge to non-IfNode"),
        }
    }
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
    fn new(func: Box<AsyncFn>, is_blocking: bool) -> Self {
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
    pub(crate) fn new() -> Self {
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
    }
}

/// ノード
///
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
