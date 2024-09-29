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
    UserNode(UserNodeBuilder),

    /// 任意の入力数を受け取るノード
    AnyInputNode(AnyInputNodeBuilder),

    /// 条件分岐ノード
    IfNode(IfNodeBuilder),

    #[cfg(test)]
    DummyNode(dummy::DummyNodeBuilder),
}
impl NodeBuilder {
    /// 新しいノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `func` - ノードの処理を行う関数(非同期)
    pub fn new_user(func: Box<AsyncFn>, is_blocking: bool) -> Self {
        Self::UserNode(UserNodeBuilder::new(func, is_blocking))
    }

    /// 新しい任意の入力数を受け取るノードビルダーを生成する
    ///
    /// # Arguments
    ///
    /// * `count` - 必要な入力の数(=出力の数)
    pub fn new_any_input(count: usize) -> Self {
        Self::AnyInputNode(AnyInputNodeBuilder::new(count))
    }

    #[cfg(test)]
    pub(crate) fn new_dummy() -> Self {
        Self::DummyNode(dummy::DummyNodeBuilder::new())
    }

    pub(crate) fn add_input(&mut self, edge: Arc<Edge>) {
        match self {
            Self::UserNode(builder) => builder.add_input(edge),
            Self::AnyInputNode(builder) => builder.add_input(edge),
            Self::IfNode(builder) => todo!(),
            #[cfg(test)]
            Self::DummyNode(builder) => builder.add_input(edge),
        }
    }

    pub(crate) fn add_output(&mut self, edge: Arc<Edge>) {
        match self {
            Self::UserNode(builder) => builder.add_output(edge),
            Self::AnyInputNode(builder) => builder.add_output(edge),
            Self::IfNode(builder) => todo!(),
            #[cfg(test)]
            Self::DummyNode(builder) => builder.add_output(edge),
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

    fn add_input(&mut self, edge: Arc<Edge>) {
        self.inputs.push(edge);
    }

    fn add_output(&mut self, edge: Arc<Edge>) {
        self.outputs.push(edge);
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

    pub(crate) fn add_input(&mut self, edge: Arc<Edge>) {
        self.inputs.push(edge);
    }

    pub(crate) fn add_output(&mut self, edge: Arc<Edge>) {
        self.outputs.push(edge);
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
    UserNode(UserNode),

    /// 任意の入力数を受け取るノード
    AnyInputNode(AnyInputNode),

    /// 条件分岐ノード
    IfNode(IfNode),
}
impl Node {
    /// ブロッキングしているかどうかを取得する
    pub(crate) fn is_blocking(&self) -> bool {
        match self {
            Self::UserNode(node) => node.is_blocking(),
            Self::AnyInputNode(_) => false,
            Self::IfNode(_) => false,
        }
    }

    /// ノードを実行する
    pub(crate) async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        match self {
            Self::UserNode(node) => node.run(registry).await,
            Self::AnyInputNode(node) => node.run(registry).await,
            Self::IfNode(node) => node.run(registry).await,
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

        pub fn add_input(&mut self, edge: Arc<Edge>) {
            self.inputs.push(edge);
        }

        pub fn add_output(&mut self, edge: Arc<Edge>) {
            self.outputs.push(edge);
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
