//! ノードモジュール
//!
//! タスクの実行を行うノードを定義するモジュール

use crate::edge::Edge;
use crate::registry::Registry;
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

    pub(crate) fn add_input(&mut self, edge: Arc<Edge>) {
        match self {
            Self::UserNode(builder) => builder.add_input(edge),
        }
    }

    pub(crate) fn add_output(&mut self, edge: Arc<Edge>) {
        match self {
            Self::UserNode(builder) => builder.add_output(edge),
        }
    }

    pub(crate) fn build(self) -> Node {
        match self {
            Self::UserNode(builder) => Node::UserNode(builder.build()),
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

    fn build(self) -> UserNode {
        UserNode {
            inputs: self.inputs,
            outputs: self.outputs,
            func: self.func,
            is_blocking: self.is_blocking,
        }
    }
}

/// ノード
///
/// ワークフローのステップとして機能するノード
#[derive(Debug)]
pub enum Node {
    /// ユーザーノード
    UserNode(UserNode),
}
impl Node {
    /// ブロッキングしているかどうかを取得する
    pub(crate) fn is_blocking(&self) -> bool {
        match self {
            Self::UserNode(node) => node.is_blocking(),
        }
    }

    /// ノードを実行する
    pub(crate) async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        match self {
            Self::UserNode(node) => node.run(registry).await,
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
