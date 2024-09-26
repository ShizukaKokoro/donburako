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

type AsyncFn = dyn for<'a> Fn(&'a Node, &'a Arc<Mutex<Registry>>) -> BoxedFuture<'a> + Send + Sync;

pub struct NodeBuilder {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    func: Box<AsyncFn>,
}
impl NodeBuilder {
    pub fn new(func: Box<AsyncFn>) -> Self {
        Self {
            inputs: Vec::new(),
            outputs: Vec::new(),
            func,
        }
    }

    pub fn add_input(&mut self, edge: Arc<Edge>) -> &mut Self {
        self.inputs.push(edge);
        self
    }

    pub fn add_output(&mut self, edge: Arc<Edge>) -> &mut Self {
        self.outputs.push(edge);
        self
    }

    pub fn build(self) -> Node {
        Node {
            inputs: self.inputs,
            outputs: self.outputs,
            func: self.func,
        }
    }
}

pub struct Node {
    inputs: Vec<Arc<Edge>>,
    outputs: Vec<Arc<Edge>>,
    func: Box<AsyncFn>,
}
impl Node {
    pub async fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    pub async fn outputs(&self) -> &Vec<Arc<Edge>> {
        &self.outputs
    }

    pub async fn run(&self, registry: &Arc<Mutex<Registry>>) {
        (self.func)(self, registry).await;
    }
}
impl std::fmt::Debug for Node {
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

        pub fn build(self) -> Node {
            Node {
                inputs: self.inputs,
                outputs: self.outputs,
                func: Box::new(|self_: &Node, registry: &Arc<Mutex<Registry>>| {
                    Box::pin(async move {
                        // 引数の取得
                        let arg0: u16 =
                            registry.lock().await.take(self_.inputs[0].clone()).unwrap();
                        let arg1: i32 =
                            registry.lock().await.take(self_.inputs[1].clone()).unwrap();

                        // 処理
                        let result0 = format!("{} + {}", arg0, arg1);
                        let result1 = arg0 as i64 + arg1 as i64;

                        // 結果の登録
                        registry
                            .lock()
                            .await
                            .store(self_.outputs[0].clone(), result0)
                            .unwrap();
                        registry
                            .lock()
                            .await
                            .store(self_.outputs[1].clone(), result1)
                            .unwrap();
                    })
                }),
            }
        }
    }
}
