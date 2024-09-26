//! ノードモジュール
//!
//! タスクの実行を行うノードを定義するモジュール

use crate::edge::Edge;
use crate::registry::Registry;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait Node: 'static + std::fmt::Debug + Send + Sync {
    async fn inputs(&self) -> &Vec<Arc<Edge>>;
    async fn outputs(&self) -> &Vec<Arc<Edge>>;
    async fn run(&self, registry: Arc<Mutex<Registry>>);
}

#[cfg(test)]
pub mod dummy {
    use super::*;

    #[derive(Debug)]
    pub struct NodeDummy {
        inputs: Vec<Arc<Edge>>,
        outputs: Vec<Arc<Edge>>,
    }
    #[async_trait]
    impl Node for NodeDummy {
        async fn inputs(&self) -> &Vec<Arc<Edge>> {
            &self.inputs
        }

        async fn outputs(&self) -> &Vec<Arc<Edge>> {
            &self.outputs
        }

        async fn run(&self, registry: Arc<Mutex<Registry>>) {
            // 引数の取得
            let arg0: u16 = registry.lock().await.take(&self.inputs[0]).unwrap();
            let arg1: i32 = registry.lock().await.take(&self.inputs[1]).unwrap();

            // 処理
            let result0 = format!("{} + {}", arg0, arg1);
            let result1 = arg0 as i64 + arg1 as i64;

            // 結果の登録
            registry
                .lock()
                .await
                .store(&self.outputs[0], result0)
                .unwrap();
            registry
                .lock()
                .await
                .store(&self.outputs[1], result1)
                .unwrap();
        }
    }
}
