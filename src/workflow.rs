//! ワークフローモジュール
//!
//! ワークフローは、複数のステップからなる処理の流れを表現するための構造体。
//! これは必ずイミュータブルな構造体であり、ビルダーを用いて構築する。

use crate::edge::Edge;
use crate::graph::Graph;
use crate::node::{Node, NodeBuilder};
use crate::registry::Registry;
use std::sync::Arc;
use tokio::sync::Mutex;

/// ワークフロービルダー
///
/// ワークフローを構築するためのビルダー。
#[derive(Default)]
pub struct WorkflowBuilder {
    nodes: Vec<NodeBuilder>,
    graph: Option<Graph>,
}
impl WorkflowBuilder {
    /// ワークフローにノードを追加する
    ///
    /// ノードは追加された順番でインデックスが割り振られる。
    /// エッジ構築の際には、このインデックスを使用する。
    ///
    /// # Arguments
    ///
    /// * `node` - ノードビルダー
    ///
    /// # Panics
    ///
    /// ノードの追加が終了した後にノードを追加しようとした場合、パニックする。
    pub fn add_node(self, node: NodeBuilder) -> Self {
        if self.graph.is_some() {
            panic!("Cannot add node after building graph");
        }
        let mut nodes = self.nodes;
        nodes.push(node);
        Self { nodes, ..self }
    }

    /// ノードの追加を終了する
    ///
    /// ノードの追加を終了し、グラフの構築を開始する。
    pub fn finish_nodes(self) -> Self {
        Self {
            graph: Some(Graph::new(self.nodes.len())),
            ..self
        }
    }

    /// エッジを追加する
    ///
    /// 入力エッジの場合は from に None を指定し、出力エッジの場合は to に None を指定する。
    /// 両方とも None の場合は追加されず drop される。
    ///
    /// # Arguments
    ///
    /// * `from` - 始点のノードインデックス(None の場合は入力エッジ)
    /// * `to` - 終点のノードインデックス(None の場合は出力エッジ)
    ///
    /// # Panics
    ///
    /// ノードの追加が終了していない場合、パニックする。
    pub fn add_edge<T: 'static + Send + Sync>(
        mut self,
        from: Option<usize>,
        to: Option<usize>,
    ) -> Self {
        if let (Some(from), Some(to)) = (from, to) {
            self.graph.as_mut().unwrap().add_edge(from, to).unwrap();
        }
        let edge = Arc::new(Edge::new::<T>());
        if let Some(from) = from {
            self.nodes[from].add_output(edge.clone());
        }
        if let Some(to) = to {
            self.nodes[to].add_input(edge.clone());
        }
        self
    }

    fn build(self) -> Workflow {
        Workflow {
            nodes: self.nodes.into_iter().map(|node| node.build()).collect(),
            graph: self.graph.unwrap(),
        }
    }
}

#[derive(Debug)]
struct Workflow {
    nodes: Vec<Node>,
    graph: Graph,
}
impl Workflow {
    async fn get_next(&self, registry: &Arc<Mutex<Registry>>) -> Option<(usize, &Node)> {
        if let Some(index) = registry.lock().await.dequeue() {
            return Some((index, self.nodes.get(index).unwrap()));
        }
        None
    }

    async fn done(&self, task_index: usize, registry: Arc<Mutex<Registry>>) {
        for next_index in self.graph.children(task_index) {
            let nt = self.nodes.get(*next_index).unwrap();
            let mut rg = registry.lock().await;
            if rg.check(nt) {
                rg.enqueue(*next_index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_workflow() {
        let node1 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        let node2 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        let node3 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        let builder = WorkflowBuilder::default()
            .add_node(node1)
            .add_node(node2)
            .add_node(node3)
            .finish_nodes()
            .add_edge::<i32>(Some(0), Some(1))
            .add_edge::<i32>(Some(1), Some(2));
        let workflow = builder.build();
        assert_eq!(workflow.nodes.len(), 3);
    }

    #[tokio::test]
    async fn test_big_workflow() {
        let node1 = NodeBuilder::new(Box::new(|self_, registry| {
            Box::pin(async {
                // 引数の取得
                let a: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[0].clone())
                    .unwrap();
                assert_eq!(a, "to 1");

                // タスクの処理

                // 結果の格納
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[0].clone(), "1 to 3")
                    .unwrap();
            })
        }));
        let node2 = NodeBuilder::new(Box::new(|self_, registry| {
            Box::pin(async {
                // 引数の取得
                let a: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[0].clone())
                    .unwrap();
                assert_eq!(a, "to 2");

                // タスクの処理

                // 結果の格納
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[0].clone(), "2 to 5")
                    .unwrap();
            })
        }));
        let node3 = NodeBuilder::new(Box::new(|self_, registry| {
            Box::pin(async {
                // 引数の取得
                let a: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[0].clone())
                    .unwrap();
                assert_eq!(a, "to 3");
                let b: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[1].clone())
                    .unwrap();
                assert_eq!(b, "1 to 3");

                // タスクの処理

                // 結果の格納
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[0].clone(), "3 to 4")
                    .unwrap();
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[1].clone(), "3 to 5")
                    .unwrap();
            })
        }));
        let node4 = NodeBuilder::new(Box::new(|self_, registry| {
            Box::pin(async {
                // 引数の取得
                let a: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[0].clone())
                    .unwrap();
                assert_eq!(a, "3 to 4");

                // タスクの処理

                // 結果の格納
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[0].clone(), "4 to 5")
                    .unwrap();
            })
        }));
        let node5 = NodeBuilder::new(Box::new(|self_, registry| {
            Box::pin(async {
                // 引数の取得
                let a: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[0].clone())
                    .unwrap();
                assert_eq!(a, "2 to 5");
                let b: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[1].clone())
                    .unwrap();
                assert_eq!(b, "3 to 5");
                let c: &str = registry
                    .lock()
                    .await
                    .take(self_.inputs()[2].clone())
                    .unwrap();
                assert_eq!(c, "4 to 5");

                // タスクの処理

                // 結果の格納
                registry
                    .lock()
                    .await
                    .store(self_.outputs()[0].clone(), "5 to")
                    .unwrap();
            })
        }));

        let builder = WorkflowBuilder::default()
            .add_node(node1)
            .add_node(node2)
            .add_node(node3)
            .add_node(node4)
            .add_node(node5)
            .finish_nodes()
            .add_edge::<&str>(None, Some(0))
            .add_edge::<&str>(None, Some(1))
            .add_edge::<&str>(None, Some(2))
            .add_edge::<&str>(Some(0), Some(2))
            .add_edge::<&str>(Some(1), Some(4))
            .add_edge::<&str>(Some(2), Some(3))
            .add_edge::<&str>(Some(2), Some(4))
            .add_edge::<&str>(Some(3), Some(4))
            .add_edge::<&str>(Some(4), None);

        let rg = Arc::new(Mutex::new(Registry::default()));
        let wf = builder.build();
    }
}
