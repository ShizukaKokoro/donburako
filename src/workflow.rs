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
use uuid::Uuid;

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
    pub fn add_edge<T: 'static + Send + Sync>(mut self, from: usize, to: usize) -> Self {
        self.graph.as_mut().unwrap().add_edge(from, to).unwrap();
        let edge = Arc::new(Edge::new::<T>());
        self.nodes[from].add_output(edge.clone());
        self.nodes[to].add_input(edge.clone());
        self
    }

    fn build(self) -> Workflow {
        if let Some(graph) = self.graph {
            graph.check_start_end();
            Workflow {
                nodes: self.nodes.into_iter().map(|node| node.build()).collect(),
                graph,
            }
        } else {
            panic!("Graph is not built");
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct WorkflowID(Uuid);
impl WorkflowID {
    pub fn new() -> Self {
        WorkflowID(Uuid::new_v4())
    }
}

#[derive(Debug)]
pub(crate) struct Workflow {
    nodes: Vec<Node>,
    graph: Graph,
}
impl Workflow {
    async fn start(&self, registry: &Arc<Mutex<Registry>>) {
        let mut rg = registry.lock().await;
        let index = self.graph.get_start();
        let node = self.nodes.get(index).unwrap();
        if rg.check(node) {
            rg.enqueue(index);
        }
    }

    async fn get_next(&self, registry: &Arc<Mutex<Registry>>) -> Option<(usize, &Node)> {
        if let Some(index) = registry.lock().await.dequeue() {
            return Some((index, self.nodes.get(index).unwrap()));
        }
        None
    }

    async fn done(&self, task_index: usize, registry: &Arc<Mutex<Registry>>) -> bool {
        let children = self.graph.children(task_index);
        if children.is_empty() {
            return true;
        }
        let mut rg = registry.lock().await;
        for next_index in self.graph.children(task_index) {
            let nt = self.nodes.get(*next_index).unwrap();
            if rg.check(nt) {
                rg.enqueue(*next_index);
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_workflow() {
        let node1 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})), false);
        let node2 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})), false);
        let node3 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})), false);
        let builder = WorkflowBuilder::default()
            .add_node(node1)
            .add_node(node2)
            .add_node(node3)
            .finish_nodes()
            .add_edge::<i32>(0, 1)
            .add_edge::<i32>(1, 2);
        let workflow = builder.build();
        assert_eq!(workflow.nodes.len(), 3);
    }

    #[tokio::test]
    async fn test_big_workflow() {
        let node0 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得

                    // タスクの処理
                    // どこからかデータを取得する

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "0 to 1").unwrap();
                    rg.store(&self_.outputs()[1], "0 to 2").unwrap();
                    rg.store(&self_.outputs()[2], "0 to 3").unwrap();
                })
            }),
            false,
        );
        let node1 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "0 to 1");
                    drop(rg);

                    // タスクの処理

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "1 to 3").unwrap();
                })
            }),
            false,
        );
        let node2 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "0 to 2");
                    drop(rg);

                    // タスクの処理

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "2 to 5").unwrap();
                })
            }),
            false,
        );
        let node3 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "0 to 3");
                    let b: &str = rg.take(&self_.inputs()[1]).unwrap();
                    assert_eq!(b, "1 to 3");
                    drop(rg);

                    // タスクの処理

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "3 to 4").unwrap();
                    rg.store(&self_.outputs()[1], "3 to 5").unwrap();
                })
            }),
            false,
        );
        let node4 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "3 to 4");
                    drop(rg);

                    // タスクの処理

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "4 to 5").unwrap();
                })
            }),
            false,
        );
        let node5 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "2 to 5");
                    let b: &str = rg.take(&self_.inputs()[1]).unwrap();
                    assert_eq!(b, "3 to 5");
                    let c: &str = rg.take(&self_.inputs()[2]).unwrap();
                    assert_eq!(c, "4 to 5");
                    drop(rg);

                    // タスクの処理

                    // 結果の格納
                    let mut rg = registry.lock().await;
                    rg.store(&self_.outputs()[0], "5 to 6").unwrap();
                })
            }),
            false,
        );
        let node6 = NodeBuilder::new(
            Box::new(|self_, registry| {
                Box::pin(async {
                    // 引数の取得
                    let mut rg = registry.lock().await;
                    let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                    assert_eq!(a, "5 to 6");
                    drop(rg);

                    // タスクの処理
                    // どこかにデータを送信する

                    // 結果の格納
                })
            }),
            false,
        );

        let builder = WorkflowBuilder::default()
            .add_node(node0)
            .add_node(node1)
            .add_node(node2)
            .add_node(node3)
            .add_node(node4)
            .add_node(node5)
            .add_node(node6)
            .finish_nodes()
            .add_edge::<&str>(0, 1)
            .add_edge::<&str>(0, 2)
            .add_edge::<&str>(0, 3)
            .add_edge::<&str>(1, 3)
            .add_edge::<&str>(2, 5)
            .add_edge::<&str>(3, 4)
            .add_edge::<&str>(3, 5)
            .add_edge::<&str>(4, 5)
            .add_edge::<&str>(5, 6);

        let rg = Arc::new(Mutex::new(Registry::new()));
        let wf = builder.build();
        wf.start(&rg).await;
        let (t0, task0) = wf.get_next(&rg).await.unwrap();
        let f0 = task0.run(&rg);
        f0.await;
        wf.done(t0, &rg).await;
        let (t1, task1) = wf.get_next(&rg).await.unwrap();
        let f1 = task1.run(&rg);
        let (t2, task2) = wf.get_next(&rg).await.unwrap();
        let f2 = task2.run(&rg);
        f1.await;
        wf.done(t1, &rg).await;
        let (t3, task3) = wf.get_next(&rg).await.unwrap();
        let f3 = task3.run(&rg);
        f3.await;
        wf.done(t3, &rg).await;
        let (t4, task4) = wf.get_next(&rg).await.unwrap();
        let f4 = task4.run(&rg);
        f4.await;
        wf.done(t4, &rg).await;
        f2.await;
        wf.done(t2, &rg).await;
        let (t5, task5) = wf.get_next(&rg).await.unwrap();
        let f5 = task5.run(&rg);
        f5.await;
        wf.done(t5, &rg).await;
        let (t6, task6) = wf.get_next(&rg).await.unwrap();
        let f6 = task6.run(&rg);
        f6.await;
        wf.done(t6, &rg).await;
    }
}
