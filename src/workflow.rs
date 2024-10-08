//! ワークフローモジュール
//!
<<<<<<< HEAD
//! ワークフローは、複数のステップからなる処理の流れを表現するための構造体。
//! これは必ずイミュータブルな構造体であり、ビルダーを用いて構築する。

use crate::graph::{Graph, GraphError};
use crate::node::{Node, NodeBuilder};
use crate::registry::Registry;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::MutexGuard;
use uuid::Uuid;

/// ワークフローエラー
#[derive(Error, Debug)]
pub enum WorkflowError {
    /// グラフエラー
    #[error("graph error: {0}")]
    GraphError(#[from] GraphError),

    /// ノード追加が完了していない
    #[error("Node addition is not complete")]
    NodeAdditionIsNotComplete,

    /// ノード追加が完了した
    #[error("Node addition is complete")]
    NodeAdditionIsComplete,
}

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
    pub fn add_node(self, node: NodeBuilder) -> Result<Self, WorkflowError> {
        if self.graph.is_some() {
            return Err(WorkflowError::NodeAdditionIsComplete);
        }
        let mut nodes = self.nodes;
=======
//! ワークフローはノードからなる有向グラフ。

use crate::node::edge::Edge;
use crate::node::Node;
use crate::operator::Operator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// ワークフローエラー
#[derive(Debug, Error, PartialEq)]
pub enum WorkflowError {
    /// ノードがすでに追加されている
    #[error("Node is already added")]
    NodeIsAlreadyAdded,

    /// エッジが無効
    #[error("The edge is not connected to any node")]
    InvalidEdge,
}

/// ワークフローID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WorkflowId(Uuid);

/// ワークフロービルダー
#[derive(Default)]
pub struct WorkflowBuilder {
    id: WorkflowId,
    nodes: Vec<Arc<Node>>,
}
impl WorkflowBuilder {
    /// ノードの追加
    pub fn add_node(self, node: Arc<Node>) -> Result<Self, WorkflowError> {
        let mut nodes = self.nodes;
        if nodes.contains(&node) {
            return Err(WorkflowError::NodeIsAlreadyAdded);
        }
>>>>>>> refactor-iter_by_sub_workflow
        nodes.push(node);
        Ok(Self { nodes, ..self })
    }

<<<<<<< HEAD
    pub(crate) fn build(self) -> Result<Workflow, WorkflowError> {
        if let Some(graph) = self.graph {
            graph.check_start_end().unwrap();
            let nodes = {
                let mut nodes = Vec::with_capacity(self.nodes.len());
                for node in self.nodes {
                    let n = match node {
                        NodeBuilder::User(node) => Node::User(node.build()),
                        NodeBuilder::AnyInput(node) => Node::AnyInput(node.build()),
                        NodeBuilder::If(node) => Node::If(node.build()),
                        #[cfg(test)]
                        NodeBuilder::Dummy(node) => Node::User(node.build()),
                    };
                    nodes.push(Arc::new(n));
                }
                nodes
            };
            Ok(Workflow {
                nodes: nodes.into_iter().collect(),
                graph,
            })
        } else {
            Err(WorkflowError::NodeAdditionIsNotComplete)
        }
    }
}

/// ワークフローID
///
/// 実行するワークフローを指定する時に利用する
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct WorkflowID(Uuid);
impl WorkflowID {
    pub(crate) fn new() -> Self {
        WorkflowID(Uuid::new_v4())
    }
}

#[derive(Debug)]
pub(crate) struct Workflow {
    nodes: Vec<Arc<Node>>,
    graph: Graph,
}
impl Workflow {
    pub(crate) fn start(&self, mut registry: MutexGuard<Registry>) -> Result<(), WorkflowError> {
        let index = self.graph.get_start()?;
        let node = self.nodes.get(index).unwrap();
        if registry.check(node) {
            registry.enqueue(index);
        }
        Ok(())
    }

    pub(crate) fn get_next(
        &self,
        mut registry: MutexGuard<Registry>,
    ) -> Option<(usize, Arc<Node>)> {
        if let Some(index) = registry.dequeue() {
            return Some((index, self.nodes.get(index).unwrap().clone()));
        }
        None
    }

    pub(crate) fn done(&self, task_index: usize, mut registry: MutexGuard<Registry>) {
        let children = self.graph.children(task_index);
        if children.is_empty() {
            registry.finished = true;
        }
        for next_index in self.graph.children(task_index) {
            let nt = self.nodes.get(*next_index).unwrap();
            if registry.check(nt) {
                registry.enqueue(*next_index);
            }
        }
=======
    /// ワークフローIDの取得
    pub fn id(&self) -> WorkflowId {
        self.id
    }

    /// ワークフローの生成
    pub(crate) fn build(self) -> (Workflow, WorkflowId) {
        let mut input_to_node = HashMap::new();
        let mut all_edges = HashSet::new();
        let mut input_edges = HashSet::new();
        let mut output_edges = HashSet::new();

        for node in self.nodes.iter() {
            for input in node.inputs() {
                let _ = input_to_node.insert(input.clone(), node.clone());
                let _ = all_edges.insert(input.clone());
                let _ = input_edges.insert(input.clone());
            }
            for output in node.outputs() {
                let _ = all_edges.insert(output.clone());
                let _ = output_edges.insert(output.clone());
            }
        }

        let start_edges = all_edges.difference(&output_edges).cloned().collect();
        let end_edges = all_edges.difference(&input_edges).cloned().collect();
        (
            Workflow {
                input_to_node,
                start_edges,
                end_edges,
            },
            self.id,
        )
    }
}

/// ワークフロー
#[derive(Debug)]
pub(crate) struct Workflow {
    /// Edge を入力に持つ Node へのマップ
    input_to_node: HashMap<Arc<Edge>, Arc<Node>>,
    /// 始点のエッジ
    start_edges: Vec<Arc<Edge>>,
    /// 終点のエッジ
    end_edges: Vec<Arc<Edge>>,
}
impl Workflow {
    /// エッジからノードを取得
    ///
    /// 指定されたエッジが入力になっているノードを取得する。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    pub(crate) fn get_node(&self, edge: &Arc<Edge>) -> Option<Arc<Node>> {
        self.input_to_node.get(edge).cloned()
    }

    /// 始点のエッジを取得
    pub(crate) fn start_edges(&self) -> &Vec<Arc<Edge>> {
        &self.start_edges
    }

    /// 終点のエッジを取得
    pub(crate) fn end_edges(&self) -> &Vec<Arc<Edge>> {
        &self.end_edges
    }

    /// エッジの数が正しいかどうか
    pub(crate) fn is_edge_count_valid(&self, op: &Operator, wf_id: WorkflowId) -> bool {
        for node in self.input_to_node.values() {
            if !node.is_edge_count_valid(op, wf_id) {
                return false;
            }
        }
        true
>>>>>>> refactor-iter_by_sub_workflow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
<<<<<<< HEAD
    use crate::edge::Edge;
    use crate::node::UserNodeBuilder;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_workflow() {
        let node1 = NodeBuilder::User(UserNodeBuilder::new(
            Box::new(|_, _| Box::pin(async {})),
            false,
        ));
        let node2 = NodeBuilder::User(UserNodeBuilder::new(
            Box::new(|_, _| Box::pin(async {})),
            false,
        ));
        let node3 = NodeBuilder::User(UserNodeBuilder::new(
            Box::new(|_, _| Box::pin(async {})),
            false,
        ));
        let builder = WorkflowBuilder::default()
            .add_node(node1)
            .unwrap()
            .add_node(node2)
            .unwrap()
            .add_node(node3)
            .unwrap();
        let workflow = builder.build().unwrap();
        assert_eq!(workflow.nodes.len(), 3);
    }

    #[tokio::test]
    async fn test_big_workflow() {
        let edge_0to1_str = Arc::new(Edge::new::<&str>());
        let edge_0to1_u8 = Arc::new(Edge::new::<u8>());
        let edge_0to2 = Arc::new(Edge::new::<&str>());
        let edge_0to3 = Arc::new(Edge::new::<&str>());
        let node0 = NodeBuilder::User(
            UserNodeBuilder::new(
                Box::new(|self_, registry| {
                    Box::pin(async {
                        // 引数の取得

                        // タスクの処理
                        // どこからかデータを取得する

                        // 結果の格納
                        let mut rg = registry.lock().await;
                        rg.store(&self_.outputs()[0], "0 to 1").unwrap();
                        rg.store(&self_.outputs()[1], 0u8).unwrap();
                        rg.store(&self_.outputs()[2], "0 to 2").unwrap();
                        rg.store(&self_.outputs()[3], "0 to 3").unwrap();
                    })
                }),
                false,
            )
            .add_output(edge_0to1_str.clone())
            .add_output(edge_0to1_u8.clone())
            .add_output(edge_0to2.clone())
            .add_output(edge_0to3.clone()),
        );
        let edge_1to3 = Arc::new(Edge::new::<&str>());
        let node1 = NodeBuilder::User(
            UserNodeBuilder::new(
                Box::new(|self_, registry| {
                    Box::pin(async {
                        // 引数の取得
                        let mut rg = registry.lock().await;
                        let a: &str = rg.take(&self_.inputs()[0]).unwrap();
                        assert_eq!(a, "0 to 1");
                        let a: u8 = rg.take(&self_.inputs()[1]).unwrap();
                        assert_eq!(a, 0u8);
                        drop(rg);

                        // タスクの処理

                        // 結果の格納
                        let mut rg = registry.lock().await;
                        rg.store(&self_.outputs()[0], "1 to 3").unwrap();
                    })
                }),
                false,
            )
            .add_input(edge_0to1_str.clone())
            .add_input(edge_0to1_u8.clone())
            .add_output(edge_1to3.clone()),
        );
        let edge_2to5 = Arc::new(Edge::new::<&str>());
        let node2 = NodeBuilder::User(
            UserNodeBuilder::new(
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
            )
            .add_output(edge_2to5.clone()),
        );
        let edge_3to4 = Arc::new(Edge::new::<&str>());
        let edge_3to5 = Arc::new(Edge::new::<&str>());
        let node3 = NodeBuilder::User(
            UserNodeBuilder::new(
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
            )
            .add_input(edge_0to3.clone())
            .add_input(edge_1to3.clone())
            .add_output(edge_3to4.clone())
            .add_output(edge_3to5.clone()),
        );
        let edge_4to5 = Arc::new(Edge::new::<&str>());
        let node4 = NodeBuilder::User(
            UserNodeBuilder::new(
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
            )
            .add_input(edge_3to4.clone())
            .add_output(edge_4to5.clone()),
        );
        let edge_5to6 = Arc::new(Edge::new::<&str>());
        let node5 = NodeBuilder::User(
            UserNodeBuilder::new(
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
            )
            .add_input(edge_2to5.clone())
            .add_input(edge_3to5.clone())
            .add_input(edge_4to5.clone())
            .add_output(edge_5to6.clone()),
        );
        let node6 = NodeBuilder::User(
            UserNodeBuilder::new(
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
            )
            .add_input(edge_5to6.clone()),
        );

        let builder = WorkflowBuilder::default()
            .add_node(node0)
            .unwrap()
            .add_node(node1)
            .unwrap()
            .add_node(node2)
            .unwrap()
            .add_node(node3)
            .unwrap()
            .add_node(node4)
            .unwrap()
            .add_node(node5)
            .unwrap()
            .add_node(node6)
            .unwrap();

        let rg = Arc::new(Mutex::new(Registry::new(WorkflowID::new())));
        let wf = builder.build().unwrap();
        wf.start(rg.lock().await).unwrap();
        let (t0, task0) = wf.get_next(rg.lock().await).unwrap();
        let f0 = task0.run(&rg);
        f0.await;
        wf.done(t0, rg.lock().await);
        assert!(!rg.lock().await.finished);
        let (t1, task1) = wf.get_next(rg.lock().await).unwrap();
        let f1 = task1.run(&rg);
        let (t2, task2) = wf.get_next(rg.lock().await).unwrap();
        let f2 = task2.run(&rg);
        f1.await;
        wf.done(t1, rg.lock().await);
        assert!(!rg.lock().await.finished);
        let (t3, task3) = wf.get_next(rg.lock().await).unwrap();
        let f3 = task3.run(&rg);
        f3.await;
        wf.done(t3, rg.lock().await);
        assert!(!rg.lock().await.finished);
        let (t4, task4) = wf.get_next(rg.lock().await).unwrap();
        let f4 = task4.run(&rg);
        f4.await;
        wf.done(t4, rg.lock().await);
        assert!(!rg.lock().await.finished);
        f2.await;
        wf.done(t2, rg.lock().await);
        assert!(!rg.lock().await.finished);
        let (t5, task5) = wf.get_next(rg.lock().await).unwrap();
        let f5 = task5.run(&rg);
        f5.await;
        wf.done(t5, rg.lock().await);
        assert!(!rg.lock().await.finished);
        let (t6, task6) = wf.get_next(rg.lock().await).unwrap();
        let f6 = task6.run(&rg);
        f6.await;
        wf.done(t6, rg.lock().await);
        assert!(rg.lock().await.finished);
=======
    use crate::node::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_workflow_builder() {
        let node = Arc::new(UserNode::new_test(vec![]).to_node("node"));
        let wf = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .build()
            .0;
        assert_eq!(wf.input_to_node.len(), 0);
    }

    #[test]
    fn test_workflow_builder_add_node() {
        let node = Arc::new(UserNode::new_test(vec![]).to_node("node"));
        let wf_err = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .add_node(node.clone());
        assert_eq!(wf_err.err(), Some(WorkflowError::NodeIsAlreadyAdded));
    }

    #[test]
    fn test_workflow_builder_multi_node() {
        let mut node0 = UserNode::new_test(vec![]);
        let edge = node0.add_output::<i32>();
        let node1 = UserNode::new_test(vec![edge.clone()]);
        let wf = WorkflowBuilder::default()
            .add_node(Arc::new(node0.to_node("node0")))
            .unwrap()
            .add_node(Arc::new(node1.to_node("node1")))
            .unwrap()
            .build()
            .0;
        assert_eq!(wf.input_to_node.len(), 1);
    }

    #[test]
    fn test_workflow_get_node() {
        let edge = Arc::new(Edge::new::<i32>());
        let node0 = UserNode::new_test(vec![edge.clone()]);
        let node0_rc = Arc::new(node0.to_node("node0"));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build()
            .0;
        let node = wf.get_node(&edge).unwrap();
        assert_eq!(node, node0_rc);
>>>>>>> refactor-iter_by_sub_workflow
    }
}
