//! ワークフローモジュール
//!
//! ワークフローは、複数のステップからなる処理の流れを表現するための構造体。
//! これは必ずイミュータブルな構造体であり、ビルダーを用いて構築する。

use crate::edge::Edge;
use crate::graph::Graph;
use crate::node::{Node, NodeBuilder};
use std::sync::Arc;

#[derive(Default)]
struct WorkflowBuilder {
    nodes: Vec<NodeBuilder>,
    graph: Option<Graph>,
}
impl WorkflowBuilder {
    fn add_node(self, node: NodeBuilder) -> Self {
        if self.graph.is_some() {
            panic!("Cannot add node after building graph");
        }
        let mut nodes = self.nodes;
        nodes.push(node);
        Self { nodes, ..self }
    }

    fn finish_nodes(self) -> Self {
        Self {
            graph: Some(Graph::new(self.nodes.len())),
            ..self
        }
    }

    fn add_edge<T: 'static + Send + Sync>(mut self, from: usize, to: usize) -> Self {
        self.graph.as_mut().unwrap().add_edge(from, to).unwrap();
        let edge = Arc::new(Edge::new::<T>());
        self.nodes[from].add_output(edge.clone());
        self.nodes[to].add_input(edge);
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
            .add_edge::<i32>(0, 1)
            .add_edge::<i32>(1, 2);
        let workflow = builder.build();
        assert_eq!(workflow.nodes.len(), 3);
    }
}
