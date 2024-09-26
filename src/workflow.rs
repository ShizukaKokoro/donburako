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
    fn add_node(&mut self, node: NodeBuilder) {
        if self.graph.is_some() {
            panic!("Cannot add node after building graph");
        }
        self.nodes.push(node);
    }

    fn finish_nodes(&mut self) {
        self.graph = Some(Graph::new(self.nodes.len()));
    }

    fn add_edge<T: 'static + Send + Sync>(&mut self, from: usize, to: usize) {
        self.graph.as_mut().unwrap().add_edge(from, to).unwrap();
        let edge = Arc::new(Edge::new::<T>());
        self.nodes[from].add_output(edge.clone());
        self.nodes[to].add_input(edge);
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
        let mut builder = WorkflowBuilder::default();
        let node1 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        let node2 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        let node3 = NodeBuilder::new(Box::new(|_, _| Box::pin(async {})));
        builder.add_node(node1);
        builder.add_node(node2);
        builder.add_node(node3);
        builder.finish_nodes();
        builder.add_edge::<i32>(0, 1);
        builder.add_edge::<i32>(1, 2);
        let workflow = builder.build();
        assert_eq!(workflow.nodes.len(), 3);
    }
}
