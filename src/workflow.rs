//! ワークフローモジュール
//!
//! ワークフローは、複数のステップからなる処理の流れを表現するための構造体。
//! これは必ずイミュータブルな構造体であり、ビルダーを用いて構築する。

use crate::edge::Edge;
use crate::graph::Graph;
use crate::node::Node;
use std::sync::Arc;

#[derive(Default)]
struct WorkflowBuilder {
    nodes: Vec<Box<dyn Node>>,
    graph: Option<Graph>,
}
impl WorkflowBuilder {
    fn add_node(&mut self, node: impl Node) {
        if self.graph.is_some() {
            panic!("Cannot add node after building graph");
        }
        self.nodes.push(Box::new(node));
    }

    fn add_edge<T: 'static + Send + Sync>(&mut self, from: usize, to: usize) {
        self.graph.as_mut().unwrap().add_edge(from, to).unwrap();
        let edge = Arc::new(Edge::new::<T>());
        self.nodes[from].add_output(edge.clone());
        self.nodes[to].add_input(edge);
    }

    fn build(self) -> Workflow {
        Workflow {
            nodes: self.nodes,
            graph: self.graph.unwrap(),
        }
    }
}

#[derive(Debug)]
struct Workflow {
    nodes: Vec<Box<dyn Node>>,
    graph: Graph,
}
