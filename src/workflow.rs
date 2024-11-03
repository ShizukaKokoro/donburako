//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::edge::Edge;
use crate::node::Node;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use tracing::debug;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowId(&'static str);
impl WorkflowId {
    /// 新しいワークフローIDを生成
    pub fn new(id: &'static str) -> Self {
        Self(id)
    }
}

/// ワークフロービルダー
#[derive(Default)]
pub struct WorkflowBuilder {
    nodes: Vec<Arc<Node>>,
    ignored_edges: HashSet<Arc<Edge>>,
}
impl WorkflowBuilder {
    /// ノードの追加
    pub fn add_node(self, node: Arc<Node>) -> Result<Self, WorkflowError> {
        let mut nodes = self.nodes;
        if nodes.contains(&node) {
            return Err(WorkflowError::NodeIsAlreadyAdded);
        }
        nodes.push(node);
        Ok(Self { nodes, ..self })
    }

    /// エッジの無視
    pub fn ignore_edge(self, edge: Arc<Edge>) -> Self {
        let mut ignored_edges = self.ignored_edges;
        let _ = ignored_edges.insert(edge);
        Self {
            ignored_edges,
            ..self
        }
    }

    /// ワークフローの生成
    pub(crate) fn build(self) -> Workflow {
        let mut input_to_node = HashMap::new();
        let mut all_edges = HashSet::new();
        let mut input_edges = HashSet::new();
        let mut output_edges = HashSet::new();
        let mut start_nodes = HashSet::new();

        for node in self.nodes.iter() {
            debug!(
                "{:?} -->| {:?} | --> {:?}",
                node.inputs(),
                node.name(),
                node.outputs()
            );
            if node.inputs().is_empty() {
                let _ = start_nodes.insert(node.clone());
            }
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
        debug!("start_edges: {:?}", start_edges);
        debug!("end_edges: {:?}", end_edges);
        Workflow {
            input_to_node,
            start_edges,
            end_edges,
            ignore_edges: self.ignored_edges,
            start_nodes,
        }
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
    /// 無視するエッジ
    ignore_edges: HashSet<Arc<Edge>>,
    /// 入力を持たない開始ノード
    start_nodes: HashSet<Arc<Node>>,
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

    /// エッジが無視されるか
    pub(crate) fn is_ignored(&self, edge: &Arc<Edge>) -> bool {
        self.ignore_edges.contains(edge)
    }

    /// 開始ノードを取得
    pub(crate) fn start_nodes(&self) -> &HashSet<Arc<Node>> {
        &self.start_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Choice;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_workflow_builder() {
        let node = Arc::new(Node::new_test(vec![], vec![], "node", Choice::All));
        let wf = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .build();
        assert_eq!(wf.input_to_node.len(), 0);
    }

    #[test]
    fn test_workflow_builder_add_node() {
        let node = Arc::new(Node::new_test(vec![], vec![], "node", Choice::All));
        let wf_err = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .add_node(node.clone());
        assert_eq!(wf_err.err(), Some(WorkflowError::NodeIsAlreadyAdded));
    }

    #[test]
    fn test_workflow_builder_multi_node() {
        let edge = Arc::new(Edge::new::<i32>());
        let node0 = Node::new_test(vec![], vec![edge.clone()], "node0", Choice::All);
        let node1 = Node::new_test(vec![edge.clone()], vec![], "node1", Choice::All);
        let wf = WorkflowBuilder::default()
            .add_node(Arc::new(node0))
            .unwrap()
            .add_node(Arc::new(node1))
            .unwrap()
            .build();
        assert_eq!(wf.input_to_node.len(), 1);
    }

    #[test]
    fn test_workflow_get_node() {
        let edge = Arc::new(Edge::new::<i32>());
        let node0 = Node::new_test(vec![edge.clone()], vec![], "node0", Choice::All);
        let node0_rc = Arc::new(node0);
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node = wf.get_node(&edge).unwrap();
        assert_eq!(node, node0_rc);
    }
}
