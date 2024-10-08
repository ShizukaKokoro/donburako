//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::edge::Edge;
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
        nodes.push(node);
        Ok(Self { nodes, ..self })
    }

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }
}
