//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::node::edge::Edge;
use crate::node::Node;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

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

/// ワークフロービルダー
#[derive(Default)]
pub struct WorkflowBuilder {
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
        Ok(Self { nodes })
    }

    /// ワークフローの生成
    pub(crate) fn build(self) -> Workflow {
        let mut input_to_node = HashMap::new();

        for node in self.nodes.iter() {
            for input in node.inputs() {
                let _ = input_to_node.insert(input.clone(), node.clone());
            }
        }

        Workflow { input_to_node }
    }
}

/// ワークフロー
#[derive(Debug)]
pub(crate) struct Workflow {
    /// Edge を入力に持つ Node へのマップ
    input_to_node: HashMap<Arc<Edge>, Arc<Node>>,
}
impl Workflow {
    /// エッジからノードを取得
    ///
    /// 指定されたエッジが入力になっているノードを取得する。
    ///
    /// # Arguments
    ///
    /// * `edge` - エッジ
    pub fn get_node(&self, edge: &Arc<Edge>) -> Option<Arc<Node>> {
        self.input_to_node.get(edge).cloned()
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
            .build();
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
            .build();
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
            .build();
        let node = wf.get_node(&edge).unwrap();
        assert_eq!(node, node0_rc);
    }
}
