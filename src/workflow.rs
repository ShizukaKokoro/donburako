//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::node::{InputPort, Node, OutputPort};
use std::{collections::HashMap, rc::Rc};
use thiserror::Error;

/// ワークフローエラー
#[derive(Debug, Error, PartialEq)]
pub enum WorkflowError {
    /// ノードがすでに追加されている
    #[error("Node is already added")]
    NodeIsAlreadyAdded,
}

/// ワークフロービルダー
#[derive(Default)]
pub struct WorkflowBuilder {
    nodes: Vec<Rc<Node>>,
}
impl WorkflowBuilder {
    /// ノードの追加
    pub fn add_node(self, node: Rc<Node>) -> Result<Self, WorkflowError> {
        let mut nodes = self.nodes;
        if nodes.contains(&node) {
            return Err(WorkflowError::NodeIsAlreadyAdded);
        }
        nodes.push(node);
        Ok(Self { nodes })
    }

    /// ワークフローの生成
    pub fn build(self) -> Workflow {
        let mut node_map = HashMap::new();
        let mut from_map = HashMap::new();

        for node in self.nodes {
            for input in node.inputs() {
                let _ = node_map.insert(input.clone(), node.clone());
            }
            for output in node.outputs() {
                let input = output.input();
                let _ = from_map.insert(input.clone(), output.clone());
            }
        }

        Workflow { node_map, from_map }
    }
}

/// ワークフロー
pub struct Workflow {
    /// InputPort から Node へのマップ
    node_map: HashMap<Rc<InputPort>, Rc<Node>>,

    /// InputPort から OutputPort への逆マップ
    from_map: HashMap<Rc<InputPort>, Rc<OutputPort>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::UserNode;

    #[test]
    fn test_workflow_builder() {
        let node = Rc::new(Node::User(UserNode::default()));
        let wf = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .build();
        assert_eq!(wf.node_map.len(), 0);
        assert_eq!(wf.from_map.len(), 0);
    }

    #[test]
    fn test_workflow_builder_add_node() {
        let node = Rc::new(Node::User(UserNode::default()));
        let wf_err = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .add_node(node.clone());
        assert_eq!(wf_err.err(), Some(WorkflowError::NodeIsAlreadyAdded));
    }

    #[test]
    fn test_workflow_builder_multi_node() {
        let mut node0 = UserNode::default();
        let ip = node0.add_output::<i32>();
        let node1 = UserNode::new(vec![ip.clone()]);
        let wf = WorkflowBuilder::default()
            .add_node(Rc::new(Node::User(node0)))
            .unwrap()
            .add_node(Rc::new(Node::User(node1)))
            .unwrap()
            .build();
        assert_eq!(wf.node_map.len(), 1);
        assert_eq!(wf.from_map.len(), 1);
    }
}
