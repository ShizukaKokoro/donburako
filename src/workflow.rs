//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::node::{Edge, Node};
use std::{collections::HashMap, rc::Rc};
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
        let mut input_to_node = HashMap::new();
        let mut output_to_node = HashMap::new();

        for node in self.nodes.iter() {
            for input in node.inputs() {
                let _ = input_to_node.insert(input.clone(), node.clone());
            }
        }
        for node in self.nodes.iter() {
            for output in node.outputs() {
                if let Some(node_from) = input_to_node.get(output) {
                    let _ = output_to_node.insert(output.clone(), node_from.clone());
                }
            }
        }

        Workflow {
            input_to_node,
            output_to_node,
        }
    }
}

/// ワークフロー
pub struct Workflow {
    /// Edge を入力に持つ Node へのマップ
    input_to_node: HashMap<Rc<Edge>, Rc<Node>>,

    /// Edge から出力する Node へのマップ
    output_to_node: HashMap<Rc<Edge>, Rc<Node>>,
}
impl Workflow {
    /// ワークフローの開始
    ///
    /// 何度も
    pub fn start(&self, edge: Rc<Edge>) -> Result<Rc<Node>, WorkflowError> {
        if let Some(node) = self.input_to_node.get(&edge) {
            Ok(node.clone())
        } else {
            Err(WorkflowError::InvalidEdge)
        }
    }

    /// ノードの完了と次のノードの取得
    pub fn done(&self, node: Rc<Node>) -> Result<Vec<Rc<Node>>, WorkflowError> {
        let mut next_nodes = vec![];
        for output in node.outputs() {
            if let Some(next_node) = self.output_to_node.get(output) {
                next_nodes.push(next_node.clone());
            }
        }
        Ok(next_nodes)
    }
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
        assert_eq!(wf.input_to_node.len(), 0);
        assert_eq!(wf.output_to_node.len(), 0);
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
        let edge = node0.add_output::<i32>();
        let node1 = UserNode::new(vec![edge.clone()]);
        let wf = WorkflowBuilder::default()
            .add_node(Rc::new(Node::User(node0)))
            .unwrap()
            .add_node(Rc::new(Node::User(node1)))
            .unwrap()
            .build();
        assert_eq!(wf.input_to_node.len(), 1);
        assert_eq!(wf.output_to_node.len(), 1);
    }

    #[test]
    fn test_workflow_start() {
        let edge = Rc::new(Edge::new::<i32>());
        let node0 = UserNode::new(vec![edge.clone()]);
        let node0_rc = Rc::new(Node::User(node0));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node = wf.start(edge.clone()).unwrap();
        assert_eq!(node, node0_rc);
    }

    #[test]
    fn test_workflow_start_invalid_edge() {
        let mut node0 = UserNode::default();
        let edge = node0.add_output::<i32>();
        let node0_rc = Rc::new(Node::User(node0));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node_err = wf.start(edge.clone());
        assert_eq!(node_err.err(), Some(WorkflowError::InvalidEdge));
    }

    #[test]
    fn test_workflow_done() {
        let mut node0 = UserNode::default();
        let edge = node0.add_output::<i32>();
        let node1 = UserNode::new(vec![edge.clone()]);
        let node0_rc = Rc::new(Node::User(node0));
        let node1_rc = Rc::new(Node::User(node1));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .add_node(node1_rc.clone())
            .unwrap()
            .build();
        let next_nodes = wf.done(node0_rc).unwrap();
        assert_eq!(next_nodes, vec![node1_rc]);
    }
}
