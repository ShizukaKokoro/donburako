//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::node::edge::Edge;
use crate::node::Node;
use std::collections::{HashMap, HashSet};
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
    pub fn build(self) -> Workflow {
        let mut input_to_node = HashMap::new();
        // すべてのエッジ
        let mut all_edges = HashSet::new();
        // ノードの入力に接続されているエッジ
        let mut input_edges = HashSet::new();
        // ノードの出力に接続されているエッジ
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

        Workflow {
            input_to_node,
            start_edges,
            end_edges,
        }
    }
}

/// ワークフロー
#[derive(Debug)]
pub struct Workflow {
    /// Edge を入力に持つ Node へのマップ
    input_to_node: HashMap<Arc<Edge>, Arc<Node>>,
    // 始端となるエッジ
    start_edges: Vec<Arc<Edge>>,
    // 終端となるエッジ
    end_edges: Vec<Arc<Edge>>,
}
impl Workflow {
    // ワークフローの API は再検討が必要
    // コンテナのワークフローの実行状況を参照する必要がある。
    // ただのデータの保管場所として、プロセッサーが利用するようにするべきかもしれない。 -> 関連関数の削除

    /// ワークフローの開始
    ///
    /// TODO: ワークフローの開始の API を再検討する
    /// 現状 Edge を引数に取るが、最初にデータを入れるべき Edge を保持していない。
    /// 他のワークフローから呼び出すために、コンテナを直接流し込める形である必要がある。
    pub fn start(&self, edge: Arc<Edge>) -> Result<Arc<Node>, WorkflowError> {
        if let Some(node) = self.input_to_node.get(&edge) {
            Ok(node.clone())
        } else {
            Err(WorkflowError::InvalidEdge)
        }
    }

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

    /// ワークフローの始端エッジを取得
    pub fn start_edges(&self) -> &Vec<Arc<Edge>> {
        &self.start_edges
    }

    /// ワークフローの終端エッジを取得
    pub fn end_edges(&self) -> &Vec<Arc<Edge>> {
        &self.end_edges
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
    fn test_workflow_start() {
        let edge = Arc::new(Edge::new::<i32>());
        let node0 = UserNode::new_test(vec![edge.clone()]);
        let node0_rc = Arc::new(node0.to_node("node0"));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node = wf.start(edge.clone()).unwrap();
        assert_eq!(node, node0_rc);
    }

    #[test]
    fn test_workflow_start_invalid_edge() {
        let mut node0 = UserNode::new_test(vec![]);
        let edge = node0.add_output::<i32>();
        let node0_rc = Arc::new(node0.to_node("node0"));
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node_err = wf.start(edge.clone());
        assert_eq!(node_err.err(), Some(WorkflowError::InvalidEdge));
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

    #[test]
    fn test_workflow_start_edges() {
        let edge0 = Arc::new(Edge::new::<i32>());
        let edge1 = Arc::new(Edge::new::<i32>());
        let node0 = UserNode::new_test(vec![edge0.clone()]);
        let node1 = UserNode::new_test(vec![edge1.clone()]);
        let wf = WorkflowBuilder::default()
            .add_node(Arc::new(node0.to_node("node0")))
            .unwrap()
            .add_node(Arc::new(node1.to_node("node1")))
            .unwrap()
            .build();
        let mut result = HashSet::new();
        for edge in wf.start_edges() {
            let _ = result.insert(edge.clone());
        }
        let mut expected = HashSet::new();
        let _ = expected.insert(edge0);
        let _ = expected.insert(edge1);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_workflow_end_edges() {
        let mut node0 = UserNode::new_test(vec![]);
        let edge0 = node0.add_output::<i32>();
        let mut node1 = UserNode::new_test(vec![]);
        let edge1 = node1.add_output::<i32>();
        let wf = WorkflowBuilder::default()
            .add_node(Arc::new(node0.to_node("node0")))
            .unwrap()
            .add_node(Arc::new(node1.to_node("node1")))
            .unwrap()
            .build();
        let mut result = HashSet::new();
        for edge in wf.end_edges() {
            let _ = result.insert(edge.clone());
        }
        let mut expected = HashSet::new();
        let _ = expected.insert(edge0);
        let _ = expected.insert(edge1);
        assert_eq!(result, expected);
    }
}
