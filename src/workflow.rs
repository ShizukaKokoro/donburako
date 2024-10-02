//! ワークフローモジュール
//!
//! ワークフローはノードからなる有向グラフ。

use crate::node::{Edge, Node};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
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
    input_to_node: HashMap<Arc<Edge>, Rc<Node>>,

    /// Edge から出力する Node へのマップ
    output_to_node: HashMap<Arc<Edge>, Rc<Node>>,
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
    pub fn start(&self, edge: Arc<Edge>) -> Result<Rc<Node>, WorkflowError> {
        if let Some(node) = self.input_to_node.get(&edge) {
            Ok(node.clone())
        } else {
            Err(WorkflowError::InvalidEdge)
        }
    }

    /// ノードの完了と次のノードの取得
    ///
    /// 終了したノードから次のノードを取得する。
    /// TODO: 全ての入力が揃った時に次のノードを取得するようにする
    /// 現状は、単に出力に接続されたノードを取得している。
    /// コンテナがワークフローの実行状態を保持しているため、ここと連携する必要がある。
    pub fn done(&self, node: Rc<Node>) -> Result<Vec<Rc<Node>>, WorkflowError> {
        let mut next_nodes = vec![];
        let mut nodes_set = HashSet::new();
        for output in node.outputs() {
            if let Some(next_node) = self.output_to_node.get(output) {
                if nodes_set.contains(next_node) {
                    continue;
                }
                next_nodes.push(next_node.clone());
                assert!(nodes_set.insert(next_node));
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
        let node = Rc::new(UserNode::new_test(vec![]).to_node());
        let wf = WorkflowBuilder::default()
            .add_node(node.clone())
            .unwrap()
            .build();
        assert_eq!(wf.input_to_node.len(), 0);
        assert_eq!(wf.output_to_node.len(), 0);
    }

    #[test]
    fn test_workflow_builder_add_node() {
        let node = Rc::new(UserNode::new_test(vec![]).to_node());
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
            .add_node(Rc::new(node0.to_node()))
            .unwrap()
            .add_node(Rc::new(node1.to_node()))
            .unwrap()
            .build();
        assert_eq!(wf.input_to_node.len(), 1);
        assert_eq!(wf.output_to_node.len(), 1);
    }

    #[test]
    fn test_workflow_start() {
        let edge = Arc::new(Edge::new::<i32>());
        let node0 = UserNode::new_test(vec![edge.clone()]);
        let node0_rc = Rc::new(node0.to_node());
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
        let node0_rc = Rc::new(node0.to_node());
        let wf = WorkflowBuilder::default()
            .add_node(node0_rc.clone())
            .unwrap()
            .build();
        let node_err = wf.start(edge.clone());
        assert_eq!(node_err.err(), Some(WorkflowError::InvalidEdge));
    }

    #[test]
    fn test_workflow_done() {
        let mut node0 = UserNode::new_test(vec![]);
        let edge = node0.add_output::<i32>();
        let node1 = UserNode::new_test(vec![edge.clone()]);
        let node0_rc = Rc::new(node0.to_node());
        let node1_rc = Rc::new(node1.to_node());
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
