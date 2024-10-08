//! 分岐ノードモジュール
//!
//! データの流れを分岐させ、統合するノードを定義する。

use super::*;
use crate::edge::Edge;
use std::sync::Arc;

/// 分岐ノード
///
/// 真偽値を受け取り、真の場合と偽の場合にそれぞれ異なるエッジにユニット型のデータを送る。
/// 分岐ノードで終わるワークフローは、終了確認ができないため、必ず統合ノードを追加する必要がある。
#[derive(Debug)]
pub struct IfNode {
    input: Arc<Edge>,
    true_output: Option<Arc<Edge>>,
    false_output: Option<Arc<Edge>>,
}
impl IfNode {
    /// ノードの生成
    pub fn new(input: Arc<Edge>) -> Result<Self, NodeError> {
        if !input.check_type::<bool>() {
            return Err(NodeError::EdgeTypeMismatch);
        }
        Ok(IfNode {
            input,
            true_output: None,
            false_output: None,
        })
    }

    /// 真の出力エッジの追加
    pub fn add_true_output(&mut self) -> Result<Arc<Edge>, NodeError> {
        if self.true_output.is_some() {
            return Err(NodeError::OutputEdgeExists);
        }
        let edge = Arc::new(Edge::new::<()>());
        self.true_output = Some(edge.clone());
        Ok(edge)
    }

    /// 偽の出力エッジの追加
    pub fn add_false_output(&mut self) -> Result<Arc<Edge>, NodeError> {
        if self.false_output.is_some() {
            return Err(NodeError::OutputEdgeExists);
        }
        let edge = Arc::new(Edge::new::<()>());
        self.false_output = Some(edge.clone());
        Ok(edge)
    }

    /// 入力エッジの取得
    pub(crate) fn input(&self) -> &Arc<Edge> {
        &self.input
    }

    /// 真の出力エッジの取得
    pub(super) fn true_output(&self) -> &Arc<Edge> {
        self.true_output.as_ref().unwrap()
    }

    /// 偽の出力エッジの取得
    pub(super) fn false_output(&self) -> &Arc<Edge> {
        self.false_output.as_ref().unwrap()
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::If(self), name)
    }

    pub(super) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        let mut con = op.get_container(self.input.clone(), exec_id).await.unwrap();
        if con.take::<bool>().unwrap() {
            con.store(());
            op.add_container(self.true_output().clone(), exec_id, con)
                .await
                .unwrap();
        } else {
            con.store(());
            op.add_container(self.false_output().clone(), exec_id, con)
                .await
                .unwrap();
        }
    }
}

/// 最速ノード
///
/// 複数の同じ型のエッジを受け取り、最も早く到着したデータを出力する。
#[derive(Debug)]
pub struct FirstChoiceNode {
    inputs: Vec<Arc<Edge>>,
    output: Option<Arc<Edge>>,
}
impl FirstChoiceNode {
    /// ノードの生成
    pub fn new(inputs: Vec<Arc<Edge>>) -> Result<Self, NodeError> {
        let ty = inputs[0].ty;
        if inputs.iter().any(|edge| edge.ty != ty) {
            return Err(NodeError::EdgeTypeMismatch);
        }
        Ok(FirstChoiceNode {
            inputs,
            output: None,
        })
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Result<Arc<Edge>, NodeError> {
        if self.output.is_some() {
            return Err(NodeError::OutputEdgeExists);
        }
        if !self.inputs[0].check_type::<T>() {
            return Err(NodeError::EdgeTypeMismatch);
        }
        let edge = Arc::new(Edge::new::<T>());
        self.output = Some(edge.clone());
        Ok(edge)
    }

    /// 入力エッジの取得
    pub(crate) fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// 出力エッジの取得
    pub(super) fn output(&self) -> &Arc<Edge> {
        self.output.as_ref().unwrap()
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::FirstChoice(self), name)
    }

    pub(super) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        for edge in self.inputs() {
            let con = op.get_container(edge.clone(), exec_id).await;
            if con.is_some() {
                let con = con.unwrap();
                op.add_container(self.output().clone(), exec_id, con)
                    .await
                    .unwrap();
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::workflow::WorkflowBuilder;

    use super::*;

    #[test]
    fn test_if_node_new() {
        let edge = Arc::new(Edge::new::<bool>());
        let _ = IfNode::new(edge.clone()).unwrap();
    }

    #[test]
    fn test_if_node_new_error() {
        let edge = Arc::new(Edge::new::<i32>());
        let node = IfNode::new(edge);
        assert_eq!(node.err().unwrap(), NodeError::EdgeTypeMismatch);
    }

    #[test]
    fn test_if_node_add_true_output() {
        let edge = Arc::new(Edge::new::<bool>());
        let mut node = IfNode::new(edge.clone()).unwrap();
        let edge = node.add_true_output().unwrap();
        assert_eq!(node.true_output.as_ref().unwrap(), &edge);
    }

    #[test]
    fn test_if_node_add_true_output_error_exist() {
        let edge = Arc::new(Edge::new::<bool>());
        let mut node = IfNode::new(edge.clone()).unwrap();
        let _ = node.add_true_output().unwrap();
        let edge = node.add_true_output();
        assert_eq!(edge.err().unwrap(), NodeError::OutputEdgeExists);
    }

    #[test]
    fn test_if_node_add_false_output() {
        let edge = Arc::new(Edge::new::<bool>());
        let mut node = IfNode::new(edge.clone()).unwrap();
        let edge = node.add_false_output().unwrap();
        assert_eq!(node.false_output.as_ref().unwrap(), &edge);
    }

    #[test]
    fn test_if_node_add_false_output_error_exist() {
        let edge = Arc::new(Edge::new::<bool>());
        let mut node = IfNode::new(edge.clone()).unwrap();
        let _ = node.add_false_output().unwrap();
        let edge = node.add_false_output();
        assert_eq!(edge.err().unwrap(), NodeError::OutputEdgeExists);
    }

    #[tokio::test]
    async fn test_if_node_run() {
        let edge_input = Arc::new(Edge::new::<bool>());
        let mut node = IfNode::new(edge_input.clone()).unwrap();
        let edge_true = node.add_true_output().unwrap();
        let edge_false = node.add_false_output().unwrap();
        let builder = WorkflowBuilder::default()
            .add_node(Arc::new(node.to_node("node")))
            .unwrap();
        let wf_id = builder.id();
        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, wf_id).await;

        op.add_new_container(edge_input.clone(), exec_id, true)
            .await
            .unwrap();
        let node = op.get_next_node().await.unwrap().0;
        node.run(&op, exec_id).await;
        assert!(op
            .get_container(edge_false.clone(), exec_id)
            .await
            .is_none());
        assert!(op.get_container(edge_true.clone(), exec_id).await.is_some());

        op.add_new_container(edge_input.clone(), exec_id, false)
            .await
            .unwrap();
        let node = op.get_next_node().await.unwrap().0;
        node.run(&op, exec_id).await;
        assert!(op.get_container(edge_true, exec_id).await.is_none());
        assert!(op.get_container(edge_false, exec_id).await.is_some());
    }

    #[test]
    fn test_first_choice_node_new() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        assert_eq!(node.inputs.len(), 2);
    }

    #[test]
    fn test_first_choice_node_new_error() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<f32>());
        let node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]);
        assert_eq!(node.err().unwrap(), NodeError::EdgeTypeMismatch);
    }

    #[test]
    fn test_first_choice_node_add_output() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let edge = node.add_output::<i32>().unwrap();
        assert_eq!(node.output.as_ref().unwrap(), &edge);
    }

    #[test]
    fn test_first_choice_node_add_output_error_exist() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let _ = node.add_output::<i32>().unwrap();
        let edge = node.add_output::<i32>();
        assert_eq!(edge.err().unwrap(), NodeError::OutputEdgeExists);
    }

    #[test]
    fn test_first_choice_node_add_output_error_type() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let edge = node.add_output::<f32>();
        assert_eq!(edge.err().unwrap(), NodeError::EdgeTypeMismatch);
    }

    #[tokio::test]
    async fn test_first_choice_node_run() {
        let edge1 = Arc::new(Edge::new::<i32>());
        let edge2 = Arc::new(Edge::new::<i32>());
        let mut node = FirstChoiceNode::new(vec![edge1.clone(), edge2.clone()]).unwrap();
        let edge = node.add_output::<i32>().unwrap();
        let builder = WorkflowBuilder::default()
            .add_node(Arc::new(node.to_node("node")))
            .unwrap();
        let wf_id = builder.id();
        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, wf_id).await;

        op.add_new_container(edge1.clone(), exec_id, 1)
            .await
            .unwrap();
        let node = op.get_next_node().await.unwrap().0;
        node.run(&op, exec_id).await;
        assert!(op.get_container(edge.clone(), exec_id).await.is_some());

        op.add_new_container(edge2.clone(), exec_id, 2)
            .await
            .unwrap();
        let node = op.get_next_node().await.unwrap().0;
        node.run(&op, exec_id).await;
        assert!(op.get_container(edge, exec_id).await.is_some());
        assert!(op.is_finished(exec_id).await);
    }
}
