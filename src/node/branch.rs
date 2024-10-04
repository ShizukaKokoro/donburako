//! 分岐ノードモジュール
//!
//! データの流れを分岐させ、統合するノードを定義する。

use super::edge::Edge;
use super::*;
use std::sync::Arc;

/// 分岐ノード
///
/// 真偽値を受け取り、真の場合と偽の場合にそれぞれ異なるエッジにユニット型のデータを送る。
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
    pub fn input(&self) -> &Arc<Edge> {
        &self.input
    }

    /// 真の出力エッジの取得
    pub fn true_output(&self) -> &Arc<Edge> {
        self.true_output.as_ref().unwrap()
    }

    /// 偽の出力エッジの取得
    pub fn false_output(&self) -> &Arc<Edge> {
        self.false_output.as_ref().unwrap()
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::If(self), name)
    }
}

/// 最速ノード
///
/// 複数の同じ型のエッジを受け取り、最も早く到着したデータを出力する。
#[derive(Debug)]
pub struct FirstChoiceNode {
    inputs: Vec<Arc<Edge>>,
    outputs: Option<Arc<Edge>>,
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
            outputs: None,
        })
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Result<Arc<Edge>, NodeError> {
        if self.outputs.is_some() {
            return Err(NodeError::OutputEdgeExists);
        }
        if !self.inputs[0].check_type::<T>() {
            return Err(NodeError::EdgeTypeMismatch);
        }
        let edge = Arc::new(Edge::new::<T>());
        self.outputs = Some(edge.clone());
        Ok(edge)
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.inputs
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Arc<Edge> {
        self.outputs.as_ref().unwrap()
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::FirstChoice(self), name)
    }
}

#[cfg(test)]
mod test {
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
        assert_eq!(node.outputs.as_ref().unwrap(), &edge);
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
}
