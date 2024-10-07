//! サブワークフロー呼び出しノードモジュール
//!
//! 処理の一環で、サブワークフローを呼び出すノードを定義する。
//! 再帰やループの中身をサブワークフローとして定義することで、柔軟に処理を構築できる。

use super::edge::Edge;
use super::*;
use std::sync::Arc;

/// 再帰ノード
///
/// 自身を呼び出すノード
#[derive(Debug)]
pub struct RecursiveNode {
    /// 入力エッジ
    input: Vec<Arc<Edge>>,
    /// 出力エッジ
    output: Vec<Arc<Edge>>,
}
impl RecursiveNode {
    /// ノードの生成
    pub fn new(input: Vec<Arc<Edge>>) -> Self {
        RecursiveNode {
            input,
            output: Vec::new(),
        }
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Arc<Edge> {
        let edge = Arc::new(Edge::new::<T>());
        self.output.push(edge.clone());
        edge
    }

    /// 入力エッジの取得
    pub(crate) fn inputs(&self) -> &Vec<Arc<Edge>> {
        &self.input
    }

    /// 出力エッジの取得
    pub(super) fn outputs(&self) -> &Vec<Arc<Edge>> {
        &self.output
    }

    /// ノードに変換
    pub fn to_node(self, name: &'static str) -> Node {
        Node::new(NodeType::Recursive(self), name)
    }

    /// ノードの実行
    pub(super) async fn run(&self, op: &Operator, exec_id: ExecutorId) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recursive_node_new() {
        let edge = Arc::new(Edge::new::<&str>());
        let node = RecursiveNode::new(vec![edge.clone()]);
        assert_eq!(node.inputs(), &vec![edge]);
    }

    #[test]
    fn test_recursive_node_add_output() {
        let edge = Arc::new(Edge::new::<&str>());
        let mut node = RecursiveNode::new(vec![edge.clone()]);
        let output = node.add_output::<i32>();
        assert_eq!(node.output, vec![output.clone()]);
    }
}
