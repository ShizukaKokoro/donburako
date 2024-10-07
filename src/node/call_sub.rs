//! サブワークフロー呼び出しノードモジュール
//!
//! 処理の一環で、サブワークフローを呼び出すノードを定義する。
//! 再帰やループの中身をサブワークフローとして定義することで、柔軟に処理を構築できる。

use log::debug;

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
        let wf_id = op.get_workflow_id(exec_id).await.unwrap();
        let (start, end) = op.get_start_end_edges(wf_id).await;
        let id = ExecutorId::new();
        debug!("start workflow: {:?}({:?}) in {:?}", wf_id, id, exec_id);
        op.start_workflow(id, wf_id).await;
        for (i, edge) in start.iter().enumerate() {
            let con = op
                .get_container(self.input[i].clone(), exec_id)
                .await
                .unwrap();
            op.add_container(edge.clone(), id, con).await.unwrap();
        }
        op.wait_finish(id, 5).await;
        for (i, edge) in end.iter().enumerate() {
            let con = op.get_container(edge.clone(), id).await.unwrap();
            op.add_container(self.output[i].clone(), exec_id, con)
                .await
                .unwrap();
        }
        op.finish_containers(id).await;
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
