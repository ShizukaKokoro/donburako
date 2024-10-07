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
    use crate::workflow::WorkflowBuilder;

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

    #[tokio::test]
    async fn test_recursive_node_run() {
        /*
        fn sum(n: i32) -> i32 {
            if n == 0 {
                0
            } else {
                n + sum(n - 1)
            }
         */
        let edge_n0 = Arc::new(Edge::new::<i32>());
        let edge_n1 = Arc::new(Edge::new::<i32>());
        let edge_n2 = Arc::new(Edge::new::<i32>());
        let mut node_is_zero = UserNode::new(
            vec![edge_n0.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut con = op
                        .get_container(self_.inputs()[0].clone(), exec_id)
                        .await
                        .unwrap();
                    let n: i32 = con.take().unwrap();
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store(n == 0);
                    op.add_container(self_.outputs()[0].clone(), exec_id, con_clone)
                        .await
                        .unwrap();
                })
            }),
            false,
        );
        let edge_is_zero = node_is_zero.add_output::<bool>();
        let mut if_node = IfNode::new(edge_is_zero.clone()).unwrap();
        let edge_true = if_node.add_true_output().unwrap();
        let edge_false = if_node.add_false_output().unwrap();
        let mut node_zero = UserNode::new(
            vec![edge_true.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut con = op
                        .get_container(self_.inputs()[0].clone(), exec_id)
                        .await
                        .unwrap();
                    let _: () = con.take().unwrap();
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store(0);
                    op.add_container(self_.outputs()[0].clone(), exec_id, con_clone)
                        .await
                        .unwrap();
                })
            }),
            false,
        );
        let edge_zero = node_zero.add_output::<i32>();
        let mut sub = UserNode::new(
            vec![edge_false.clone(), edge_n1.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut con = op
                        .get_container(self_.inputs()[0].clone(), exec_id)
                        .await
                        .unwrap();
                    let _: () = con.take().unwrap();
                    let mut con = op
                        .get_container(self_.inputs()[1].clone(), exec_id)
                        .await
                        .unwrap();
                    let n: i32 = con.take().unwrap();
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store(n - 1);
                    op.add_container(self_.outputs()[0].clone(), exec_id, con_clone)
                        .await
                        .unwrap();
                })
            }),
            false,
        );
        let edge_sub = sub.add_output::<i32>();
        let mut rec = RecursiveNode::new(vec![edge_sub.clone()]);
        let edge_rec = rec.add_output::<i32>();
        let mut node_add = UserNode::new(
            vec![edge_n2.clone(), edge_rec.clone()],
            Box::new(|self_, op, exec_id| {
                Box::pin(async move {
                    let mut con = op
                        .get_container(self_.inputs()[0].clone(), exec_id)
                        .await
                        .unwrap();
                    let n: i32 = con.take().unwrap();
                    let mut con = op
                        .get_container(self_.inputs()[1].clone(), exec_id)
                        .await
                        .unwrap();
                    let rec: i32 = con.take().unwrap();
                    let mut con_clone = con.clone_container().unwrap();
                    con_clone.store(n + rec);
                    op.add_container(self_.outputs()[0].clone(), exec_id, con_clone)
                        .await
                        .unwrap();
                })
            }),
            false,
        );
        let edge_add = node_add.add_output::<i32>();
        let mut node_select =
            FirstChoiceNode::new(vec![edge_zero.clone(), edge_add.clone()]).unwrap();
        let edge_select = node_select.add_output::<i32>().unwrap();

        let builder = WorkflowBuilder::default()
            .add_node(Arc::new(node_is_zero.to_node("is_zero")))
            .unwrap()
            .add_node(Arc::new(if_node.to_node("if")))
            .unwrap()
            .add_node(Arc::new(node_zero.to_node("zero")))
            .unwrap()
            .add_node(Arc::new(sub.to_node("sub")))
            .unwrap()
            .add_node(Arc::new(rec.to_node("rec")))
            .unwrap()
            .add_node(Arc::new(node_add.to_node("add")))
            .unwrap()
            .add_node(Arc::new(node_select.to_node("select")))
            .unwrap();

        let op = Operator::new(vec![builder]);
        let exec_id = ExecutorId::new();
        op.start_workflow(exec_id, 0).await;
        op.add_new_container(edge_n0, exec_id, 10).await.unwrap();
        op.add_new_container(edge_n1, exec_id, 10).await.unwrap();
        op.add_new_container(edge_n2, exec_id, 10).await.unwrap();

        let n_is_zero = op.get_next_node().await.unwrap().0;
        n_is_zero.run(&op, exec_id).await;
        let n_if = op.get_next_node().await.unwrap().0;
        n_if.run(&op, exec_id).await;
        let n_zero = op.get_next_node().await.unwrap().0;
        n_zero.run(&op, exec_id).await;
        let n_sub = op.get_next_node().await.unwrap().0;
        n_sub.run(&op, exec_id).await;
        let n_rec = op.get_next_node().await.unwrap().0;
        n_rec.run(&op, exec_id).await;
        let n_add = op.get_next_node().await.unwrap().0;
        n_add.run(&op, exec_id).await;
        let n_select = op.get_next_node().await.unwrap().0;
        n_select.run(&op, exec_id).await;
        let result: i32 = op
            .get_container(edge_select, exec_id)
            .await
            .unwrap()
            .take()
            .unwrap();
        assert_eq!(result, 55);
    }
}
