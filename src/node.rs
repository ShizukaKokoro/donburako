//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! ノード同士の繋がりはエッジによって表される。

use std::any::TypeId;
use std::rc::Rc;
use uuid::Uuid;

/// ノードID
#[derive(Default, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(Uuid);
impl NodeId {
    /// ノードIDの生成
    pub fn new() -> Self {
        NodeId(Uuid::new_v4())
    }
}

/// ノード
///
/// NOTE: サイズが大きめ？
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    kind: NodeType,
}
impl Node {
    /// ノードの生成
    pub fn new(kind: NodeType) -> Self {
        Node {
            id: NodeId::new(),
            kind,
        }
    }

    /// ノードの実行
    ///
    /// ノードの種類に応じた処理を実行する。
    ///
    /// # Arguments
    ///
    /// * `container_map` - コンテナマップ
    pub async fn run(&self, container_map: &ContainerMap) {
        match &self.kind {
            NodeType::User(node) => {
                let func = &node.func;
                func(node, container_map).await;
            }
        }
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Rc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.inputs(),
        }
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Vec<Rc<Edge>> {
        match &self.kind {
            NodeType::User(node) => node.outputs(),
        }
    }

    /// ノードの種類の取得
    pub fn kind(&self) -> &NodeType {
        &self.kind
    }
}
impl std::cmp::PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// ノードの種類
#[derive(Debug)]
pub enum NodeType {
    /// ユーザー定義ノード
    User(UserNode),
}

/// ユーザー定義ノード
///
/// ユーザーが定義した任意の処理を実行するノード。
/// その処理を一つの関数だとした時に、その関数が受け取る引数の数だけ入力エッジが必要になる。
/// また、その関数が返す値の数だけ出力エッジが必要になる。
/// 全ての入力エッジにデータが来るまで、ノードは実行されない。
#[derive(Debug, Default, PartialEq)]
pub struct UserNode {
    inputs: Vec<Rc<Edge>>,
    outputs: Vec<Rc<Edge>>,
}
impl UserNode {
    /// ノードの生成
    pub fn new(inputs: Vec<Rc<Edge>>) -> Self {
        UserNode {
            inputs,
            outputs: Vec::new(),
        }
    }

    /// 出力エッジの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Rc<Edge> {
        let edge = Rc::new(Edge::new::<T>());
        self.outputs.push(edge.clone());
        edge
    }

    /// 入力エッジの取得
    pub fn inputs(&self) -> &Vec<Rc<Edge>> {
        &self.inputs
    }

    /// 出力エッジの取得
    pub fn outputs(&self) -> &Vec<Rc<Edge>> {
        &self.outputs
    }
}

/// エッジ
///
/// ノード間を繋ぐデータの流れを表す。
/// エッジはデータの型を持ち、その型が異なるコンテナは通ることができない。
/// エッジは一意な ID を持ち、他のエッジと区別するために使われる。
///
/// ノードはエッジへの参照を持つが、エッジはノードへの参照を持たない。
/// エッジからノードへのマッピングは、ワークフローが行う。
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    ty: TypeId,
    id: Uuid,
}
impl Edge {
    /// エッジの生成
    pub fn new<T: 'static + Send + Sync>() -> Self {
        Edge {
            ty: TypeId::of::<T>(),
            id: Uuid::new_v4(),
        }
    }

    /// 型のチェック
    pub fn check_type<T: 'static + Send + Sync>(&self) -> bool {
        self.ty == TypeId::of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_nodes() {
        let mut node1 = UserNode::default();
        let edge = node1.add_output::<i32>();
        assert_eq!(node1.outputs.len(), 1);
        let node2 = UserNode::new(vec![edge.clone()]);
        assert_eq!(node2.inputs.len(), 1);
        assert_eq!(edge.ty, TypeId::of::<i32>());
        assert_eq!(edge, node2.inputs[0]);
    }
}
