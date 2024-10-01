//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! また、ノードは自身のポートを持ち、このポートが他のポートと繋がることによってデータの流れを形成する。
//! ポートとポートを繋いでいく。

use std::any::TypeId;
use std::rc::Rc;
use uuid::Uuid;

/// ノード
#[derive(Debug, PartialEq)]
pub enum Node {
    /// ユーザー定義ノード
    User(UserNode),
}
impl Node {
    /// 入力ポートの取得
    pub fn inputs(&self) -> &Vec<Rc<InputPort>> {
        match self {
            Node::User(node) => &node.inputs,
        }
    }

    /// 出力ポートの取得
    pub fn outputs(&self) -> &Vec<Rc<OutputPort>> {
        match self {
            Node::User(node) => &node.outputs,
        }
    }
}

/// ユーザー定義ノード
///
/// ユーザーが定義した任意の処理を実行するノード。
/// その処理を一つの関数だとした時に、その関数が受け取る引数の数だけ入力ポートが必要になる。
/// また、その関数が返す値の数だけ出力ポートが必要になる。
/// 全ての入力ポートにデータが来るまで、ノードは実行されない。
#[derive(Debug, Default, PartialEq)]
pub struct UserNode {
    inputs: Vec<Rc<InputPort>>,
    outputs: Vec<Rc<OutputPort>>,
}
impl UserNode {
    /// ノードの生成
    pub fn new(inputs: Vec<Rc<InputPort>>) -> Self {
        UserNode {
            inputs,
            outputs: Vec::new(),
        }
    }

    /// 出力ポートの追加
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Rc<InputPort> {
        let ip = Rc::new(InputPort::new::<T>());
        let op = Rc::new(OutputPort::new(ip.clone()));
        self.outputs.push(op.clone());
        ip
    }
}

/// 出力ポート
///
/// 他のポートにデータを送るポート。
#[derive(Debug, PartialEq)]
pub struct OutputPort {
    to: Rc<InputPort>,
}
impl OutputPort {
    /// 出力ポートの生成
    fn new(to: Rc<InputPort>) -> Self {
        OutputPort { to }
    }

    /// この出力ポートがどの入力ポートに接続されているかを取得
    pub fn input(&self) -> Rc<InputPort> {
        self.to.clone()
    }
}

/// 入力ポート
///
/// 他のポートからデータを受け取るポート。
/// ワークフローはそれぞれの入力ポートがどのノードのポートかを知っている必要がある。
/// 出力ポートからのデータを受け取るために、出力ポートを参照している。
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InputPort {
    ty: TypeId,
    id: Uuid,
}
impl InputPort {
    /// 入力ポートの生成
    pub fn new<T: 'static + Send + Sync>() -> Self {
        InputPort {
            ty: TypeId::of::<T>(),
            id: Uuid::new_v4(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_nodes() {
        let mut node1 = UserNode::default();
        let ip = node1.add_output::<i32>();
        assert_eq!(node1.outputs.len(), 1);
        let op = node1.outputs[0].clone();
        assert_eq!(op.to, ip);
        let node2 = UserNode::new(vec![ip.clone()]);
        let ip = op.to.clone();
        assert_eq!(node2.inputs.len(), 1);
        assert_eq!(ip.ty, TypeId::of::<i32>());
        assert_eq!(ip, node2.inputs[0]);
    }
}
