//! ノードモジュール
//!
//! このエンジンが扱うタスクの基本単位を表すノードを定義する。
//! 出入り口が複数あり、それぞれの出入り口にはデータが流れる。
//! ノードはそれぞれ、どのノードのどの出口からデータを受け取り、どのノードのどの入口にデータを送るかを保持している。
//! ノードは実行時に、コンテナが保存されている構造体を受け取り、その中のデータを読み書きすることになる。
//! また、ノードは自身のポートを持ち、このポートが他のポートと繋がることによってデータの流れを形成する。
//! ポートとポートを繋いでいく。

use std::any::TypeId;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

/// ノード
pub enum Node {
    /// ユーザー定義ノード
    User(UserNode),
}

/// ユーザー定義ノード
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
    pub fn add_output<T: 'static + Send + Sync>(&mut self) -> Rc<Connection> {
        let input = Rc::new(InputPort::new());
        let con = Rc::new(Connection::new::<T>(input.clone()));
        input.set_from(Rc::downgrade(&con));
        let port = Rc::new(OutputPort::new(con.clone()));
        self.outputs.push(port.clone());
        con
    }
}

/// 出力ポート
#[derive(Debug, PartialEq)]
pub struct OutputPort {
    to: Rc<Connection>,
}
impl OutputPort {
    /// 出力ポートの生成
    fn new(to: Rc<Connection>) -> Self {
        OutputPort { to }
    }
}

/// 接続
#[derive(Debug, PartialEq)]
pub struct Connection {
    ty: TypeId,
    to: Rc<InputPort>,
}
impl Connection {
    /// 接続の生成
    fn new<T: 'static + Send + Sync>(to: Rc<InputPort>) -> Self {
        Connection {
            ty: TypeId::of::<T>(),
            to,
        }
    }

    /// 入力ポートの取得
    pub fn to(&self) -> Rc<InputPort> {
        self.to.clone()
    }
}

/// 入力ポート
#[derive(Debug)]
pub struct InputPort {
    from: RefCell<Option<Weak<Connection>>>,
}
impl InputPort {
    /// 入力ポートの生成
    fn new() -> Self {
        InputPort {
            from: RefCell::new(None),
        }
    }

    fn set_from(&self, from: Weak<Connection>) {
        *self.from.borrow_mut() = Some(from);
    }
}
impl PartialEq for InputPort {
    fn eq(&self, other: &Self) -> bool {
        let s_from = self.from.borrow();
        let o_from = other.from.borrow();
        if let Some(s_from) = &*s_from {
            if let Some(o_from) = &*o_from {
                Rc::ptr_eq(&s_from.upgrade().unwrap(), &o_from.upgrade().unwrap())
            } else {
                false
            }
        } else {
            o_from.is_none()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_nodes() {
        let mut node1 = UserNode::default();
        let con = node1.add_output::<i32>();
        let node2 = UserNode::new(vec![con.to()]);
        assert_eq!(node1.outputs.len(), 1);
        let op = node1.outputs[0].clone();
        assert_eq!(op.to, con);
        let con = op.to.clone();
        assert_eq!(con.ty, TypeId::of::<i32>());
        let ip = con.to.clone();
        assert_eq!(node2.inputs.len(), 1);
        assert_eq!(ip, node2.inputs[0]);
        let con = ip.from.borrow().as_ref().unwrap().upgrade().unwrap();
        assert_eq!(ip, con.to());
    }
}
