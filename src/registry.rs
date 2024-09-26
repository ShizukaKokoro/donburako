//! レジストリモジュール
//!
//! レジストリは、ワークフローの作業領域。
//! ワークフロー実行中に、データの読み書きを行う。
//! レジストリによって、同一のワークフローを同時に複数実行することができる。

use crate::edge::{Edge, EdgeId};
use crate::node::Node;
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("Type mismatch")]
    TypeMismatch,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct RegistryID(Uuid);
impl RegistryID {
    pub fn new() -> Self {
        RegistryID(Uuid::new_v4())
    }
}

#[derive(Debug)]
pub struct Registry {
    data: HashMap<EdgeId, Box<dyn Any + 'static + Send + Sync>>,
    queue: VecDeque<usize>,
}
impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
            queue: VecDeque::new(),
        }
    }

    pub fn store<T: 'static + Send + Sync>(
        &mut self,
        edge: &Edge,
        data: T,
    ) -> Result<(), RegistryError> {
        if !edge.check_type::<T>() {
            return Err(RegistryError::TypeMismatch);
        }
        self.data.insert(edge.id(), Box::new(data));
        Ok(())
    }

    pub fn take<T: 'static + Send + Sync>(&mut self, edge: &Edge) -> Result<T, RegistryError> {
        if !edge.check_type::<T>() {
            return Err(RegistryError::TypeMismatch);
        }
        let data = self.data.remove(&edge.id()).unwrap();
        Ok(*data.downcast().unwrap())
    }

    pub(crate) fn check(&self, node: &Node) -> bool {
        for input in node.inputs() {
            if !self.data.contains_key(&input.id()) {
                return false;
            }
        }
        true
    }

    pub(crate) fn enqueue(&mut self, node_index: usize) {
        self.queue.push_back(node_index);
    }

    pub(crate) fn dequeue(&mut self) -> Option<usize> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::dummy::DummyNodeBuilder;
    use std::sync::Arc;

    #[test]
    fn test_store() {
        let mut registry = Registry::new();
        let edge = Arc::new(Edge::new::<i32>());
        registry.store(&edge, 42).unwrap();
        assert_eq!(registry.data.len(), 1);
    }

    #[test]
    fn test_take() {
        let mut registry = Registry::new();
        let edge = Arc::new(Edge::new::<i32>());
        registry.store(&edge, 42).unwrap();
        let data: i32 = registry.take(&edge).unwrap();
        assert_eq!(data, 42);
        assert_eq!(registry.data.len(), 0);
    }

    #[test]
    fn test_store_type_mismatch() {
        let mut registry = Registry::new();
        let edge = Arc::new(Edge::new::<i32>());
        registry.store(&edge, 42).unwrap();
        let res = registry.store(&edge, "test");
        assert!(res.is_err());
    }

    #[test]
    fn test_take_type_mismatch() {
        let mut registry = Registry::new();
        let edge = Arc::new(Edge::new::<i32>());
        registry.store(&edge, 42).unwrap();
        let res = registry.take::<String>(&edge);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_check() {
        let mut registry = Registry::new();
        let edge = Arc::new(Edge::new::<i32>());
        registry.store(&edge, 42).unwrap();
        let node = {
            let mut builder = DummyNodeBuilder::new();
            builder.add_input(edge);
            builder.build()
        };
        let res = registry.check(&node);
        assert!(res);
    }

    #[test]
    fn test_queue() {
        let mut registry = Registry::new();
        registry.enqueue(0);
        registry.enqueue(1);
        assert_eq!(registry.dequeue(), Some(0));
        assert_eq!(registry.dequeue(), Some(1));
        assert_eq!(registry.dequeue(), None);
    }
}
