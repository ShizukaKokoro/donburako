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

#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("Type mismatch")]
    TypeMismatch,
}

#[derive(Default, Debug)]
pub struct Registry {
    data: HashMap<EdgeId, Box<dyn Any + 'static + Send + Sync>>,
    queue: VecDeque<usize>,
}
impl Registry {
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

    pub async fn check(&self, node: &dyn Node) -> bool {
        for input in node.inputs().await {
            if !self.data.contains_key(&input.id()) {
                return false;
            }
        }
        true
    }

    pub fn enqueue(&mut self, node_index: usize) {
        self.queue.push_back(node_index);
    }

    pub fn dequeue(&mut self) -> Option<usize> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::dummy::NodeDummy;
    use std::sync::Arc;

    #[test]
    fn test_store() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        assert_eq!(registry.data.len(), 1);
    }

    #[test]
    fn test_take() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        let data: i32 = registry.take(&edge).unwrap();
        assert_eq!(data, 42);
        assert_eq!(registry.data.len(), 0);
    }

    #[test]
    fn test_store_type_mismatch() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        let res = registry.store(&edge, "test");
        assert!(res.is_err());
    }

    #[test]
    fn test_take_type_mismatch() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        let res = registry.take::<String>(&edge);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_check() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        let mut node = NodeDummy::default();
        node.add_input(Arc::new(edge));
        let res = registry.check(&node).await;
        assert!(res);
    }

    #[test]
    fn test_queue() {
        let mut registry = Registry::default();
        registry.enqueue(0);
        registry.enqueue(1);
        assert_eq!(registry.dequeue(), Some(0));
        assert_eq!(registry.dequeue(), Some(1));
        assert_eq!(registry.dequeue(), None);
    }
}
