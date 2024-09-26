//! レジストリモジュール
//!
//! レジストリは、ワークフローの作業領域。
//! ワークフロー実行中に、データの読み書きを行う。
//! レジストリによって、同一のワークフローを同時に複数実行することができる。

use crate::edge::{Edge, EdgeId};
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

    /*
    pub async fn check(&self, node: &dyn Task) -> bool {
        for input in node.inputs().await {
            if !self.data.contains_key(&input.edge_id) {
                return false;
            }
        }
        true
    } */

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
    #[should_panic]
    fn test_store_panic() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        registry.store(&edge, "test").unwrap();
    }

    #[test]
    #[should_panic]
    fn test_take_panic() {
        let mut registry = Registry::default();
        let edge = Edge::new::<i32>();
        registry.store(&edge, 42).unwrap();
        let _: String = registry.take(&edge).unwrap();
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
