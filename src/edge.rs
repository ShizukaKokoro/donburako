//! エッジモジュール
//!
//! グラフのエッジを表す構造体を定義する。

use std::any::TypeId;
use uuid::Uuid;

/// エッジID
///
/// エッジの識別子。
/// レジストリにエッジを登録する際に使用される。
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct EdgeId {
    id: Uuid,
}
impl EdgeId {
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}
impl Default for EdgeId {
    fn default() -> Self {
        Self::new()
    }
}

/// エッジ
///
/// ワークフローのエッジ。
/// ワークフローの各ノード間を結ぶ。
/// ノードをどのように繋ぐかは [Workflow] が保持する。
pub struct Edge {
    ty: TypeId,
    id: EdgeId,
}
impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartEdge").field("id", &self.id).finish()
    }
}
impl Edge {
    pub fn new<T: 'static + Send + Sync>() -> Self {
        Edge {
            ty: TypeId::of::<T>(),
            id: EdgeId::default(),
        }
    }

    pub fn check_type<T: 'static + Send + Sync>(&self) -> bool {
        self.ty == TypeId::of::<T>()
    }

    pub fn id(&self) -> EdgeId {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_id() {
        let id1 = EdgeId::new();
        let id2 = EdgeId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_edge() {
        let edge1 = Edge::new::<i32>();
        let edge2 = Edge::new::<i32>();
        let edge3 = Edge::new::<f64>();
        assert!(edge1.check_type::<i32>());
        assert!(!edge1.check_type::<f64>());
        assert_ne!(edge1.id(), edge2.id());
        assert_ne!(edge1.id(), edge3.id());
    }
}
