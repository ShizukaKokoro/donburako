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
