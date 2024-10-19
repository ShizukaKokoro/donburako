//! エッジモジュール

use std::any::TypeId;
use uuid::Uuid;

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
    pub(super) ty: TypeId,
    id: Uuid,
}
impl Edge {
    /// エッジの生成
    pub fn new<T: 'static + Send + Sync>(
        #[cfg(all(feature = "serialize", not(test)))] id: Uuid,
    ) -> Self {
        Edge {
            ty: TypeId::of::<T>(),
            #[cfg(all(feature = "serialize", not(test)))]
            id,
            #[cfg(any(not(feature = "serialize"), test))]
            id: Uuid::new_v4(),
        }
    }

    /// 型のチェック
    pub fn check_type<T: 'static + Send + Sync>(&self) -> bool {
        self.ty == TypeId::of::<T>()
    }
}
