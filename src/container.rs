//! コンテナモジュール
//!
//! データを運ぶためのコンテナを提供する。
//! 中に入っているデータの型は、決まっておらず、任意の型を格納し、任意の型を取り出すことができる。
//! ただし、取り出すデータの型は入れたデータの型と一致している必要がある。

use std::any::{Any, TypeId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// コンテナエラー
#[derive(Debug, Error, PartialEq)]
pub enum ContainerError {
    /// コンテナ内の型の不一致
    #[error("Type is mismatched with the stored data")]
    TypeMismatch,

    /// コンテナ内にデータが存在しない
    #[error("Data is not found in the container")]
    DataNotFound,
}

/// キャンセルスタック
#[derive(Default, Clone)]
pub struct CancelStack(Vec<CancellationToken>);
impl CancelStack {
    /// キャンセルトークンを追加する
    ///
    /// # Arguments
    ///
    /// * `token` - キャンセルトークン
    pub fn push(&mut self, token: CancellationToken) {
        self.0.push(token);
    }

    /// キャンセルを確認する
    pub fn check(&mut self) -> bool {
        if let Some(token) = self.0.last() {
            if token.is_cancelled() {
                let _ = self.0.pop();
                return true;
            }
        }
        false
    }

    /// キャンセル
    pub fn cancel(&mut self) {
        let token = self.0.pop();
        if let Some(token) = token {
            token.cancel();
        }
    }
}
impl std::fmt::Debug for CancelStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelStack")
            .field("len", &self.0.len())
            .finish()
    }
}

/// コンテナ
///
/// データを格納するためのコンテナ。
#[derive(Default)]
pub struct Container {
    data: Option<Box<dyn Any + 'static + Send + Sync>>,
    ty: Option<TypeId>,
}
impl Container {
    /// データを格納する
    ///
    /// # Arguments
    ///
    /// * `data` - 格納するデータ
    pub fn store<T: 'static + Send + Sync>(&mut self, data: T) {
        self.data = Some(Box::new(data));
        self.ty = Some(TypeId::of::<T>());
    }

    /// データを取り出す
    ///
    /// # Returns
    ///
    /// 格納されているデータ
    pub fn take<T: 'static + Send + Sync>(&mut self) -> Result<T, ContainerError> {
        if let Some(data) = self.data.take() {
            if let Some(ty) = self.ty {
                if ty == TypeId::of::<T>() {
                    if let Ok(data) = data.downcast::<T>() {
                        return Ok(*data);
                    }
                } else {
                    return Err(ContainerError::TypeMismatch);
                }
            }
        }
        Err(ContainerError::DataNotFound)
    }
impl std::fmt::Debug for Container {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Container")
            .field("data", &self.data)
            .field("stack", &self.stack)
            .finish()
    }
}
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_cancel_stack_push() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());

        assert_eq!(stack.0.len(), 1);
    }

    #[test]
    fn test_cancel_stack_check() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());

        assert!(!stack.check());
    }

    #[test]
    fn test_cancel_stack_check_canceled() {
        let mut stack = CancelStack::default();
        let token = CancellationToken::new();
        stack.push(token.clone());
        token.cancel();

        assert!(stack.check());
        assert_eq!(stack.0.len(), 0);
    }

    #[test]
    fn test_cancel_stack_cancel() {
        let mut stack = CancelStack::default();
        stack.push(CancellationToken::new());
        stack.cancel();

        assert_eq!(stack.0.len(), 0);
    }

    #[test]
    fn test_cancel_stack_cancel_empty() {
        let mut stack = CancelStack::default();
        stack.cancel();

        assert_eq!(stack.0.len(), 0);
    }

    #[test]
    fn test_container_init() {
        let container = Container::default();

        assert!(container.data.is_none());
        assert!(container.ty.is_none());
    }

    #[test]
    fn test_container_store() {
        let mut container = Container::default();
        container.store(42);

        assert!(container.data.is_some());
        assert!(container.ty.is_some());
    }

    #[test]
    fn test_container_take() {
        let mut container = Container::default();
        container.store(42);

        let data = container.take::<i32>().unwrap();
        assert_eq!(data, 42);
    }

    #[test]
    fn test_container_take_type_mismatch() {
        let mut container = Container::default();
        container.store(42);

        let result = container.take::<&str>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ContainerError::TypeMismatch);
    }

    #[test]
    fn test_container_take_data_not_found() {
        let mut container = Container::default();

        let result = container.take::<i32>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ContainerError::DataNotFound);
    }

    #[test]
    fn test_container_debug() {
        let mut container = Container::default();
        container.store(42);

        let debug = format!("{:?}", container);
        assert_eq!(
            debug,
            "Container { data: Some(Any { .. }), stack: CancelStack { len: 0 } }"
        );

        let container = Container::default();
        let debug = format!("{:?}", container);
        assert_eq!(
            debug,
            "Container { data: None, stack: CancelStack { len: 0 } }"
        );
    }
}
