//! コンテナモジュール
//!
//! データを運ぶためのコンテナを提供する。
//! 中に入っているデータの型は、決まっておらず、任意の型を格納し、任意の型を取り出すことができる。
//! ただし、取り出すデータの型は入れたデータの型と一致している必要がある。

use tokio_util::sync::CancellationToken;

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
}
