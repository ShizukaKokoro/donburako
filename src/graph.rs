//! グラフモジュール
//!
//! 復路を許す非巡回有向グラフを表す構造体を提供する。

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Invalid path")]
    InvalidPath,
}

#[derive(Debug)]
pub struct Graph(Vec<Vec<usize>>);
impl Graph {
    pub fn new(size: usize) -> Self {
        Graph(vec![vec![]; size])
    }

    pub fn add_edge(&mut self, from: usize, to: usize) -> Result<(), GraphError> {
        if self.check_valid_path(from, to) {
            self.0[from].push(to);
            Ok(())
        } else {
            Err(GraphError::InvalidPath)
        }
    }

    /// パスへの追加が妥当かどうかを判定する。
    ///
    /// from から to へのパスを追加する時、 to から from へのパスが存在しないことを確認する。
    /// もし、 to から from へのパスが存在する場合、巡回グラフになるため、追加できない。
    fn check_valid_path(&self, from: usize, to: usize) -> bool {
        let mut visited = vec![false; self.0.len()];
        !self.dfs(to, from, &mut visited)
    }

    fn dfs(&self, from: usize, to: usize, visited: &mut Vec<bool>) -> bool {
        if from == to {
            return true;
        }
        visited[from] = true;
        for &next in &self.0[from] {
            if !visited[next] && self.dfs(next, to, visited) {
                return true;
            }
        }
        false
    }

    pub fn get_start(&self) -> Vec<usize> {
        let mut start = vec![true; self.0.len()];
        for edges in &self.0 {
            for &edge in edges {
                start[edge] = false;
            }
        }
        start
            .iter()
            .enumerate()
            .filter_map(|(index, &is_start)| if is_start { Some(index) } else { None })
            .collect()
    }

    pub fn children(&self, from: usize) -> &Vec<usize> {
        &self.0[from]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_edge() {
        let mut graph = Graph::new(3);
        assert!(graph.add_edge(0, 1).is_ok());
        assert!(graph.add_edge(1, 2).is_ok());
        assert!(graph.add_edge(2, 0).is_err());
    }

    #[test]
    fn test_get_start() {
        let mut graph = Graph::new(3);
        graph.add_edge(0, 1).unwrap();
        graph.add_edge(1, 2).unwrap();
        assert_eq!(graph.get_start(), vec![0]);
    }
}
