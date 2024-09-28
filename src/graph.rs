//! グラフモジュール
//!
//! 復路を許す非巡回有向グラフを表す構造体を提供する。

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Invalid path")]
    InvalidPath,
    #[error("Multiple start nodes")]
    MultipleStartNodes,
    #[error("Multiple end nodes")]
    MultipleEndNodes,
}

#[derive(Debug)]
pub struct Graph(Vec<Vec<usize>>);
impl Graph {
    pub fn new(size: usize) -> Self {
        Graph(vec![vec![]; size])
    }

    pub fn add_edge(&mut self, from: usize, to: usize) -> Result<(), GraphError> {
        if self.check_valid_path(from, to) {
            if !self.0[from].contains(&to) {
                self.0[from].push(to);
            }
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

    pub fn get_start(&self) -> Result<usize, GraphError> {
        let mut is_start = vec![true; self.0.len()];
        for edges in &self.0 {
            for &edge in edges {
                is_start[edge] = false;
            }
        }
        let mut start = None;
        for (i, flag) in is_start.iter().enumerate() {
            if *flag {
                if start.is_none() {
                    start = Some(i);
                } else {
                    return Err(GraphError::MultipleStartNodes);
                }
            }
        }
        Ok(start.unwrap())
    }

    pub fn get_end(&self) -> Result<usize, GraphError> {
        let mut is_end = vec![false; self.0.len()];
        for (i, edges) in self.0.iter().enumerate() {
            if edges.is_empty() {
                is_end[i] = true;
            }
        }
        let mut end = None;
        for (i, flag) in is_end.iter().enumerate() {
            if *flag {
                if end.is_none() {
                    end = Some(i);
                } else {
                    return Err(GraphError::MultipleEndNodes);
                }
            }
        }
        Ok(end.unwrap())
    }

    pub fn check_start_end(&self) -> Result<(), GraphError> {
        if !self.check_valid_path(self.get_start()?, self.get_end()?) {
            Err(GraphError::InvalidPath)
        } else {
            Ok(())
        }
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
        assert_eq!(graph.get_start().unwrap(), 0);
    }

    #[test]
    fn test_get_end() {
        let mut graph = Graph::new(3);
        graph.add_edge(0, 1).unwrap();
        graph.add_edge(1, 2).unwrap();
        assert_eq!(graph.get_end().unwrap(), 2);
    }

    #[test]
    fn test_check_start_end() {
        let mut graph = Graph::new(3);
        graph.add_edge(0, 1).unwrap();
        graph.add_edge(1, 2).unwrap();
        assert!(graph.check_start_end().is_ok());
    }
}
