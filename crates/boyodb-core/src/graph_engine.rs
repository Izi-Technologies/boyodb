//! Graph Query Engine for BoyoDB
//!
//! Graph database capabilities:
//! - Node and edge storage
//! - Cypher-like query support
//! - Path finding algorithms (BFS, DFS, Dijkstra)
//! - Graph algorithms (PageRank, community detection)

use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Node in the graph
#[derive(Debug, Clone)]
pub struct Node {
    /// Unique node ID
    pub id: String,
    /// Node labels (types)
    pub labels: Vec<String>,
    /// Node properties
    pub properties: HashMap<String, PropertyValue>,
}

/// Edge in the graph
#[derive(Debug, Clone)]
pub struct Edge {
    /// Unique edge ID
    pub id: String,
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Edge type/label
    pub edge_type: String,
    /// Edge properties
    pub properties: HashMap<String, PropertyValue>,
    /// Weight (for weighted graphs)
    pub weight: f64,
}

/// Property value types
#[derive(Debug, Clone)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<PropertyValue>),
    Map(HashMap<String, PropertyValue>),
}

impl PropertyValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PropertyValue::Int(i) => Some(*i as f64),
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// Path result
#[derive(Debug, Clone)]
pub struct Path {
    /// Nodes in the path
    pub nodes: Vec<Node>,
    /// Edges in the path
    pub edges: Vec<Edge>,
    /// Total path length/cost
    pub total_weight: f64,
}

impl Path {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            total_weight: 0.0,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl Default for Path {
    fn default() -> Self {
        Self::new()
    }
}

/// Graph traversal direction
#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// Pattern matching for graph queries
#[derive(Debug, Clone)]
pub struct NodePattern {
    /// Variable name
    pub variable: Option<String>,
    /// Required labels
    pub labels: Vec<String>,
    /// Property filters
    pub properties: HashMap<String, PropertyValue>,
}

#[derive(Debug, Clone)]
pub struct EdgePattern {
    /// Variable name
    pub variable: Option<String>,
    /// Edge type filter
    pub edge_type: Option<String>,
    /// Direction
    pub direction: Direction,
    /// Min hops (for variable length)
    pub min_hops: usize,
    /// Max hops (for variable length)
    pub max_hops: usize,
    /// Property filters
    pub properties: HashMap<String, PropertyValue>,
}

/// Graph query
#[derive(Debug, Clone)]
pub struct GraphQuery {
    /// Match patterns
    pub patterns: Vec<(NodePattern, Option<EdgePattern>, Option<NodePattern>)>,
    /// Where conditions
    pub conditions: Vec<QueryCondition>,
    /// Return items
    pub returns: Vec<String>,
    /// Order by
    pub order_by: Option<(String, bool)>,
    /// Limit
    pub limit: Option<usize>,
}

/// Query condition
#[derive(Debug, Clone)]
pub enum QueryCondition {
    Equals(String, PropertyValue),
    NotEquals(String, PropertyValue),
    GreaterThan(String, f64),
    LessThan(String, f64),
    Contains(String, String),
    StartsWith(String, String),
    And(Box<QueryCondition>, Box<QueryCondition>),
    Or(Box<QueryCondition>, Box<QueryCondition>),
    Not(Box<QueryCondition>),
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<HashMap<String, ResultValue>>,
}

/// Result value
#[derive(Debug, Clone)]
pub enum ResultValue {
    Node(Node),
    Edge(Edge),
    Path(Path),
    Property(PropertyValue),
    List(Vec<ResultValue>),
}

/// Graph statistics
#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub label_counts: HashMap<String, usize>,
    pub edge_type_counts: HashMap<String, usize>,
    pub avg_degree: f64,
}

/// Priority queue entry for Dijkstra
#[derive(Debug, Clone)]
struct DijkstraEntry {
    node_id: String,
    distance: f64,
}

impl Eq for DijkstraEntry {}

impl PartialEq for DijkstraEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Graph database
pub struct GraphDatabase {
    /// Nodes by ID
    nodes: RwLock<HashMap<String, Node>>,
    /// Edges by ID
    edges: RwLock<HashMap<String, Edge>>,
    /// Adjacency list (outgoing)
    outgoing: RwLock<HashMap<String, Vec<String>>>,
    /// Adjacency list (incoming)
    incoming: RwLock<HashMap<String, Vec<String>>>,
    /// Label index
    label_index: RwLock<HashMap<String, HashSet<String>>>,
    /// Edge type index
    edge_type_index: RwLock<HashMap<String, HashSet<String>>>,
}

impl GraphDatabase {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            edges: RwLock::new(HashMap::new()),
            outgoing: RwLock::new(HashMap::new()),
            incoming: RwLock::new(HashMap::new()),
            label_index: RwLock::new(HashMap::new()),
            edge_type_index: RwLock::new(HashMap::new()),
        }
    }

    /// Create a node
    pub fn create_node(
        &self,
        id: &str,
        labels: Vec<String>,
        properties: HashMap<String, PropertyValue>,
    ) -> Node {
        let node = Node {
            id: id.to_string(),
            labels: labels.clone(),
            properties,
        };

        self.nodes.write().insert(id.to_string(), node.clone());

        // Update label index
        let mut label_index = self.label_index.write();
        for label in &labels {
            label_index
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(id.to_string());
        }

        // Initialize adjacency lists
        self.outgoing.write().entry(id.to_string()).or_default();
        self.incoming.write().entry(id.to_string()).or_default();

        node
    }

    /// Create an edge
    pub fn create_edge(
        &self,
        id: &str,
        source: &str,
        target: &str,
        edge_type: &str,
        properties: HashMap<String, PropertyValue>,
        weight: f64,
    ) -> Option<Edge> {
        // Verify nodes exist
        let nodes = self.nodes.read();
        if !nodes.contains_key(source) || !nodes.contains_key(target) {
            return None;
        }
        drop(nodes);

        let edge = Edge {
            id: id.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            edge_type: edge_type.to_string(),
            properties,
            weight,
        };

        self.edges.write().insert(id.to_string(), edge.clone());

        // Update adjacency lists
        self.outgoing
            .write()
            .entry(source.to_string())
            .or_default()
            .push(id.to_string());
        self.incoming
            .write()
            .entry(target.to_string())
            .or_default()
            .push(id.to_string());

        // Update edge type index
        self.edge_type_index
            .write()
            .entry(edge_type.to_string())
            .or_insert_with(HashSet::new)
            .insert(id.to_string());

        Some(edge)
    }

    /// Get node by ID
    pub fn get_node(&self, id: &str) -> Option<Node> {
        self.nodes.read().get(id).cloned()
    }

    /// Get edge by ID
    pub fn get_edge(&self, id: &str) -> Option<Edge> {
        self.edges.read().get(id).cloned()
    }

    /// Get neighbors of a node
    pub fn neighbors(&self, node_id: &str, direction: Direction) -> Vec<String> {
        let edges = self.edges.read();
        let outgoing = self.outgoing.read();
        let incoming = self.incoming.read();

        let mut neighbors = HashSet::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            if let Some(edge_ids) = outgoing.get(node_id) {
                for edge_id in edge_ids {
                    if let Some(edge) = edges.get(edge_id) {
                        neighbors.insert(edge.target.clone());
                    }
                }
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
            if let Some(edge_ids) = incoming.get(node_id) {
                for edge_id in edge_ids {
                    if let Some(edge) = edges.get(edge_id) {
                        neighbors.insert(edge.source.clone());
                    }
                }
            }
        }

        neighbors.into_iter().collect()
    }

    /// BFS traversal
    pub fn bfs(&self, start: &str, max_depth: usize, direction: Direction) -> Vec<(Node, usize)> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        if let Some(node) = self.get_node(start) {
            visited.insert(start.to_string());
            queue.push_back((node.clone(), 0));
            result.push((node, 0));
        }

        while let Some((_, depth)) = queue.front() {
            if *depth >= max_depth {
                break;
            }

            let (current, depth) = queue.pop_front().unwrap();

            for neighbor_id in self.neighbors(&current.id, direction) {
                if !visited.contains(&neighbor_id) {
                    visited.insert(neighbor_id.clone());
                    if let Some(neighbor) = self.get_node(&neighbor_id) {
                        queue.push_back((neighbor.clone(), depth + 1));
                        result.push((neighbor, depth + 1));
                    }
                }
            }
        }

        result
    }

    /// DFS traversal
    pub fn dfs(&self, start: &str, max_depth: usize, direction: Direction) -> Vec<(Node, usize)> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        self.dfs_recursive(start, 0, max_depth, direction, &mut visited, &mut result);

        result
    }

    fn dfs_recursive(
        &self,
        node_id: &str,
        depth: usize,
        max_depth: usize,
        direction: Direction,
        visited: &mut HashSet<String>,
        result: &mut Vec<(Node, usize)>,
    ) {
        if depth > max_depth || visited.contains(node_id) {
            return;
        }

        visited.insert(node_id.to_string());

        if let Some(node) = self.get_node(node_id) {
            result.push((node, depth));

            for neighbor_id in self.neighbors(node_id, direction) {
                self.dfs_recursive(
                    &neighbor_id,
                    depth + 1,
                    max_depth,
                    direction,
                    visited,
                    result,
                );
            }
        }
    }

    /// Dijkstra's shortest path
    pub fn shortest_path(&self, start: &str, end: &str) -> Option<Path> {
        let mut distances: HashMap<String, f64> = HashMap::new();
        let mut previous: HashMap<String, (String, String)> = HashMap::new(); // (prev_node, edge_id)
        let mut heap = BinaryHeap::new();

        distances.insert(start.to_string(), 0.0);
        heap.push(DijkstraEntry {
            node_id: start.to_string(),
            distance: 0.0,
        });

        let edges = self.edges.read();
        let outgoing = self.outgoing.read();

        while let Some(DijkstraEntry { node_id, distance }) = heap.pop() {
            if node_id == end {
                // Reconstruct path
                return Some(self.reconstruct_path(start, end, &previous));
            }

            if distance > *distances.get(&node_id).unwrap_or(&f64::INFINITY) {
                continue;
            }

            if let Some(edge_ids) = outgoing.get(&node_id) {
                for edge_id in edge_ids {
                    if let Some(edge) = edges.get(edge_id) {
                        let new_distance = distance + edge.weight;

                        if new_distance < *distances.get(&edge.target).unwrap_or(&f64::INFINITY) {
                            distances.insert(edge.target.clone(), new_distance);
                            previous
                                .insert(edge.target.clone(), (node_id.clone(), edge_id.clone()));
                            heap.push(DijkstraEntry {
                                node_id: edge.target.clone(),
                                distance: new_distance,
                            });
                        }
                    }
                }
            }
        }

        None
    }

    fn reconstruct_path(
        &self,
        start: &str,
        end: &str,
        previous: &HashMap<String, (String, String)>,
    ) -> Path {
        let mut path = Path::new();
        let mut current = end.to_string();

        while current != start {
            if let Some(node) = self.get_node(&current) {
                path.nodes.push(node);
            }

            if let Some((prev_node, edge_id)) = previous.get(&current) {
                if let Some(edge) = self.get_edge(edge_id) {
                    path.total_weight += edge.weight;
                    path.edges.push(edge);
                }
                current = prev_node.clone();
            } else {
                break;
            }
        }

        if let Some(node) = self.get_node(start) {
            path.nodes.push(node);
        }

        path.nodes.reverse();
        path.edges.reverse();
        path
    }

    /// Find all paths between two nodes
    pub fn all_paths(&self, start: &str, end: &str, max_depth: usize) -> Vec<Path> {
        let mut results = Vec::new();
        let mut current_path = Vec::new();
        let mut visited = HashSet::new();

        self.all_paths_recursive(
            start,
            end,
            max_depth,
            &mut current_path,
            &mut visited,
            &mut results,
        );

        results
    }

    fn all_paths_recursive(
        &self,
        current: &str,
        end: &str,
        remaining_depth: usize,
        current_path: &mut Vec<(String, Option<String>)>,
        visited: &mut HashSet<String>,
        results: &mut Vec<Path>,
    ) {
        if remaining_depth == 0 {
            return;
        }

        visited.insert(current.to_string());
        current_path.push((current.to_string(), None));

        if current == end {
            let mut path = Path::new();
            for (node_id, edge_id) in current_path.iter() {
                if let Some(node) = self.get_node(node_id) {
                    path.nodes.push(node);
                }
                if let Some(eid) = edge_id {
                    if let Some(edge) = self.get_edge(eid) {
                        path.total_weight += edge.weight;
                        path.edges.push(edge);
                    }
                }
            }
            results.push(path);
        } else {
            // Collect edges to explore while holding locks, then release
            let edges_to_explore: Vec<(String, String)> = {
                let edges = self.edges.read();
                let outgoing = self.outgoing.read();

                outgoing
                    .get(current)
                    .map(|edge_ids| {
                        edge_ids
                            .iter()
                            .filter_map(|edge_id| {
                                edges.get(edge_id).and_then(|edge| {
                                    if !visited.contains(&edge.target) {
                                        Some((edge_id.clone(), edge.target.clone()))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };

            for (edge_id, target) in edges_to_explore {
                current_path.last_mut().unwrap().1 = Some(edge_id);

                self.all_paths_recursive(
                    &target,
                    end,
                    remaining_depth - 1,
                    current_path,
                    visited,
                    results,
                );
            }
        }

        visited.remove(current);
        current_path.pop();
    }

    /// PageRank algorithm
    pub fn pagerank(&self, damping: f64, iterations: usize) -> HashMap<String, f64> {
        let nodes = self.nodes.read();
        let n = nodes.len() as f64;

        if n == 0.0 {
            return HashMap::new();
        }

        let mut ranks: HashMap<String, f64> = nodes.keys().map(|k| (k.clone(), 1.0 / n)).collect();

        for _ in 0..iterations {
            let mut new_ranks: HashMap<String, f64> = HashMap::new();

            for node_id in nodes.keys() {
                let incoming_nodes = self.neighbors(node_id, Direction::Incoming);
                let mut sum = 0.0;

                for incoming in &incoming_nodes {
                    let outgoing_count = self.neighbors(incoming, Direction::Outgoing).len() as f64;
                    if outgoing_count > 0.0 {
                        sum += ranks.get(incoming).unwrap_or(&0.0) / outgoing_count;
                    }
                }

                let rank = (1.0 - damping) / n + damping * sum;
                new_ranks.insert(node_id.clone(), rank);
            }

            ranks = new_ranks;
        }

        ranks
    }

    /// Community detection using label propagation
    pub fn detect_communities(&self, max_iterations: usize) -> HashMap<String, usize> {
        let nodes = self.nodes.read();

        // Initialize: each node in its own community
        let mut labels: HashMap<String, usize> = nodes
            .keys()
            .enumerate()
            .map(|(i, k)| (k.clone(), i))
            .collect();

        for _ in 0..max_iterations {
            let mut changed = false;

            for node_id in nodes.keys() {
                let neighbors = self.neighbors(node_id, Direction::Both);

                if neighbors.is_empty() {
                    continue;
                }

                // Count neighbor labels
                let mut label_counts: HashMap<usize, usize> = HashMap::new();
                for neighbor in &neighbors {
                    if let Some(&label) = labels.get(neighbor) {
                        *label_counts.entry(label).or_insert(0) += 1;
                    }
                }

                // Find most common label
                if let Some((&most_common, _)) = label_counts.iter().max_by_key(|(_, &count)| count)
                {
                    if labels.get(node_id) != Some(&most_common) {
                        labels.insert(node_id.clone(), most_common);
                        changed = true;
                    }
                }
            }

            if !changed {
                break;
            }
        }

        labels
    }

    /// Get graph statistics
    pub fn stats(&self) -> GraphStats {
        let nodes = self.nodes.read();
        let edges = self.edges.read();

        let mut label_counts: HashMap<String, usize> = HashMap::new();
        for node in nodes.values() {
            for label in &node.labels {
                *label_counts.entry(label.clone()).or_insert(0) += 1;
            }
        }

        let mut edge_type_counts: HashMap<String, usize> = HashMap::new();
        for edge in edges.values() {
            *edge_type_counts.entry(edge.edge_type.clone()).or_insert(0) += 1;
        }

        let avg_degree = if !nodes.is_empty() {
            (edges.len() * 2) as f64 / nodes.len() as f64
        } else {
            0.0
        };

        GraphStats {
            node_count: nodes.len(),
            edge_count: edges.len(),
            label_counts,
            edge_type_counts,
            avg_degree,
        }
    }
}

impl Default for GraphDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> GraphDatabase {
        let db = GraphDatabase::new();

        // Create nodes
        db.create_node("a", vec!["Person".to_string()], HashMap::new());
        db.create_node("b", vec!["Person".to_string()], HashMap::new());
        db.create_node("c", vec!["Person".to_string()], HashMap::new());
        db.create_node("d", vec!["Person".to_string()], HashMap::new());

        // Create edges
        db.create_edge("e1", "a", "b", "KNOWS", HashMap::new(), 1.0);
        db.create_edge("e2", "b", "c", "KNOWS", HashMap::new(), 2.0);
        db.create_edge("e3", "a", "c", "KNOWS", HashMap::new(), 5.0);
        db.create_edge("e4", "c", "d", "KNOWS", HashMap::new(), 1.0);

        db
    }

    #[test]
    fn test_create_node() {
        let db = GraphDatabase::new();
        let node = db.create_node("1", vec!["Person".to_string()], HashMap::new());
        assert_eq!(node.id, "1");
        assert!(node.labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_create_edge() {
        let db = create_test_graph();
        let edge = db.get_edge("e1").unwrap();
        assert_eq!(edge.source, "a");
        assert_eq!(edge.target, "b");
    }

    #[test]
    fn test_neighbors() {
        let db = create_test_graph();
        let neighbors = db.neighbors("a", Direction::Outgoing);
        assert!(neighbors.contains(&"b".to_string()));
        assert!(neighbors.contains(&"c".to_string()));
    }

    #[test]
    fn test_bfs() {
        let db = create_test_graph();
        let result = db.bfs("a", 2, Direction::Outgoing);
        assert!(!result.is_empty());
        assert_eq!(result[0].0.id, "a");
    }

    #[test]
    fn test_shortest_path() {
        let db = create_test_graph();
        let path = db.shortest_path("a", "d").unwrap();

        // Shortest path should be a -> b -> c -> d (cost 4) not a -> c -> d (cost 6)
        assert_eq!(path.nodes.len(), 4);
        assert!((path.total_weight - 4.0).abs() < 0.001);
    }

    #[test]
    fn test_pagerank() {
        let db = create_test_graph();
        let ranks = db.pagerank(0.85, 20);

        // All nodes should have some rank
        assert_eq!(ranks.len(), 4);
        for rank in ranks.values() {
            assert!(*rank > 0.0);
        }
    }

    #[test]
    fn test_communities() {
        let db = create_test_graph();
        let communities = db.detect_communities(10);
        assert_eq!(communities.len(), 4);
    }

    #[test]
    fn test_stats() {
        let db = create_test_graph();
        let stats = db.stats();
        assert_eq!(stats.node_count, 4);
        assert_eq!(stats.edge_count, 4);
    }
}
