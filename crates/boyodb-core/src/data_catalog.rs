//! Data Catalog for BoyoDB
//!
//! Metadata management and data discovery:
//! - Table and column metadata
//! - Data lineage tracking
//! - Search and discovery
//! - Business glossary

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Catalog entry types
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    Column,
    Index,
    Function,
    Procedure,
    Pipeline,
    Dashboard,
    Report,
}

/// Data classification levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Classification {
    Public,
    Internal,
    Confidential,
    Restricted,
    Pii,
}

/// Catalog entry
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// Unique identifier
    pub id: String,
    /// Entry type
    pub entry_type: EntryType,
    /// Full qualified name
    pub fqn: String,
    /// Display name
    pub name: String,
    /// Description
    pub description: String,
    /// Owner
    pub owner: String,
    /// Tags
    pub tags: Vec<String>,
    /// Classification
    pub classification: Classification,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Parent entry ID
    pub parent_id: Option<String>,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
    /// Last accessed timestamp
    pub last_accessed_at: Option<i64>,
    /// Popularity score
    pub popularity: f64,
}

/// Column catalog entry
#[derive(Debug, Clone)]
pub struct ColumnEntry {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Description
    pub description: String,
    /// Is nullable
    pub nullable: bool,
    /// Is primary key
    pub is_primary_key: bool,
    /// Is foreign key
    pub is_foreign_key: bool,
    /// Foreign key reference
    pub fk_reference: Option<(String, String)>,
    /// Classification
    pub classification: Classification,
    /// Tags
    pub tags: Vec<String>,
    /// Sample values
    pub sample_values: Vec<String>,
    /// Statistics
    pub stats: Option<ColumnStats>,
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: u64,
    pub null_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub avg_length: Option<f64>,
}

/// Lineage node
#[derive(Debug, Clone)]
pub struct LineageNode {
    /// Node ID (entry ID)
    pub id: String,
    /// Node type
    pub node_type: LineageNodeType,
    /// Name
    pub name: String,
    /// FQN
    pub fqn: String,
}

/// Lineage node types
#[derive(Debug, Clone, PartialEq)]
pub enum LineageNodeType {
    Source,
    Table,
    View,
    Transformation,
    Pipeline,
    Dashboard,
    External,
}

/// Lineage edge
#[derive(Debug, Clone)]
pub struct LineageEdge {
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Edge type
    pub edge_type: LineageEdgeType,
    /// Transformation description
    pub transformation: Option<String>,
    /// Columns involved
    pub columns: Vec<(String, String)>,
}

/// Lineage edge types
#[derive(Debug, Clone)]
pub enum LineageEdgeType {
    Direct,
    Aggregation,
    Join,
    Filter,
    Transformation,
    Copy,
}

/// Data lineage graph
#[derive(Debug, Clone, Default)]
pub struct LineageGraph {
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
}

impl LineageGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: LineageNode) {
        if !self.nodes.iter().any(|n| n.id == node.id) {
            self.nodes.push(node);
        }
    }

    pub fn add_edge(&mut self, edge: LineageEdge) {
        self.edges.push(edge);
    }

    pub fn upstream(&self, node_id: &str) -> Vec<&LineageNode> {
        let upstream_ids: HashSet<_> = self
            .edges
            .iter()
            .filter(|e| e.target == node_id)
            .map(|e| &e.source)
            .collect();

        self.nodes
            .iter()
            .filter(|n| upstream_ids.contains(&n.id))
            .collect()
    }

    pub fn downstream(&self, node_id: &str) -> Vec<&LineageNode> {
        let downstream_ids: HashSet<_> = self
            .edges
            .iter()
            .filter(|e| e.source == node_id)
            .map(|e| &e.target)
            .collect();

        self.nodes
            .iter()
            .filter(|n| downstream_ids.contains(&n.id))
            .collect()
    }
}

/// Business term definition
#[derive(Debug, Clone)]
pub struct GlossaryTerm {
    /// Term ID
    pub id: String,
    /// Term name
    pub name: String,
    /// Definition
    pub definition: String,
    /// Abbreviations
    pub abbreviations: Vec<String>,
    /// Related terms
    pub related_terms: Vec<String>,
    /// Associated catalog entries
    pub associated_entries: Vec<String>,
    /// Owner
    pub owner: String,
    /// Status
    pub status: TermStatus,
    /// Created timestamp
    pub created_at: i64,
}

/// Term status
#[derive(Debug, Clone, PartialEq)]
pub enum TermStatus {
    Draft,
    Approved,
    Deprecated,
}

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Entry
    pub entry: CatalogEntry,
    /// Relevance score
    pub score: f64,
    /// Matched fields
    pub matched_fields: Vec<String>,
    /// Snippet
    pub snippet: String,
}

/// Search query
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Search text
    pub text: String,
    /// Entry types to include
    pub entry_types: Option<Vec<EntryType>>,
    /// Tags to filter
    pub tags: Option<Vec<String>>,
    /// Owner filter
    pub owner: Option<String>,
    /// Classification filter
    pub classification: Option<Classification>,
    /// Limit results
    pub limit: usize,
    /// Offset for pagination
    pub offset: usize,
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            text: String::new(),
            entry_types: None,
            tags: None,
            owner: None,
            classification: None,
            limit: 20,
            offset: 0,
        }
    }
}

/// Data catalog
pub struct DataCatalog {
    /// All entries
    entries: RwLock<HashMap<String, CatalogEntry>>,
    /// Column entries by table ID
    columns: RwLock<HashMap<String, Vec<ColumnEntry>>>,
    /// Lineage graph
    lineage: RwLock<LineageGraph>,
    /// Business glossary
    glossary: RwLock<HashMap<String, GlossaryTerm>>,
    /// Tag index
    tag_index: RwLock<HashMap<String, HashSet<String>>>,
    /// Search index (word -> entry IDs)
    search_index: RwLock<HashMap<String, HashSet<String>>>,
    /// Entry counter
    entry_counter: RwLock<u64>,
}

impl DataCatalog {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            columns: RwLock::new(HashMap::new()),
            lineage: RwLock::new(LineageGraph::new()),
            glossary: RwLock::new(HashMap::new()),
            tag_index: RwLock::new(HashMap::new()),
            search_index: RwLock::new(HashMap::new()),
            entry_counter: RwLock::new(0),
        }
    }

    /// Generate unique ID
    fn generate_id(&self) -> String {
        let mut counter = self.entry_counter.write();
        *counter += 1;
        format!("entry_{}", *counter)
    }

    /// Register a catalog entry
    pub fn register(&self, mut entry: CatalogEntry) -> String {
        if entry.id.is_empty() {
            entry.id = self.generate_id();
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        if entry.created_at == 0 {
            entry.created_at = now;
        }
        entry.updated_at = now;

        let id = entry.id.clone();

        // Update tag index
        {
            let mut tag_index = self.tag_index.write();
            for tag in &entry.tags {
                tag_index
                    .entry(tag.clone())
                    .or_insert_with(HashSet::new)
                    .insert(id.clone());
            }
        }

        // Update search index
        self.index_entry(&entry);

        self.entries.write().insert(id.clone(), entry);

        id
    }

    /// Index entry for search
    fn index_entry(&self, entry: &CatalogEntry) {
        let mut search_index = self.search_index.write();

        // Index name words
        for word in entry.name.to_lowercase().split_whitespace() {
            search_index
                .entry(word.to_string())
                .or_insert_with(HashSet::new)
                .insert(entry.id.clone());
        }

        // Index description words
        for word in entry.description.to_lowercase().split_whitespace() {
            if word.len() > 2 {
                search_index
                    .entry(word.to_string())
                    .or_insert_with(HashSet::new)
                    .insert(entry.id.clone());
            }
        }

        // Index tags
        for tag in &entry.tags {
            search_index
                .entry(tag.to_lowercase())
                .or_insert_with(HashSet::new)
                .insert(entry.id.clone());
        }

        // Index FQN parts
        for part in entry.fqn.split('.') {
            search_index
                .entry(part.to_lowercase())
                .or_insert_with(HashSet::new)
                .insert(entry.id.clone());
        }
    }

    /// Get entry by ID
    pub fn get(&self, id: &str) -> Option<CatalogEntry> {
        self.entries.read().get(id).cloned()
    }

    /// Get entry by FQN
    pub fn get_by_fqn(&self, fqn: &str) -> Option<CatalogEntry> {
        self.entries.read().values().find(|e| e.fqn == fqn).cloned()
    }

    /// Update entry
    pub fn update(&self, id: &str, updates: HashMap<String, String>) -> bool {
        let mut entries = self.entries.write();

        if let Some(entry) = entries.get_mut(id) {
            for (key, value) in updates {
                match key.as_str() {
                    "description" => entry.description = value,
                    "owner" => entry.owner = value,
                    _ => {
                        entry.metadata.insert(key, value);
                    }
                }
            }

            entry.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            true
        } else {
            false
        }
    }

    /// Add tags to entry
    pub fn add_tags(&self, id: &str, tags: Vec<String>) -> bool {
        let mut entries = self.entries.write();

        if let Some(entry) = entries.get_mut(id) {
            let mut tag_index = self.tag_index.write();

            for tag in tags {
                if !entry.tags.contains(&tag) {
                    entry.tags.push(tag.clone());
                    tag_index
                        .entry(tag)
                        .or_insert_with(HashSet::new)
                        .insert(id.to_string());
                }
            }
            true
        } else {
            false
        }
    }

    /// Remove tags from entry
    pub fn remove_tags(&self, id: &str, tags: &[String]) -> bool {
        let mut entries = self.entries.write();

        if let Some(entry) = entries.get_mut(id) {
            let mut tag_index = self.tag_index.write();

            for tag in tags {
                entry.tags.retain(|t| t != tag);
                if let Some(ids) = tag_index.get_mut(tag) {
                    ids.remove(id);
                }
            }
            true
        } else {
            false
        }
    }

    /// Register columns for a table
    pub fn register_columns(&self, table_id: &str, columns: Vec<ColumnEntry>) {
        self.columns.write().insert(table_id.to_string(), columns);
    }

    /// Get columns for a table
    pub fn get_columns(&self, table_id: &str) -> Vec<ColumnEntry> {
        self.columns
            .read()
            .get(table_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Search the catalog
    pub fn search(&self, query: &SearchQuery) -> Vec<SearchResult> {
        let entries = self.entries.read();
        let search_index = self.search_index.read();

        // Find matching entry IDs
        let search_words: Vec<String> = query
            .text
            .to_lowercase()
            .split_whitespace()
            .map(String::from)
            .collect();

        let mut entry_scores: HashMap<String, (f64, Vec<String>)> = HashMap::new();

        for word in &search_words {
            if let Some(ids) = search_index.get(word) {
                for id in ids {
                    let entry = entry_scores.entry(id.clone()).or_insert((0.0, Vec::new()));
                    entry.0 += 1.0;
                    entry.1.push(word.clone());
                }
            }

            // Prefix matching
            for (indexed_word, ids) in search_index.iter() {
                if indexed_word.starts_with(word) && indexed_word != word {
                    for id in ids {
                        let entry = entry_scores.entry(id.clone()).or_insert((0.0, Vec::new()));
                        entry.0 += 0.5;
                    }
                }
            }
        }

        // Build results
        let mut results: Vec<SearchResult> = entry_scores
            .into_iter()
            .filter_map(|(id, (score, matched))| {
                entries.get(&id).and_then(|entry| {
                    // Apply filters
                    if let Some(ref types) = query.entry_types {
                        if !types.contains(&entry.entry_type) {
                            return None;
                        }
                    }

                    if let Some(ref tags) = query.tags {
                        if !tags.iter().any(|t| entry.tags.contains(t)) {
                            return None;
                        }
                    }

                    if let Some(ref owner) = query.owner {
                        if &entry.owner != owner {
                            return None;
                        }
                    }

                    if let Some(class) = query.classification {
                        if entry.classification != class {
                            return None;
                        }
                    }

                    // Calculate final score
                    let mut final_score = score;
                    final_score += entry.popularity * 0.1;

                    // Generate snippet
                    let snippet = if !entry.description.is_empty() {
                        let words: Vec<&str> =
                            entry.description.split_whitespace().take(20).collect();
                        words.join(" ") + "..."
                    } else {
                        entry.name.clone()
                    };

                    Some(SearchResult {
                        entry: entry.clone(),
                        score: final_score,
                        matched_fields: matched,
                        snippet,
                    })
                })
            })
            .collect();

        // Sort by score
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

        // Apply pagination
        results
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect()
    }

    /// Add lineage relationship
    pub fn add_lineage(&self, source_id: &str, target_id: &str, edge_type: LineageEdgeType) {
        let entries = self.entries.read();

        let source_node = entries.get(source_id).map(|e| LineageNode {
            id: e.id.clone(),
            node_type: match e.entry_type {
                EntryType::Table => LineageNodeType::Table,
                EntryType::View => LineageNodeType::View,
                EntryType::Pipeline => LineageNodeType::Pipeline,
                EntryType::Dashboard => LineageNodeType::Dashboard,
                _ => LineageNodeType::Source,
            },
            name: e.name.clone(),
            fqn: e.fqn.clone(),
        });

        let target_node = entries.get(target_id).map(|e| LineageNode {
            id: e.id.clone(),
            node_type: match e.entry_type {
                EntryType::Table => LineageNodeType::Table,
                EntryType::View => LineageNodeType::View,
                EntryType::Pipeline => LineageNodeType::Pipeline,
                EntryType::Dashboard => LineageNodeType::Dashboard,
                _ => LineageNodeType::Source,
            },
            name: e.name.clone(),
            fqn: e.fqn.clone(),
        });

        drop(entries);

        let mut lineage = self.lineage.write();

        if let Some(node) = source_node {
            lineage.add_node(node);
        }

        if let Some(node) = target_node {
            lineage.add_node(node);
        }

        lineage.add_edge(LineageEdge {
            source: source_id.to_string(),
            target: target_id.to_string(),
            edge_type,
            transformation: None,
            columns: Vec::new(),
        });
    }

    /// Get lineage for an entry
    pub fn get_lineage(&self, id: &str, direction: LineageDirection, depth: usize) -> LineageGraph {
        let lineage = self.lineage.read();
        let mut result = LineageGraph::new();
        let mut visited = HashSet::new();
        let mut to_visit = vec![(id.to_string(), 0)];

        while let Some((current_id, current_depth)) = to_visit.pop() {
            if current_depth > depth || visited.contains(&current_id) {
                continue;
            }

            visited.insert(current_id.clone());

            // Add current node
            if let Some(node) = lineage.nodes.iter().find(|n| n.id == current_id) {
                result.add_node(node.clone());
            }

            // Add edges and queue next nodes
            for edge in &lineage.edges {
                match direction {
                    LineageDirection::Upstream => {
                        if edge.target == current_id && !visited.contains(&edge.source) {
                            result.add_edge(edge.clone());
                            to_visit.push((edge.source.clone(), current_depth + 1));
                        }
                    }
                    LineageDirection::Downstream => {
                        if edge.source == current_id && !visited.contains(&edge.target) {
                            result.add_edge(edge.clone());
                            to_visit.push((edge.target.clone(), current_depth + 1));
                        }
                    }
                    LineageDirection::Both => {
                        if edge.target == current_id && !visited.contains(&edge.source) {
                            result.add_edge(edge.clone());
                            to_visit.push((edge.source.clone(), current_depth + 1));
                        }
                        if edge.source == current_id && !visited.contains(&edge.target) {
                            result.add_edge(edge.clone());
                            to_visit.push((edge.target.clone(), current_depth + 1));
                        }
                    }
                }
            }
        }

        result
    }

    /// Add glossary term
    pub fn add_glossary_term(&self, term: GlossaryTerm) -> String {
        let id = term.id.clone();
        self.glossary.write().insert(id.clone(), term);
        id
    }

    /// Get glossary term
    pub fn get_glossary_term(&self, id: &str) -> Option<GlossaryTerm> {
        self.glossary.read().get(id).cloned()
    }

    /// Search glossary
    pub fn search_glossary(&self, query: &str) -> Vec<GlossaryTerm> {
        let glossary = self.glossary.read();
        let lower_query = query.to_lowercase();

        glossary
            .values()
            .filter(|term| {
                term.name.to_lowercase().contains(&lower_query)
                    || term.definition.to_lowercase().contains(&lower_query)
                    || term
                        .abbreviations
                        .iter()
                        .any(|a| a.to_lowercase().contains(&lower_query))
            })
            .cloned()
            .collect()
    }

    /// Link glossary term to catalog entry
    pub fn link_term_to_entry(&self, term_id: &str, entry_id: &str) -> bool {
        let mut glossary = self.glossary.write();

        if let Some(term) = glossary.get_mut(term_id) {
            if !term.associated_entries.contains(&entry_id.to_string()) {
                term.associated_entries.push(entry_id.to_string());
            }
            true
        } else {
            false
        }
    }

    /// Get entries by tag
    pub fn get_by_tag(&self, tag: &str) -> Vec<CatalogEntry> {
        let tag_index = self.tag_index.read();
        let entries = self.entries.read();

        tag_index
            .get(tag)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| entries.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get popular entries
    pub fn get_popular(&self, limit: usize) -> Vec<CatalogEntry> {
        let entries = self.entries.read();
        let mut sorted: Vec<_> = entries.values().cloned().collect();
        sorted.sort_by(|a, b| b.popularity.partial_cmp(&a.popularity).unwrap());
        sorted.truncate(limit);
        sorted
    }

    /// Record access (updates popularity)
    pub fn record_access(&self, id: &str) {
        let mut entries = self.entries.write();

        if let Some(entry) = entries.get_mut(id) {
            entry.last_accessed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            );
            entry.popularity += 1.0;
        }
    }

    /// Get catalog statistics
    pub fn stats(&self) -> CatalogStats {
        let entries = self.entries.read();
        let glossary = self.glossary.read();

        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for entry in entries.values() {
            *type_counts
                .entry(format!("{:?}", entry.entry_type))
                .or_insert(0) += 1;
        }

        CatalogStats {
            total_entries: entries.len(),
            entry_types: type_counts,
            total_glossary_terms: glossary.len(),
            total_tags: self.tag_index.read().len(),
        }
    }
}

impl Default for DataCatalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Lineage direction
#[derive(Debug, Clone, Copy)]
pub enum LineageDirection {
    Upstream,
    Downstream,
    Both,
}

/// Catalog statistics
#[derive(Debug, Clone)]
pub struct CatalogStats {
    pub total_entries: usize,
    pub entry_types: HashMap<String, usize>,
    pub total_glossary_terms: usize,
    pub total_tags: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_entry() {
        let catalog = DataCatalog::new();

        let entry = CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "db.schema.users".to_string(),
            name: "users".to_string(),
            description: "User accounts table".to_string(),
            owner: "data-team".to_string(),
            tags: vec!["pii".to_string(), "core".to_string()],
            classification: Classification::Pii,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 0.0,
        };

        let id = catalog.register(entry);
        assert!(!id.is_empty());

        let retrieved = catalog.get(&id).unwrap();
        assert_eq!(retrieved.name, "users");
    }

    #[test]
    fn test_search() {
        let catalog = DataCatalog::new();

        catalog.register(CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "db.users".to_string(),
            name: "users".to_string(),
            description: "User accounts".to_string(),
            owner: "data-team".to_string(),
            tags: vec!["core".to_string()],
            classification: Classification::Internal,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 10.0,
        });

        catalog.register(CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "db.orders".to_string(),
            name: "orders".to_string(),
            description: "Customer orders".to_string(),
            owner: "data-team".to_string(),
            tags: vec!["core".to_string()],
            classification: Classification::Internal,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 5.0,
        });

        let query = SearchQuery {
            text: "user".to_string(),
            ..Default::default()
        };

        let results = catalog.search(&query);
        assert!(!results.is_empty());
        assert_eq!(results[0].entry.name, "users");
    }

    #[test]
    fn test_lineage() {
        let catalog = DataCatalog::new();

        let source_id = catalog.register(CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "raw.events".to_string(),
            name: "events".to_string(),
            description: "Raw events".to_string(),
            owner: "data-team".to_string(),
            tags: vec![],
            classification: Classification::Internal,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 0.0,
        });

        let target_id = catalog.register(CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "analytics.events_daily".to_string(),
            name: "events_daily".to_string(),
            description: "Daily aggregated events".to_string(),
            owner: "data-team".to_string(),
            tags: vec![],
            classification: Classification::Internal,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 0.0,
        });

        catalog.add_lineage(&source_id, &target_id, LineageEdgeType::Aggregation);

        let lineage = catalog.get_lineage(&target_id, LineageDirection::Upstream, 5);
        assert!(!lineage.nodes.is_empty());
        assert!(!lineage.edges.is_empty());
    }

    #[test]
    fn test_glossary() {
        let catalog = DataCatalog::new();

        let term_id = catalog.add_glossary_term(GlossaryTerm {
            id: "term_1".to_string(),
            name: "Customer".to_string(),
            definition: "A person or organization that purchases products or services".to_string(),
            abbreviations: vec!["Cust".to_string()],
            related_terms: vec![],
            associated_entries: vec![],
            owner: "business-team".to_string(),
            status: TermStatus::Approved,
            created_at: 0,
        });

        let term = catalog.get_glossary_term(&term_id).unwrap();
        assert_eq!(term.name, "Customer");

        let results = catalog.search_glossary("customer");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_tags() {
        let catalog = DataCatalog::new();

        let id = catalog.register(CatalogEntry {
            id: String::new(),
            entry_type: EntryType::Table,
            fqn: "db.table1".to_string(),
            name: "table1".to_string(),
            description: "Test table".to_string(),
            owner: "owner".to_string(),
            tags: vec!["tag1".to_string()],
            classification: Classification::Internal,
            metadata: HashMap::new(),
            parent_id: None,
            created_at: 0,
            updated_at: 0,
            last_accessed_at: None,
            popularity: 0.0,
        });

        catalog.add_tags(&id, vec!["tag2".to_string(), "tag3".to_string()]);

        let entry = catalog.get(&id).unwrap();
        assert!(entry.tags.contains(&"tag2".to_string()));

        let by_tag = catalog.get_by_tag("tag2");
        assert_eq!(by_tag.len(), 1);
    }
}
