//! Query Hints Module
//!
//! Provides optimizer hints for controlling query execution:
//! - Index hints (USE_INDEX, FORCE_INDEX, IGNORE_INDEX)
//! - Join hints (USE_HASH_JOIN, USE_MERGE_JOIN, USE_NL_JOIN)
//! - Parallelism hints (PARALLEL, NO_PARALLEL)
//! - Scan hints (FULL_SCAN, INDEX_SCAN)
//! - Resource hints (MAX_MEMORY, TIMEOUT)
//!
//! Syntax: /*+ HINT_NAME(params) */

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Query hint types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryHint {
    // Index hints
    UseIndex { table: String, index: String },
    ForceIndex { table: String, index: String },
    IgnoreIndex { table: String, index: String },
    NoIndex { table: String },

    // Join hints
    UseHashJoin { tables: Vec<String> },
    UseMergeJoin { tables: Vec<String> },
    UseNestedLoop { tables: Vec<String> },
    JoinOrder { tables: Vec<String> },
    Leading { tables: Vec<String> },

    // Scan hints
    FullScan { table: String },
    IndexScan { table: String },
    IndexOnly { table: String },
    NoFullScan { table: String },

    // Parallelism hints
    Parallel { degree: Option<usize> },
    NoParallel,
    ParallelIndex { table: String, degree: usize },

    // Resource hints
    MaxMemory { bytes: usize },
    Timeout { millis: u64 },
    MaxRows { rows: usize },

    // Optimization hints
    FirstRows { n: usize },
    AllRows,
    NoMerge { table: String },
    NoRewrite,
    NoPushPredicate { table: String },
    PushPredicate { table: String },

    // Caching hints
    NoCache,
    CacheResult { ttl_secs: u64 },
    NoQueryCache,

    // Statistics hints
    IgnoreStats { table: String },
    CardinalityEstimate { table: String, rows: usize },

    // Execution hints
    Streaming,
    NoStreaming,
    MaterializeEarly { table: String },

    // Debug hints
    ExplainPlan,
    TraceExecution,
}

/// Parsed hint with metadata
#[derive(Clone, Debug)]
pub struct ParsedHint {
    pub hint: QueryHint,
    pub source_position: usize,
    pub raw_text: String,
}

/// Collection of hints for a query
#[derive(Clone, Debug, Default)]
pub struct QueryHints {
    pub hints: Vec<ParsedHint>,
    /// Index hints by table
    pub index_hints: HashMap<String, Vec<IndexHint>>,
    /// Join hints
    pub join_hints: Vec<JoinHint>,
    /// Resource limits
    pub resource_limits: ResourceLimits,
    /// Parallelism settings
    pub parallelism: ParallelismHint,
    /// Caching settings
    pub caching: CachingHint,
}

/// Index-specific hint
#[derive(Clone, Debug)]
pub struct IndexHint {
    pub hint_type: IndexHintType,
    pub index_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexHintType {
    Use,
    Force,
    Ignore,
    None,
}

/// Join-specific hint
#[derive(Clone, Debug)]
pub struct JoinHint {
    pub join_type: JoinHintType,
    pub tables: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinHintType {
    Hash,
    Merge,
    NestedLoop,
    Any,
}

/// Resource limits from hints
#[derive(Clone, Debug, Default)]
pub struct ResourceLimits {
    pub max_memory_bytes: Option<usize>,
    pub timeout_millis: Option<u64>,
    pub max_rows: Option<usize>,
}

/// Parallelism hint settings
#[derive(Clone, Debug)]
pub struct ParallelismHint {
    pub enabled: bool,
    pub degree: Option<usize>,
    pub per_table: HashMap<String, usize>,
}

impl Default for ParallelismHint {
    fn default() -> Self {
        Self {
            enabled: true,
            degree: None,
            per_table: HashMap::new(),
        }
    }
}

/// Caching hint settings
#[derive(Clone, Debug, Default)]
pub struct CachingHint {
    pub use_cache: bool,
    pub cache_result: bool,
    pub result_ttl_secs: Option<u64>,
}

/// Hint parser
pub struct HintParser;

impl HintParser {
    /// Parse hints from SQL query
    /// Extracts /*+ ... */ style hints
    pub fn parse(sql: &str) -> QueryHints {
        let mut hints = QueryHints::default();

        // Find all hint blocks
        let mut pos = 0;
        while let Some(start) = sql[pos..].find("/*+") {
            let start = pos + start;
            if let Some(end) = sql[start..].find("*/") {
                let end = start + end;
                let hint_text = &sql[start + 3..end].trim();
                Self::parse_hint_block(hint_text, start, &mut hints);
                pos = end + 2;
            } else {
                break;
            }
        }

        hints
    }

    fn parse_hint_block(text: &str, position: usize, hints: &mut QueryHints) {
        // Split by whitespace or comma
        for hint_str in text.split(|c: char| c == ',' || c.is_whitespace()) {
            let hint_str = hint_str.trim();
            if hint_str.is_empty() {
                continue;
            }

            if let Some(hint) = Self::parse_single_hint(hint_str) {
                // Apply hint to appropriate collection
                Self::apply_hint(&hint, hints);

                hints.hints.push(ParsedHint {
                    hint,
                    source_position: position,
                    raw_text: hint_str.to_string(),
                });
            }
        }
    }

    fn parse_single_hint(text: &str) -> Option<QueryHint> {
        let text = text.trim();
        let upper = text.to_uppercase();

        // Check for hints with parameters
        if let Some(paren_start) = text.find('(') {
            if let Some(paren_end) = text.rfind(')') {
                let name = text[..paren_start].trim().to_uppercase();
                let params = text[paren_start + 1..paren_end].trim();

                return Self::parse_parameterized_hint(&name, params);
            }
        }

        // Simple hints without parameters
        match upper.as_str() {
            "NO_PARALLEL" | "NOPARALLEL" => Some(QueryHint::NoParallel),
            "ALL_ROWS" | "ALLROWS" => Some(QueryHint::AllRows),
            "NO_CACHE" | "NOCACHE" => Some(QueryHint::NoCache),
            "NO_QUERY_CACHE" => Some(QueryHint::NoQueryCache),
            "NO_REWRITE" | "NOREWRITE" => Some(QueryHint::NoRewrite),
            "STREAMING" => Some(QueryHint::Streaming),
            "NO_STREAMING" => Some(QueryHint::NoStreaming),
            "EXPLAIN_PLAN" => Some(QueryHint::ExplainPlan),
            "TRACE_EXECUTION" | "TRACE" => Some(QueryHint::TraceExecution),
            _ => None,
        }
    }

    fn parse_parameterized_hint(name: &str, params: &str) -> Option<QueryHint> {
        let params = params.trim();
        let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

        match name {
            // Index hints
            "USE_INDEX" | "INDEX" => {
                if parts.len() >= 2 {
                    Some(QueryHint::UseIndex {
                        table: parts[0].to_string(),
                        index: parts[1].to_string(),
                    })
                } else if parts.len() == 1 {
                    Some(QueryHint::UseIndex {
                        table: "".to_string(),
                        index: parts[0].to_string(),
                    })
                } else {
                    None
                }
            }
            "FORCE_INDEX" => {
                if parts.len() >= 2 {
                    Some(QueryHint::ForceIndex {
                        table: parts[0].to_string(),
                        index: parts[1].to_string(),
                    })
                } else {
                    None
                }
            }
            "IGNORE_INDEX" | "NO_INDEX" => {
                if parts.len() >= 2 {
                    Some(QueryHint::IgnoreIndex {
                        table: parts[0].to_string(),
                        index: parts[1].to_string(),
                    })
                } else if parts.len() == 1 {
                    Some(QueryHint::NoIndex {
                        table: parts[0].to_string(),
                    })
                } else {
                    None
                }
            }

            // Join hints
            "USE_HASH_JOIN" | "HASH_JOIN" | "HASH" => Some(QueryHint::UseHashJoin {
                tables: parts.iter().map(|s| s.to_string()).collect(),
            }),
            "USE_MERGE_JOIN" | "MERGE_JOIN" | "MERGE" => Some(QueryHint::UseMergeJoin {
                tables: parts.iter().map(|s| s.to_string()).collect(),
            }),
            "USE_NL_JOIN" | "NL_JOIN" | "NESTED_LOOP" | "NL" => Some(QueryHint::UseNestedLoop {
                tables: parts.iter().map(|s| s.to_string()).collect(),
            }),
            "JOIN_ORDER" | "ORDERED" => Some(QueryHint::JoinOrder {
                tables: parts.iter().map(|s| s.to_string()).collect(),
            }),
            "LEADING" => Some(QueryHint::Leading {
                tables: parts.iter().map(|s| s.to_string()).collect(),
            }),

            // Scan hints
            "FULL" | "FULL_SCAN" => Some(QueryHint::FullScan {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "INDEX_SCAN" => Some(QueryHint::IndexScan {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "INDEX_ONLY" => Some(QueryHint::IndexOnly {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "NO_FULL_SCAN" => Some(QueryHint::NoFullScan {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),

            // Parallelism hints
            "PARALLEL" => {
                let degree = parts.first().and_then(|s| s.parse().ok());
                Some(QueryHint::Parallel { degree })
            }
            "PARALLEL_INDEX" => {
                if parts.len() >= 2 {
                    Some(QueryHint::ParallelIndex {
                        table: parts[0].to_string(),
                        degree: parts[1].parse().unwrap_or(4),
                    })
                } else {
                    None
                }
            }

            // Resource hints
            "MAX_MEMORY" | "MEMORY" => {
                let bytes = Self::parse_bytes(parts.first().copied().unwrap_or("0"));
                Some(QueryHint::MaxMemory { bytes })
            }
            "TIMEOUT" => {
                let millis = parts.first().and_then(|s| s.parse().ok()).unwrap_or(30000);
                Some(QueryHint::Timeout { millis })
            }
            "MAX_ROWS" | "ROWNUM" => {
                let rows = parts.first().and_then(|s| s.parse().ok()).unwrap_or(1000000);
                Some(QueryHint::MaxRows { rows })
            }

            // Optimization hints
            "FIRST_ROWS" => {
                let n = parts.first().and_then(|s| s.parse().ok()).unwrap_or(10);
                Some(QueryHint::FirstRows { n })
            }
            "NO_MERGE" => Some(QueryHint::NoMerge {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "NO_PUSH_PRED" | "NO_PUSH_PREDICATE" => Some(QueryHint::NoPushPredicate {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "PUSH_PRED" | "PUSH_PREDICATE" => Some(QueryHint::PushPredicate {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),

            // Caching hints
            "CACHE_RESULT" | "RESULT_CACHE" => {
                let ttl = parts.first().and_then(|s| s.parse().ok()).unwrap_or(300);
                Some(QueryHint::CacheResult { ttl_secs: ttl })
            }

            // Statistics hints
            "IGNORE_STATS" => Some(QueryHint::IgnoreStats {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),
            "CARDINALITY" | "OPT_ESTIMATE" => {
                if parts.len() >= 2 {
                    Some(QueryHint::CardinalityEstimate {
                        table: parts[0].to_string(),
                        rows: parts[1].parse().unwrap_or(1000),
                    })
                } else {
                    None
                }
            }

            // Execution hints
            "MATERIALIZE_EARLY" | "MATERIALIZE" => Some(QueryHint::MaterializeEarly {
                table: parts.first().map(|s| s.to_string()).unwrap_or_default(),
            }),

            _ => None,
        }
    }

    fn parse_bytes(s: &str) -> usize {
        let s = s.trim().to_uppercase();

        if let Some(num) = s.strip_suffix("GB") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024 * 1024;
        }
        if let Some(num) = s.strip_suffix("G") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024 * 1024;
        }
        if let Some(num) = s.strip_suffix("MB") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024;
        }
        if let Some(num) = s.strip_suffix("M") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024;
        }
        if let Some(num) = s.strip_suffix("KB") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024;
        }
        if let Some(num) = s.strip_suffix("K") {
            return num.trim().parse::<usize>().unwrap_or(0) * 1024;
        }

        s.parse().unwrap_or(0)
    }

    fn apply_hint(hint: &QueryHint, hints: &mut QueryHints) {
        match hint {
            QueryHint::UseIndex { table, index } => {
                hints.index_hints
                    .entry(table.clone())
                    .or_default()
                    .push(IndexHint {
                        hint_type: IndexHintType::Use,
                        index_name: Some(index.clone()),
                    });
            }
            QueryHint::ForceIndex { table, index } => {
                hints.index_hints
                    .entry(table.clone())
                    .or_default()
                    .push(IndexHint {
                        hint_type: IndexHintType::Force,
                        index_name: Some(index.clone()),
                    });
            }
            QueryHint::IgnoreIndex { table, index } => {
                hints.index_hints
                    .entry(table.clone())
                    .or_default()
                    .push(IndexHint {
                        hint_type: IndexHintType::Ignore,
                        index_name: Some(index.clone()),
                    });
            }
            QueryHint::NoIndex { table } => {
                hints.index_hints
                    .entry(table.clone())
                    .or_default()
                    .push(IndexHint {
                        hint_type: IndexHintType::None,
                        index_name: None,
                    });
            }

            QueryHint::UseHashJoin { tables } => {
                hints.join_hints.push(JoinHint {
                    join_type: JoinHintType::Hash,
                    tables: tables.clone(),
                });
            }
            QueryHint::UseMergeJoin { tables } => {
                hints.join_hints.push(JoinHint {
                    join_type: JoinHintType::Merge,
                    tables: tables.clone(),
                });
            }
            QueryHint::UseNestedLoop { tables } => {
                hints.join_hints.push(JoinHint {
                    join_type: JoinHintType::NestedLoop,
                    tables: tables.clone(),
                });
            }

            QueryHint::Parallel { degree } => {
                hints.parallelism.enabled = true;
                hints.parallelism.degree = *degree;
            }
            QueryHint::NoParallel => {
                hints.parallelism.enabled = false;
            }
            QueryHint::ParallelIndex { table, degree } => {
                hints.parallelism.per_table.insert(table.clone(), *degree);
            }

            QueryHint::MaxMemory { bytes } => {
                hints.resource_limits.max_memory_bytes = Some(*bytes);
            }
            QueryHint::Timeout { millis } => {
                hints.resource_limits.timeout_millis = Some(*millis);
            }
            QueryHint::MaxRows { rows } => {
                hints.resource_limits.max_rows = Some(*rows);
            }

            QueryHint::NoCache | QueryHint::NoQueryCache => {
                hints.caching.use_cache = false;
            }
            QueryHint::CacheResult { ttl_secs } => {
                hints.caching.cache_result = true;
                hints.caching.result_ttl_secs = Some(*ttl_secs);
            }

            _ => {}
        }
    }
}

impl QueryHints {
    /// Check if any hints are present
    pub fn has_hints(&self) -> bool {
        !self.hints.is_empty()
    }

    /// Get index hint for a table
    pub fn get_index_hint(&self, table: &str) -> Option<&IndexHint> {
        self.index_hints.get(table).and_then(|hints| hints.first())
    }

    /// Get join hint type for tables
    pub fn get_join_hint(&self, table1: &str, table2: &str) -> Option<JoinHintType> {
        for join_hint in &self.join_hints {
            if join_hint.tables.contains(&table1.to_string())
                && join_hint.tables.contains(&table2.to_string())
            {
                return Some(join_hint.join_type);
            }
        }
        None
    }

    /// Check if parallel execution is allowed
    pub fn allow_parallel(&self) -> bool {
        self.parallelism.enabled
    }

    /// Get parallel degree
    pub fn parallel_degree(&self) -> Option<usize> {
        self.parallelism.degree
    }

    /// Get timeout limit
    pub fn timeout_millis(&self) -> Option<u64> {
        self.resource_limits.timeout_millis
    }

    /// Get max memory limit
    pub fn max_memory(&self) -> Option<usize> {
        self.resource_limits.max_memory_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_hints() {
        let sql = "SELECT /*+ NO_PARALLEL */ * FROM users";
        let hints = HintParser::parse(sql);

        assert!(hints.has_hints());
        assert!(!hints.allow_parallel());
    }

    #[test]
    fn test_parse_index_hints() {
        let sql = "SELECT /*+ USE_INDEX(users, idx_email) */ * FROM users WHERE email = 'test'";
        let hints = HintParser::parse(sql);

        assert!(hints.has_hints());
        let index_hint = hints.get_index_hint("users").unwrap();
        assert_eq!(index_hint.hint_type, IndexHintType::Use);
        assert_eq!(index_hint.index_name, Some("idx_email".to_string()));
    }

    #[test]
    fn test_parse_join_hints() {
        let sql = "SELECT /*+ USE_HASH_JOIN(orders, customers) */ * FROM orders o JOIN customers c ON o.customer_id = c.id";
        let hints = HintParser::parse(sql);

        assert!(hints.has_hints());
        let join_type = hints.get_join_hint("orders", "customers");
        assert_eq!(join_type, Some(JoinHintType::Hash));
    }

    #[test]
    fn test_parse_resource_hints() {
        let sql = "SELECT /*+ MAX_MEMORY(1GB) TIMEOUT(5000) */ * FROM big_table";
        let hints = HintParser::parse(sql);

        assert_eq!(hints.max_memory(), Some(1024 * 1024 * 1024));
        assert_eq!(hints.timeout_millis(), Some(5000));
    }

    #[test]
    fn test_parse_parallel_hints() {
        let sql = "SELECT /*+ PARALLEL(8) */ COUNT(*) FROM huge_table";
        let hints = HintParser::parse(sql);

        assert!(hints.allow_parallel());
        assert_eq!(hints.parallel_degree(), Some(8));
    }

    #[test]
    fn test_multiple_hints() {
        let sql = "SELECT /*+ USE_INDEX(users, idx_name) PARALLEL(4) MAX_ROWS(1000) */ * FROM users";
        let hints = HintParser::parse(sql);

        assert_eq!(hints.hints.len(), 3);
        assert!(hints.get_index_hint("users").is_some());
        assert_eq!(hints.parallel_degree(), Some(4));
        assert_eq!(hints.resource_limits.max_rows, Some(1000));
    }
}
