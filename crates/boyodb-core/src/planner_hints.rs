//! Query Hints - Planner Hints for Query Optimization
//!
//! This module provides query hints for influencing the planner:
//! - Index hints (USE INDEX, FORCE INDEX, IGNORE INDEX)
//! - Join order hints
//! - Scan method hints
//! - Parallelism hints
//! - Optimizer goal hints

use std::collections::HashMap;

// ============================================================================
// HINT TYPES
// ============================================================================

/// Query hint
#[derive(Debug, Clone, PartialEq)]
pub enum QueryHint {
    /// Use specific index
    UseIndex(IndexHint),
    /// Force specific index
    ForceIndex(IndexHint),
    /// Ignore specific index
    IgnoreIndex(IndexHint),
    /// Set join order
    JoinOrder(JoinOrderHint),
    /// Set scan method
    ScanMethod(ScanMethodHint),
    /// Set join method
    JoinMethod(JoinMethodHint),
    /// Set parallel workers
    Parallel(ParallelHint),
    /// Set optimizer goal
    OptimizerGoal(OptimizerGoal),
    /// Set row estimate
    RowEstimate(RowEstimateHint),
    /// Leading tables for join order
    Leading(Vec<String>),
    /// No merge for subquery
    NoMerge(String),
    /// Materialize subquery
    Materialize(String),
    /// Push predicates
    PushPredicate(String),
    /// No push predicates
    NoPushPredicate(String),
    /// Set statement timeout
    Timeout(u64),
    /// Custom hint
    Custom(String, String),
}

/// Index hint
#[derive(Debug, Clone, PartialEq)]
pub struct IndexHint {
    /// Table name
    pub table: String,
    /// Index names (empty = all indexes)
    pub indexes: Vec<String>,
    /// Hint scope
    pub scope: IndexHintScope,
}

impl IndexHint {
    /// Create a new index hint
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            indexes: Vec::new(),
            scope: IndexHintScope::All,
        }
    }

    /// Add specific index
    pub fn with_index(mut self, index: &str) -> Self {
        self.indexes.push(index.to_string());
        self
    }

    /// Set scope
    pub fn with_scope(mut self, scope: IndexHintScope) -> Self {
        self.scope = scope;
        self
    }
}

/// Index hint scope
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexHintScope {
    /// All operations
    All,
    /// JOIN operations only
    Join,
    /// ORDER BY operations only
    OrderBy,
    /// GROUP BY operations only
    GroupBy,
}

/// Join order hint
#[derive(Debug, Clone, PartialEq)]
pub struct JoinOrderHint {
    /// Tables in join order
    pub tables: Vec<String>,
    /// Fixed order (no reordering)
    pub fixed: bool,
}

impl JoinOrderHint {
    /// Create a new join order hint
    pub fn new(tables: Vec<String>) -> Self {
        Self {
            tables,
            fixed: false,
        }
    }

    /// Set fixed order
    pub fn fixed(mut self) -> Self {
        self.fixed = true;
        self
    }
}

/// Scan method hint
#[derive(Debug, Clone, PartialEq)]
pub struct ScanMethodHint {
    /// Table name
    pub table: String,
    /// Scan method
    pub method: ScanMethod,
}

/// Scan method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanMethod {
    /// Sequential scan
    SeqScan,
    /// Index scan
    IndexScan,
    /// Index-only scan
    IndexOnlyScan,
    /// Bitmap scan
    BitmapScan,
    /// TID scan
    TidScan,
    /// No sequential scan
    NoSeqScan,
    /// No index scan
    NoIndexScan,
    /// No bitmap scan
    NoBitmapScan,
}

impl std::fmt::Display for ScanMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ScanMethod::SeqScan => "SeqScan",
            ScanMethod::IndexScan => "IndexScan",
            ScanMethod::IndexOnlyScan => "IndexOnlyScan",
            ScanMethod::BitmapScan => "BitmapScan",
            ScanMethod::TidScan => "TidScan",
            ScanMethod::NoSeqScan => "NoSeqScan",
            ScanMethod::NoIndexScan => "NoIndexScan",
            ScanMethod::NoBitmapScan => "NoBitmapScan",
        };
        write!(f, "{}", name)
    }
}

/// Join method hint
#[derive(Debug, Clone, PartialEq)]
pub struct JoinMethodHint {
    /// Tables involved
    pub tables: Vec<String>,
    /// Join method
    pub method: JoinMethod,
}

/// Join method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinMethod {
    /// Nested loop join
    NestLoop,
    /// Hash join
    HashJoin,
    /// Merge join
    MergeJoin,
    /// No nested loop
    NoNestLoop,
    /// No hash join
    NoHashJoin,
    /// No merge join
    NoMergeJoin,
}

impl std::fmt::Display for JoinMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            JoinMethod::NestLoop => "NestLoop",
            JoinMethod::HashJoin => "HashJoin",
            JoinMethod::MergeJoin => "MergeJoin",
            JoinMethod::NoNestLoop => "NoNestLoop",
            JoinMethod::NoHashJoin => "NoHashJoin",
            JoinMethod::NoMergeJoin => "NoMergeJoin",
        };
        write!(f, "{}", name)
    }
}

/// Parallel hint
#[derive(Debug, Clone, PartialEq)]
pub struct ParallelHint {
    /// Table name (or empty for query-wide)
    pub table: Option<String>,
    /// Number of workers (0 = disable parallel)
    pub workers: usize,
    /// Force parallel
    pub force: bool,
}

impl ParallelHint {
    /// Create a query-wide parallel hint
    pub fn query_wide(workers: usize) -> Self {
        Self {
            table: None,
            workers,
            force: false,
        }
    }

    /// Create a table-specific parallel hint
    pub fn for_table(table: &str, workers: usize) -> Self {
        Self {
            table: Some(table.to_string()),
            workers,
            force: false,
        }
    }

    /// Force parallel execution
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }
}

/// Optimizer goal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OptimizerGoal {
    /// Minimize first row latency
    FirstRows,
    /// Minimize first N rows latency
    FirstRowsN(u32),
    /// Minimize total cost (default)
    #[default]
    AllRows,
    /// Minimize memory usage
    LowMemory,
    /// Maximize parallelism
    MaxParallel,
}

impl std::fmt::Display for OptimizerGoal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizerGoal::FirstRows => write!(f, "FIRST_ROWS"),
            OptimizerGoal::FirstRowsN(n) => write!(f, "FIRST_ROWS_{}", n),
            OptimizerGoal::AllRows => write!(f, "ALL_ROWS"),
            OptimizerGoal::LowMemory => write!(f, "LOW_MEMORY"),
            OptimizerGoal::MaxParallel => write!(f, "MAX_PARALLEL"),
        }
    }
}

/// Row estimate hint
#[derive(Debug, Clone, PartialEq)]
pub struct RowEstimateHint {
    /// Table or subquery name
    pub target: String,
    /// Estimated rows
    pub rows: u64,
    /// Scale factor (multiply existing estimate)
    pub scale: Option<f64>,
}

impl RowEstimateHint {
    /// Create an absolute row estimate
    pub fn absolute(target: &str, rows: u64) -> Self {
        Self {
            target: target.to_string(),
            rows,
            scale: None,
        }
    }

    /// Create a scaled row estimate
    pub fn scaled(target: &str, scale: f64) -> Self {
        Self {
            target: target.to_string(),
            rows: 0,
            scale: Some(scale),
        }
    }
}

// ============================================================================
// HINT COLLECTION
// ============================================================================

/// Collection of hints for a query
#[derive(Debug, Clone, Default)]
pub struct HintCollection {
    /// All hints
    hints: Vec<QueryHint>,
    /// Index hints by table
    index_hints: HashMap<String, Vec<IndexHint>>,
    /// Scan method hints by table
    scan_hints: HashMap<String, ScanMethod>,
    /// Join method hints
    join_hints: Vec<JoinMethodHint>,
    /// Parallel hints
    parallel_hints: Vec<ParallelHint>,
    /// Optimizer goal
    optimizer_goal: Option<OptimizerGoal>,
    /// Leading tables
    leading: Option<Vec<String>>,
    /// Row estimates
    row_estimates: HashMap<String, RowEstimateHint>,
}

impl HintCollection {
    /// Create a new empty collection
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a hint
    pub fn add(&mut self, hint: QueryHint) {
        match &hint {
            QueryHint::UseIndex(h) | QueryHint::ForceIndex(h) | QueryHint::IgnoreIndex(h) => {
                self.index_hints
                    .entry(h.table.clone())
                    .or_default()
                    .push(h.clone());
            }
            QueryHint::ScanMethod(h) => {
                self.scan_hints.insert(h.table.clone(), h.method);
            }
            QueryHint::JoinMethod(h) => {
                self.join_hints.push(h.clone());
            }
            QueryHint::Parallel(h) => {
                self.parallel_hints.push(h.clone());
            }
            QueryHint::OptimizerGoal(g) => {
                self.optimizer_goal = Some(*g);
            }
            QueryHint::Leading(tables) => {
                self.leading = Some(tables.clone());
            }
            QueryHint::RowEstimate(h) => {
                self.row_estimates.insert(h.target.clone(), h.clone());
            }
            _ => {}
        }
        self.hints.push(hint);
    }

    /// Get index hints for a table
    pub fn get_index_hints(&self, table: &str) -> Option<&Vec<IndexHint>> {
        self.index_hints.get(table)
    }

    /// Get scan method for a table
    pub fn get_scan_method(&self, table: &str) -> Option<ScanMethod> {
        self.scan_hints.get(table).copied()
    }

    /// Get join method hints for tables
    pub fn get_join_method(&self, tables: &[&str]) -> Option<JoinMethod> {
        for hint in &self.join_hints {
            let matches = hint.tables.iter().all(|t| tables.contains(&t.as_str()));
            if matches {
                return Some(hint.method);
            }
        }
        None
    }

    /// Get parallel workers for a table
    pub fn get_parallel_workers(&self, table: Option<&str>) -> Option<usize> {
        for hint in &self.parallel_hints {
            match (&hint.table, table) {
                (Some(t), Some(target)) if t == target => return Some(hint.workers),
                (None, _) => return Some(hint.workers),
                _ => {}
            }
        }
        None
    }

    /// Get optimizer goal
    pub fn get_optimizer_goal(&self) -> Option<OptimizerGoal> {
        self.optimizer_goal
    }

    /// Get leading tables
    pub fn get_leading(&self) -> Option<&Vec<String>> {
        self.leading.as_ref()
    }

    /// Get row estimate for a target
    pub fn get_row_estimate(&self, target: &str) -> Option<&RowEstimateHint> {
        self.row_estimates.get(target)
    }

    /// Check if any hints are present
    pub fn is_empty(&self) -> bool {
        self.hints.is_empty()
    }

    /// Get all hints
    pub fn all_hints(&self) -> &[QueryHint] {
        &self.hints
    }

    /// Merge with another collection
    pub fn merge(&mut self, other: HintCollection) {
        for hint in other.hints {
            self.add(hint);
        }
    }
}

// ============================================================================
// HINT PARSER
// ============================================================================

/// Parse hints from a comment block
pub fn parse_hints(comment: &str) -> HintCollection {
    let mut collection = HintCollection::new();

    // Look for hint markers
    let comment = comment.trim();
    if !comment.starts_with("/*+") || !comment.ends_with("*/") {
        return collection;
    }

    let content = &comment[3..comment.len() - 2].trim();

    // Parse individual hints
    for hint_str in content.split_whitespace() {
        if let Some(hint) = parse_single_hint(hint_str) {
            collection.add(hint);
        }
    }

    // Also try parsing structured hints
    for line in content.lines() {
        let line = line.trim();
        if let Some(hint) = parse_structured_hint(line) {
            collection.add(hint);
        }
    }

    collection
}

fn parse_single_hint(hint: &str) -> Option<QueryHint> {
    let hint_upper = hint.to_uppercase();

    // Simple keyword hints
    match hint_upper.as_str() {
        "FIRST_ROWS" => return Some(QueryHint::OptimizerGoal(OptimizerGoal::FirstRows)),
        "ALL_ROWS" => return Some(QueryHint::OptimizerGoal(OptimizerGoal::AllRows)),
        "LOW_MEMORY" => return Some(QueryHint::OptimizerGoal(OptimizerGoal::LowMemory)),
        "MAX_PARALLEL" => return Some(QueryHint::OptimizerGoal(OptimizerGoal::MaxParallel)),
        _ => {}
    }

    // FIRST_ROWS_N
    if hint_upper.starts_with("FIRST_ROWS_") {
        if let Ok(n) = hint[11..].parse::<u32>() {
            return Some(QueryHint::OptimizerGoal(OptimizerGoal::FirstRowsN(n)));
        }
    }

    // NO_* hints
    if hint_upper.starts_with("NO_") {
        match &hint_upper[3..] {
            "SEQSCAN" => {
                return Some(QueryHint::ScanMethod(ScanMethodHint {
                    table: String::new(),
                    method: ScanMethod::NoSeqScan,
                }))
            }
            "INDEXSCAN" => {
                return Some(QueryHint::ScanMethod(ScanMethodHint {
                    table: String::new(),
                    method: ScanMethod::NoIndexScan,
                }))
            }
            "NESTLOOP" => {
                return Some(QueryHint::JoinMethod(JoinMethodHint {
                    tables: Vec::new(),
                    method: JoinMethod::NoNestLoop,
                }))
            }
            "HASHJOIN" => {
                return Some(QueryHint::JoinMethod(JoinMethodHint {
                    tables: Vec::new(),
                    method: JoinMethod::NoHashJoin,
                }))
            }
            "MERGEJOIN" => {
                return Some(QueryHint::JoinMethod(JoinMethodHint {
                    tables: Vec::new(),
                    method: JoinMethod::NoMergeJoin,
                }))
            }
            _ => {}
        }
    }

    None
}

fn parse_structured_hint(line: &str) -> Option<QueryHint> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    // Parse function-style hints: HINT_NAME(args)
    if let Some(paren_start) = line.find('(') {
        if let Some(paren_end) = line.rfind(')') {
            let name = line[..paren_start].to_uppercase();
            let args = &line[paren_start + 1..paren_end];

            match name.as_str() {
                "USE_INDEX" | "USEINDEX" => {
                    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                    if !parts.is_empty() {
                        let mut hint = IndexHint::new(parts[0]);
                        for idx in parts.iter().skip(1) {
                            hint = hint.with_index(idx);
                        }
                        return Some(QueryHint::UseIndex(hint));
                    }
                }
                "FORCE_INDEX" | "FORCEINDEX" => {
                    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                    if !parts.is_empty() {
                        let mut hint = IndexHint::new(parts[0]);
                        for idx in parts.iter().skip(1) {
                            hint = hint.with_index(idx);
                        }
                        return Some(QueryHint::ForceIndex(hint));
                    }
                }
                "IGNORE_INDEX" | "IGNOREINDEX" => {
                    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                    if !parts.is_empty() {
                        let mut hint = IndexHint::new(parts[0]);
                        for idx in parts.iter().skip(1) {
                            hint = hint.with_index(idx);
                        }
                        return Some(QueryHint::IgnoreIndex(hint));
                    }
                }
                "SEQSCAN" => {
                    return Some(QueryHint::ScanMethod(ScanMethodHint {
                        table: args.to_string(),
                        method: ScanMethod::SeqScan,
                    }));
                }
                "INDEXSCAN" => {
                    return Some(QueryHint::ScanMethod(ScanMethodHint {
                        table: args.to_string(),
                        method: ScanMethod::IndexScan,
                    }));
                }
                "NESTLOOP" => {
                    let tables: Vec<String> =
                        args.split(',').map(|s| s.trim().to_string()).collect();
                    return Some(QueryHint::JoinMethod(JoinMethodHint {
                        tables,
                        method: JoinMethod::NestLoop,
                    }));
                }
                "HASHJOIN" => {
                    let tables: Vec<String> =
                        args.split(',').map(|s| s.trim().to_string()).collect();
                    return Some(QueryHint::JoinMethod(JoinMethodHint {
                        tables,
                        method: JoinMethod::HashJoin,
                    }));
                }
                "MERGEJOIN" => {
                    let tables: Vec<String> =
                        args.split(',').map(|s| s.trim().to_string()).collect();
                    return Some(QueryHint::JoinMethod(JoinMethodHint {
                        tables,
                        method: JoinMethod::MergeJoin,
                    }));
                }
                "PARALLEL" => {
                    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                    if let Some(workers_str) = parts.first() {
                        if let Ok(workers) = workers_str.parse::<usize>() {
                            let table = parts.get(1).map(|s| s.to_string());
                            return Some(QueryHint::Parallel(ParallelHint {
                                table,
                                workers,
                                force: false,
                            }));
                        }
                    }
                }
                "LEADING" => {
                    let tables: Vec<String> =
                        args.split(',').map(|s| s.trim().to_string()).collect();
                    return Some(QueryHint::Leading(tables));
                }
                "ROWS" => {
                    let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                    if parts.len() >= 2 {
                        if let Ok(rows) = parts[1].parse::<u64>() {
                            return Some(QueryHint::RowEstimate(RowEstimateHint::absolute(
                                parts[0], rows,
                            )));
                        }
                    }
                }
                "NO_MERGE" | "NOMERGE" => {
                    return Some(QueryHint::NoMerge(args.to_string()));
                }
                "MATERIALIZE" => {
                    return Some(QueryHint::Materialize(args.to_string()));
                }
                _ => {}
            }
        }
    }

    None
}

/// Extract hint comment from SQL
pub fn extract_hints(sql: &str) -> Option<(HintCollection, usize)> {
    let sql = sql.trim();

    // Look for hint comment after SELECT/INSERT/UPDATE/DELETE
    let keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH"];

    for keyword in keywords {
        if let Some(pos) = sql.to_uppercase().find(keyword) {
            let after_keyword = &sql[pos + keyword.len()..];
            let after_keyword = after_keyword.trim_start();

            if after_keyword.starts_with("/*+") {
                if let Some(end) = after_keyword.find("*/") {
                    let hint_comment = &after_keyword[..end + 2];
                    let hints = parse_hints(hint_comment);
                    let hint_end = pos
                        + keyword.len()
                        + (after_keyword.len() - after_keyword.trim_start().len())
                        + end
                        + 2;
                    return Some((hints, hint_end));
                }
            }
        }
    }

    None
}

// ============================================================================
// HINT APPLIER
// ============================================================================

/// Apply hints to planner settings
#[derive(Debug, Clone, Default)]
pub struct PlannerSettings {
    /// Enable sequential scan
    pub enable_seqscan: bool,
    /// Enable index scan
    pub enable_indexscan: bool,
    /// Enable index-only scan
    pub enable_indexonlyscan: bool,
    /// Enable bitmap scan
    pub enable_bitmapscan: bool,
    /// Enable TID scan
    pub enable_tidscan: bool,
    /// Enable nested loop
    pub enable_nestloop: bool,
    /// Enable hash join
    pub enable_hashjoin: bool,
    /// Enable merge join
    pub enable_mergejoin: bool,
    /// Enable parallel mode
    pub enable_parallel: bool,
    /// Parallel workers
    pub parallel_workers: Option<usize>,
    /// Optimizer goal
    pub optimizer_goal: OptimizerGoal,
}

impl PlannerSettings {
    /// Create default settings
    pub fn default_all_enabled() -> Self {
        Self {
            enable_seqscan: true,
            enable_indexscan: true,
            enable_indexonlyscan: true,
            enable_bitmapscan: true,
            enable_tidscan: true,
            enable_nestloop: true,
            enable_hashjoin: true,
            enable_mergejoin: true,
            enable_parallel: true,
            parallel_workers: None,
            optimizer_goal: OptimizerGoal::AllRows,
        }
    }

    /// Apply hints to settings
    pub fn apply_hints(&mut self, hints: &HintCollection) {
        for hint in hints.all_hints() {
            match hint {
                QueryHint::ScanMethod(h) => match h.method {
                    ScanMethod::NoSeqScan => self.enable_seqscan = false,
                    ScanMethod::NoIndexScan => self.enable_indexscan = false,
                    ScanMethod::NoBitmapScan => self.enable_bitmapscan = false,
                    _ => {}
                },
                QueryHint::JoinMethod(h) => match h.method {
                    JoinMethod::NoNestLoop => self.enable_nestloop = false,
                    JoinMethod::NoHashJoin => self.enable_hashjoin = false,
                    JoinMethod::NoMergeJoin => self.enable_mergejoin = false,
                    _ => {}
                },
                QueryHint::Parallel(h) => {
                    if h.workers == 0 {
                        self.enable_parallel = false;
                    } else {
                        self.parallel_workers = Some(h.workers);
                    }
                }
                QueryHint::OptimizerGoal(g) => {
                    self.optimizer_goal = *g;
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_hint() {
        let hint = IndexHint::new("users")
            .with_index("idx_email")
            .with_scope(IndexHintScope::Join);

        assert_eq!(hint.table, "users");
        assert_eq!(hint.indexes, vec!["idx_email"]);
        assert_eq!(hint.scope, IndexHintScope::Join);
    }

    #[test]
    fn test_join_order_hint() {
        let hint =
            JoinOrderHint::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]).fixed();
        assert!(hint.fixed);
        assert_eq!(hint.tables.len(), 3);
    }

    #[test]
    fn test_parallel_hint() {
        let hint = ParallelHint::query_wide(4).force();
        assert_eq!(hint.workers, 4);
        assert!(hint.force);
        assert!(hint.table.is_none());

        let hint = ParallelHint::for_table("users", 2);
        assert_eq!(hint.workers, 2);
        assert_eq!(hint.table, Some("users".to_string()));
    }

    #[test]
    fn test_optimizer_goal_display() {
        assert_eq!(OptimizerGoal::FirstRows.to_string(), "FIRST_ROWS");
        assert_eq!(OptimizerGoal::FirstRowsN(100).to_string(), "FIRST_ROWS_100");
        assert_eq!(OptimizerGoal::AllRows.to_string(), "ALL_ROWS");
    }

    #[test]
    fn test_hint_collection() {
        let mut collection = HintCollection::new();

        collection.add(QueryHint::ScanMethod(ScanMethodHint {
            table: "users".to_string(),
            method: ScanMethod::IndexScan,
        }));

        collection.add(QueryHint::OptimizerGoal(OptimizerGoal::FirstRows));

        assert_eq!(
            collection.get_scan_method("users"),
            Some(ScanMethod::IndexScan)
        );
        assert_eq!(
            collection.get_optimizer_goal(),
            Some(OptimizerGoal::FirstRows)
        );
    }

    #[test]
    fn test_hint_collection_index() {
        let mut collection = HintCollection::new();

        let hint = IndexHint::new("users").with_index("idx_email");
        collection.add(QueryHint::UseIndex(hint));

        let hints = collection.get_index_hints("users").unwrap();
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].indexes, vec!["idx_email"]);
    }

    #[test]
    fn test_parse_simple_hints() {
        let comment = "/*+ FIRST_ROWS ALL_ROWS */";
        let collection = parse_hints(comment);

        // Last one wins
        assert_eq!(
            collection.get_optimizer_goal(),
            Some(OptimizerGoal::AllRows)
        );
    }

    #[test]
    fn test_parse_no_hints() {
        let comment = "/*+ NO_SEQSCAN NO_HASHJOIN */";
        let collection = parse_hints(comment);

        assert!(!collection.is_empty());
    }

    #[test]
    fn test_parse_structured_hints() {
        let comment = "/*+ UseIndex(users, idx_email) PARALLEL(4) */";
        let collection = parse_hints(comment);

        let hints = collection.get_index_hints("users").unwrap();
        assert_eq!(hints[0].indexes, vec!["idx_email"]);

        assert_eq!(collection.get_parallel_workers(None), Some(4));
    }

    #[test]
    fn test_parse_leading() {
        let comment = "/*+ LEADING(a, b, c) */";
        let collection = parse_hints(comment);

        let leading = collection.get_leading().unwrap();
        assert_eq!(
            leading,
            &vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_extract_hints() {
        let sql = "SELECT /*+ FIRST_ROWS */ * FROM users";
        let (hints, _) = extract_hints(sql).unwrap();

        assert_eq!(hints.get_optimizer_goal(), Some(OptimizerGoal::FirstRows));
    }

    #[test]
    fn test_planner_settings_default() {
        let settings = PlannerSettings::default_all_enabled();
        assert!(settings.enable_seqscan);
        assert!(settings.enable_indexscan);
        assert!(settings.enable_hashjoin);
    }

    #[test]
    fn test_planner_settings_apply_hints() {
        let mut settings = PlannerSettings::default_all_enabled();

        let mut hints = HintCollection::new();
        hints.add(QueryHint::ScanMethod(ScanMethodHint {
            table: String::new(),
            method: ScanMethod::NoSeqScan,
        }));
        hints.add(QueryHint::JoinMethod(JoinMethodHint {
            tables: Vec::new(),
            method: JoinMethod::NoHashJoin,
        }));
        hints.add(QueryHint::OptimizerGoal(OptimizerGoal::FirstRows));

        settings.apply_hints(&hints);

        assert!(!settings.enable_seqscan);
        assert!(!settings.enable_hashjoin);
        assert_eq!(settings.optimizer_goal, OptimizerGoal::FirstRows);
    }

    #[test]
    fn test_row_estimate_hint() {
        let hint = RowEstimateHint::absolute("subquery", 1000);
        assert_eq!(hint.target, "subquery");
        assert_eq!(hint.rows, 1000);
        assert!(hint.scale.is_none());

        let hint = RowEstimateHint::scaled("subquery", 10.0);
        assert_eq!(hint.scale, Some(10.0));
    }

    #[test]
    fn test_scan_method_display() {
        assert_eq!(ScanMethod::SeqScan.to_string(), "SeqScan");
        assert_eq!(ScanMethod::IndexScan.to_string(), "IndexScan");
        assert_eq!(ScanMethod::NoSeqScan.to_string(), "NoSeqScan");
    }

    #[test]
    fn test_join_method_display() {
        assert_eq!(JoinMethod::NestLoop.to_string(), "NestLoop");
        assert_eq!(JoinMethod::HashJoin.to_string(), "HashJoin");
        assert_eq!(JoinMethod::MergeJoin.to_string(), "MergeJoin");
    }
}
