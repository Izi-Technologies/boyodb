//! Enhanced EXPLAIN with Buffers and Timing
//!
//! This module provides detailed query execution plan analysis,
//! including buffer usage, timing information, and cost estimates.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// ============================================================================
// Explain Options
// ============================================================================

/// Options for EXPLAIN output
#[derive(Debug, Clone)]
pub struct ExplainOptions {
    /// Include actual execution times
    pub analyze: bool,
    /// Include buffer usage statistics
    pub buffers: bool,
    /// Include timing breakdown
    pub timing: bool,
    /// Include row estimates vs actual
    pub costs: bool,
    /// Output format
    pub format: ExplainFormat,
    /// Verbose output with extra details
    pub verbose: bool,
    /// Include WAL usage
    pub wal: bool,
    /// Include settings affecting plan
    pub settings: bool,
    /// Summary at end
    pub summary: bool,
}

impl Default for ExplainOptions {
    fn default() -> Self {
        Self {
            analyze: false,
            buffers: false,
            timing: true,
            costs: true,
            format: ExplainFormat::Text,
            verbose: false,
            wal: false,
            settings: false,
            summary: true,
        }
    }
}

/// Output format for EXPLAIN
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
    Xml,
    Yaml,
}

// ============================================================================
// Plan Nodes
// ============================================================================

/// Type of plan node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanNodeType {
    // Scan types
    SeqScan,
    IndexScan,
    IndexOnlyScan,
    BitmapIndexScan,
    BitmapHeapScan,
    TidScan,
    SubqueryScan,
    FunctionScan,
    ValuesScan,
    CteScan,
    WorkTableScan,
    ForeignScan,
    CustomScan,

    // Join types
    NestedLoop,
    MergeJoin,
    HashJoin,

    // Aggregation
    Aggregate,
    GroupAggregate,
    HashAggregate,
    MixedAggregate,
    WindowAgg,

    // Set operations
    Append,
    MergeAppend,
    RecursiveUnion,
    SetOp,

    // Sort/Limit
    Sort,
    IncrementalSort,
    Limit,
    Unique,

    // Misc
    Hash,
    Materialize,
    Result,
    ProjectSet,
    ModifyTable,
    LockRows,
    Gather,
    GatherMerge,
}

impl PlanNodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanNodeType::SeqScan => "Seq Scan",
            PlanNodeType::IndexScan => "Index Scan",
            PlanNodeType::IndexOnlyScan => "Index Only Scan",
            PlanNodeType::BitmapIndexScan => "Bitmap Index Scan",
            PlanNodeType::BitmapHeapScan => "Bitmap Heap Scan",
            PlanNodeType::TidScan => "Tid Scan",
            PlanNodeType::SubqueryScan => "Subquery Scan",
            PlanNodeType::FunctionScan => "Function Scan",
            PlanNodeType::ValuesScan => "Values Scan",
            PlanNodeType::CteScan => "CTE Scan",
            PlanNodeType::WorkTableScan => "WorkTable Scan",
            PlanNodeType::ForeignScan => "Foreign Scan",
            PlanNodeType::CustomScan => "Custom Scan",
            PlanNodeType::NestedLoop => "Nested Loop",
            PlanNodeType::MergeJoin => "Merge Join",
            PlanNodeType::HashJoin => "Hash Join",
            PlanNodeType::Aggregate => "Aggregate",
            PlanNodeType::GroupAggregate => "GroupAggregate",
            PlanNodeType::HashAggregate => "HashAggregate",
            PlanNodeType::MixedAggregate => "MixedAggregate",
            PlanNodeType::WindowAgg => "WindowAgg",
            PlanNodeType::Append => "Append",
            PlanNodeType::MergeAppend => "Merge Append",
            PlanNodeType::RecursiveUnion => "Recursive Union",
            PlanNodeType::SetOp => "SetOp",
            PlanNodeType::Sort => "Sort",
            PlanNodeType::IncrementalSort => "Incremental Sort",
            PlanNodeType::Limit => "Limit",
            PlanNodeType::Unique => "Unique",
            PlanNodeType::Hash => "Hash",
            PlanNodeType::Materialize => "Materialize",
            PlanNodeType::Result => "Result",
            PlanNodeType::ProjectSet => "ProjectSet",
            PlanNodeType::ModifyTable => "ModifyTable",
            PlanNodeType::LockRows => "LockRows",
            PlanNodeType::Gather => "Gather",
            PlanNodeType::GatherMerge => "Gather Merge",
        }
    }
}

// ============================================================================
// Buffer Statistics
// ============================================================================

/// Buffer usage statistics
#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    /// Shared blocks hit (found in cache)
    pub shared_hit: u64,
    /// Shared blocks read from disk
    pub shared_read: u64,
    /// Shared blocks dirtied
    pub shared_dirtied: u64,
    /// Shared blocks written
    pub shared_written: u64,
    /// Local blocks hit
    pub local_hit: u64,
    /// Local blocks read
    pub local_read: u64,
    /// Local blocks dirtied
    pub local_dirtied: u64,
    /// Local blocks written
    pub local_written: u64,
    /// Temp blocks read
    pub temp_read: u64,
    /// Temp blocks written
    pub temp_written: u64,
}

impl BufferStats {
    /// Total blocks accessed
    pub fn total_blocks(&self) -> u64 {
        self.shared_hit + self.shared_read + self.local_hit + self.local_read
    }

    /// Cache hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.shared_hit + self.local_hit;
        let total = self.total_blocks();
        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Merge with another BufferStats
    pub fn merge(&mut self, other: &BufferStats) {
        self.shared_hit += other.shared_hit;
        self.shared_read += other.shared_read;
        self.shared_dirtied += other.shared_dirtied;
        self.shared_written += other.shared_written;
        self.local_hit += other.local_hit;
        self.local_read += other.local_read;
        self.local_dirtied += other.local_dirtied;
        self.local_written += other.local_written;
        self.temp_read += other.temp_read;
        self.temp_written += other.temp_written;
    }
}

// ============================================================================
// Timing Statistics
// ============================================================================

/// Timing breakdown
#[derive(Debug, Clone, Default)]
pub struct TimingStats {
    /// Total execution time
    pub total_time: Duration,
    /// Time for this node only (excluding children)
    pub self_time: Duration,
    /// Time spent in I/O wait
    pub io_time: Duration,
    /// Time spent in CPU
    pub cpu_time: Duration,
    /// Number of loops
    pub loops: u64,
}

impl TimingStats {
    /// Average time per loop
    pub fn avg_time(&self) -> Duration {
        if self.loops == 0 {
            Duration::ZERO
        } else {
            self.total_time / self.loops as u32
        }
    }
}

// ============================================================================
// Plan Node
// ============================================================================

/// A node in the execution plan
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Node type
    pub node_type: PlanNodeType,
    /// Table/index being accessed
    pub relation: Option<String>,
    /// Alias name
    pub alias: Option<String>,
    /// Index name (for index scans)
    pub index_name: Option<String>,
    /// Join type (for joins)
    pub join_type: Option<String>,
    /// Sort keys (for sort)
    pub sort_key: Vec<String>,
    /// Filter condition
    pub filter: Option<String>,
    /// Index condition
    pub index_cond: Option<String>,
    /// Hash condition (for hash join)
    pub hash_cond: Option<String>,
    /// Output columns
    pub output: Vec<String>,
    /// Estimated startup cost
    pub startup_cost: f64,
    /// Estimated total cost
    pub total_cost: f64,
    /// Estimated rows
    pub plan_rows: f64,
    /// Estimated row width
    pub plan_width: u32,
    /// Actual rows (if ANALYZE)
    pub actual_rows: Option<u64>,
    /// Actual loops (if ANALYZE)
    pub actual_loops: Option<u64>,
    /// Timing stats (if ANALYZE + TIMING)
    pub timing: Option<TimingStats>,
    /// Buffer stats (if ANALYZE + BUFFERS)
    pub buffers: Option<BufferStats>,
    /// Child nodes
    pub children: Vec<PlanNode>,
    /// Parallel workers planned
    pub workers_planned: Option<u32>,
    /// Parallel workers launched
    pub workers_launched: Option<u32>,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

impl PlanNode {
    pub fn new(node_type: PlanNodeType) -> Self {
        Self {
            node_type,
            relation: None,
            alias: None,
            index_name: None,
            join_type: None,
            sort_key: Vec::new(),
            filter: None,
            index_cond: None,
            hash_cond: None,
            output: Vec::new(),
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            actual_rows: None,
            actual_loops: None,
            timing: None,
            buffers: None,
            children: Vec::new(),
            workers_planned: None,
            workers_launched: None,
            properties: HashMap::new(),
        }
    }

    /// Set relation
    pub fn with_relation(mut self, relation: &str) -> Self {
        self.relation = Some(relation.to_string());
        self
    }

    /// Set index name
    pub fn with_index(mut self, index: &str) -> Self {
        self.index_name = Some(index.to_string());
        self
    }

    /// Set costs
    pub fn with_costs(mut self, startup: f64, total: f64, rows: f64, width: u32) -> Self {
        self.startup_cost = startup;
        self.total_cost = total;
        self.plan_rows = rows;
        self.plan_width = width;
        self
    }

    /// Set actual stats
    pub fn with_actual(mut self, rows: u64, loops: u64, time: Duration) -> Self {
        self.actual_rows = Some(rows);
        self.actual_loops = Some(loops);
        self.timing = Some(TimingStats {
            total_time: time,
            self_time: time,
            loops,
            ..Default::default()
        });
        self
    }

    /// Add a child node
    pub fn add_child(&mut self, child: PlanNode) {
        self.children.push(child);
    }

    /// Calculate total rows processed
    pub fn total_rows(&self) -> u64 {
        self.actual_rows.unwrap_or(0) * self.actual_loops.unwrap_or(1)
    }

    /// Get total buffer stats including children
    pub fn total_buffers(&self) -> BufferStats {
        let mut stats = self.buffers.clone().unwrap_or_default();
        for child in &self.children {
            stats.merge(&child.total_buffers());
        }
        stats
    }
}

// ============================================================================
// Explain Output
// ============================================================================

/// Execution plan with metadata
#[derive(Debug, Clone)]
pub struct ExplainPlan {
    /// Root node
    pub root: PlanNode,
    /// Planning time
    pub planning_time: Duration,
    /// Execution time (if ANALYZE)
    pub execution_time: Option<Duration>,
    /// Trigger statistics
    pub triggers: Vec<TriggerStats>,
    /// Query text
    pub query: String,
    /// JIT statistics
    pub jit: Option<JitStats>,
    /// Settings that affected the plan
    pub settings: HashMap<String, String>,
}

/// Trigger execution statistics
#[derive(Debug, Clone)]
pub struct TriggerStats {
    pub name: String,
    pub relation: String,
    pub time: Duration,
    pub calls: u64,
}

/// JIT compilation statistics
#[derive(Debug, Clone)]
pub struct JitStats {
    pub functions: u32,
    pub options: JitOptions,
    pub timing: JitTiming,
}

#[derive(Debug, Clone, Default)]
pub struct JitOptions {
    pub inlining: bool,
    pub optimization: bool,
    pub expressions: bool,
    pub deforming: bool,
}

#[derive(Debug, Clone, Default)]
pub struct JitTiming {
    pub generation: Duration,
    pub inlining: Duration,
    pub optimization: Duration,
    pub emission: Duration,
    pub total: Duration,
}

// ============================================================================
// Explain Formatter
// ============================================================================

/// Formats execution plans
pub struct ExplainFormatter {
    options: ExplainOptions,
}

impl ExplainFormatter {
    pub fn new(options: ExplainOptions) -> Self {
        Self { options }
    }

    /// Format plan as text
    pub fn format_text(&self, plan: &ExplainPlan) -> String {
        let mut output = Vec::new();

        self.format_node_text(&plan.root, 0, &mut output);

        if self.options.summary {
            output.push(String::new());
            output.push(format!(
                "Planning Time: {:.3} ms",
                plan.planning_time.as_secs_f64() * 1000.0
            ));

            if let Some(exec_time) = plan.execution_time {
                output.push(format!(
                    "Execution Time: {:.3} ms",
                    exec_time.as_secs_f64() * 1000.0
                ));
            }
        }

        output.join("\n")
    }

    fn format_node_text(&self, node: &PlanNode, indent: usize, output: &mut Vec<String>) {
        let prefix = "  ".repeat(indent);
        let arrow = if indent > 0 { "->  " } else { "" };

        // Node type and relation
        let mut line = format!("{}{}{}", prefix, arrow, node.node_type.as_str());

        if let Some(ref relation) = node.relation {
            line.push_str(&format!(" on {}", relation));
        }

        if let Some(ref index) = node.index_name {
            line.push_str(&format!(" using {}", index));
        }

        if let Some(ref alias) = node.alias {
            if node.relation.as_ref() != Some(alias) {
                line.push_str(&format!(" {}", alias));
            }
        }

        // Costs
        if self.options.costs {
            line.push_str(&format!(
                "  (cost={:.2}..{:.2} rows={:.0} width={})",
                node.startup_cost, node.total_cost, node.plan_rows, node.plan_width
            ));
        }

        // Actual stats
        if self.options.analyze {
            if let (Some(rows), Some(loops)) = (node.actual_rows, node.actual_loops) {
                let time_str = if let Some(ref timing) = node.timing {
                    format!(
                        " time={:.3}..{:.3}",
                        timing.total_time.as_secs_f64() * 1000.0 / loops as f64,
                        timing.total_time.as_secs_f64() * 1000.0 / loops as f64
                    )
                } else {
                    String::new()
                };

                line.push_str(&format!(
                    " (actual rows={}{} loops={})",
                    rows, time_str, loops
                ));
            }
        }

        output.push(line);

        // Index condition
        if let Some(ref cond) = node.index_cond {
            output.push(format!("{}      Index Cond: {}", prefix, cond));
        }

        // Filter
        if let Some(ref filter) = node.filter {
            output.push(format!("{}      Filter: {}", prefix, filter));
        }

        // Hash condition
        if let Some(ref cond) = node.hash_cond {
            output.push(format!("{}      Hash Cond: {}", prefix, cond));
        }

        // Sort key
        if !node.sort_key.is_empty() {
            output.push(format!(
                "{}      Sort Key: {}",
                prefix,
                node.sort_key.join(", ")
            ));
        }

        // Buffers
        if self.options.buffers {
            if let Some(ref buffers) = node.buffers {
                if buffers.shared_hit > 0 || buffers.shared_read > 0 {
                    output.push(format!(
                        "{}      Buffers: shared hit={} read={}",
                        prefix, buffers.shared_hit, buffers.shared_read
                    ));
                }
            }
        }

        // Workers
        if let Some(workers) = node.workers_planned {
            if let Some(launched) = node.workers_launched {
                output.push(format!(
                    "{}      Workers Planned: {}  Workers Launched: {}",
                    prefix, workers, launched
                ));
            }
        }

        // Children
        for child in &node.children {
            self.format_node_text(child, indent + 1, output);
        }
    }

    /// Format plan as JSON
    pub fn format_json(&self, plan: &ExplainPlan) -> String {
        let mut obj = serde_json_lite::Object::new();

        obj.insert("Plan", self.node_to_json(&plan.root));
        obj.insert("Planning Time", plan.planning_time.as_secs_f64() * 1000.0);

        if let Some(exec) = plan.execution_time {
            obj.insert("Execution Time", exec.as_secs_f64() * 1000.0);
        }

        format!("[{}]", obj.to_json_string())
    }

    fn node_to_json(&self, node: &PlanNode) -> serde_json_lite::Value {
        let mut obj = serde_json_lite::Object::new();

        obj.insert("Node Type", node.node_type.as_str());

        if let Some(ref rel) = node.relation {
            obj.insert("Relation Name", rel.clone());
        }

        if let Some(ref idx) = node.index_name {
            obj.insert("Index Name", idx.clone());
        }

        obj.insert("Startup Cost", node.startup_cost);
        obj.insert("Total Cost", node.total_cost);
        obj.insert("Plan Rows", node.plan_rows);
        obj.insert("Plan Width", node.plan_width as f64);

        if let Some(rows) = node.actual_rows {
            obj.insert("Actual Rows", rows as f64);
        }

        if let Some(loops) = node.actual_loops {
            obj.insert("Actual Loops", loops as f64);
        }

        if !node.children.is_empty() {
            let plans: Vec<_> = node.children.iter().map(|c| self.node_to_json(c)).collect();
            obj.insert("Plans", serde_json_lite::Value::Array(plans));
        }

        serde_json_lite::Value::Object(obj)
    }
}

// Simple JSON-lite implementation
mod serde_json_lite {
    use std::collections::HashMap;

    /// A newtype wrapper for JSON objects
    #[derive(Clone, Default)]
    pub struct Object {
        inner: HashMap<&'static str, Value>,
    }

    impl Object {
        pub fn new() -> Self {
            Self {
                inner: HashMap::new(),
            }
        }

        pub fn insert<T: Into<Value>>(&mut self, key: &'static str, value: T) {
            self.inner.insert(key, value.into());
        }

        pub fn to_json_string(&self) -> String {
            let items: Vec<_> = self
                .inner
                .iter()
                .map(|(k, v)| format!("\"{}\": {}", k, v.to_json_string()))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }

    #[derive(Clone)]
    pub enum Value {
        Null,
        Bool(bool),
        Number(f64),
        String(String),
        Array(Vec<Value>),
        Object(Object),
    }

    impl Value {
        pub fn to_json_string(&self) -> String {
            match self {
                Value::Null => "null".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => format!("{:.2}", n),
                Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
                Value::Array(arr) => {
                    let items: Vec<_> = arr.iter().map(|v| v.to_json_string()).collect();
                    format!("[{}]", items.join(", "))
                }
                Value::Object(obj) => obj.to_json_string(),
            }
        }
    }

    impl From<&str> for Value {
        fn from(s: &str) -> Self {
            Value::String(s.to_string())
        }
    }

    impl From<String> for Value {
        fn from(s: String) -> Self {
            Value::String(s)
        }
    }

    impl From<f64> for Value {
        fn from(n: f64) -> Self {
            Value::Number(n)
        }
    }

    impl From<bool> for Value {
        fn from(b: bool) -> Self {
            Value::Bool(b)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_node_type() {
        assert_eq!(PlanNodeType::SeqScan.as_str(), "Seq Scan");
        assert_eq!(PlanNodeType::HashJoin.as_str(), "Hash Join");
    }

    #[test]
    fn test_buffer_stats() {
        let mut stats = BufferStats {
            shared_hit: 100,
            shared_read: 50,
            ..Default::default()
        };

        assert_eq!(stats.total_blocks(), 150);
        assert!((stats.hit_ratio() - 0.666).abs() < 0.01);

        let other = BufferStats {
            shared_hit: 50,
            shared_read: 25,
            ..Default::default()
        };

        stats.merge(&other);
        assert_eq!(stats.shared_hit, 150);
        assert_eq!(stats.shared_read, 75);
    }

    #[test]
    fn test_timing_stats() {
        let timing = TimingStats {
            total_time: Duration::from_millis(100),
            loops: 10,
            ..Default::default()
        };

        assert_eq!(timing.avg_time(), Duration::from_millis(10));
    }

    #[test]
    fn test_plan_node() {
        let node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64)
            .with_actual(950, 1, Duration::from_millis(50));

        assert_eq!(node.relation, Some("users".to_string()));
        assert_eq!(node.total_rows(), 950);
    }

    #[test]
    fn test_plan_node_children() {
        let mut parent = PlanNode::new(PlanNodeType::HashJoin);

        let left = PlanNode::new(PlanNodeType::SeqScan).with_relation("users");
        let right = PlanNode::new(PlanNodeType::IndexScan).with_relation("orders");

        parent.add_child(left);
        parent.add_child(right);

        assert_eq!(parent.children.len(), 2);
    }

    #[test]
    fn test_explain_formatter_text() {
        let formatter = ExplainFormatter::new(ExplainOptions::default());

        let node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64);

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: None,
            triggers: Vec::new(),
            query: "SELECT * FROM users".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("Seq Scan on users"));
        assert!(output.contains("cost=0.00..100.00"));
        assert!(output.contains("Planning Time:"));
    }

    #[test]
    fn test_explain_formatter_analyze() {
        let formatter = ExplainFormatter::new(ExplainOptions {
            analyze: true,
            ..Default::default()
        });

        let node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64)
            .with_actual(950, 1, Duration::from_millis(10));

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: Some(Duration::from_millis(15)),
            triggers: Vec::new(),
            query: "SELECT * FROM users".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("actual rows=950"));
        assert!(output.contains("loops=1"));
        assert!(output.contains("Execution Time:"));
    }

    #[test]
    fn test_explain_formatter_buffers() {
        let formatter = ExplainFormatter::new(ExplainOptions {
            analyze: true,
            buffers: true,
            ..Default::default()
        });

        let mut node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64)
            .with_actual(950, 1, Duration::from_millis(10));

        node.buffers = Some(BufferStats {
            shared_hit: 100,
            shared_read: 50,
            ..Default::default()
        });

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: Some(Duration::from_millis(15)),
            triggers: Vec::new(),
            query: "SELECT * FROM users".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("Buffers: shared hit=100 read=50"));
    }

    #[test]
    fn test_explain_nested_plan() {
        let formatter = ExplainFormatter::new(ExplainOptions::default());

        let mut hash_join = PlanNode::new(PlanNodeType::HashJoin);
        hash_join.hash_cond = Some("users.id = orders.user_id".to_string());

        let seq_scan = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 50.0, 100.0, 32);

        let mut hash = PlanNode::new(PlanNodeType::Hash);
        let index_scan = PlanNode::new(PlanNodeType::IndexScan)
            .with_relation("orders")
            .with_index("orders_user_id_idx")
            .with_costs(0.0, 80.0, 200.0, 48);
        hash.add_child(index_scan);

        hash_join.add_child(seq_scan);
        hash_join.add_child(hash);

        let plan = ExplainPlan {
            root: hash_join,
            planning_time: Duration::from_millis(1),
            execution_time: None,
            triggers: Vec::new(),
            query: "SELECT * FROM users JOIN orders ON users.id = orders.user_id".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("Hash Join"));
        assert!(output.contains("Seq Scan on users"));
        assert!(output.contains("Index Scan on orders using orders_user_id_idx"));
        assert!(output.contains("Hash Cond:"));
    }

    #[test]
    fn test_explain_format_json() {
        let formatter = ExplainFormatter::new(ExplainOptions {
            format: ExplainFormat::Json,
            ..Default::default()
        });

        let node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64);

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: None,
            triggers: Vec::new(),
            query: "SELECT * FROM users".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_json(&plan);
        assert!(output.contains("\"Node Type\": \"Seq Scan\""));
        assert!(output.contains("\"Relation Name\": \"users\""));
    }

    #[test]
    fn test_total_buffers() {
        let mut parent = PlanNode::new(PlanNodeType::HashJoin);
        parent.buffers = Some(BufferStats {
            shared_hit: 10,
            shared_read: 5,
            ..Default::default()
        });

        let mut child = PlanNode::new(PlanNodeType::SeqScan);
        child.buffers = Some(BufferStats {
            shared_hit: 20,
            shared_read: 10,
            ..Default::default()
        });

        parent.add_child(child);

        let total = parent.total_buffers();
        assert_eq!(total.shared_hit, 30);
        assert_eq!(total.shared_read, 15);
    }

    #[test]
    fn test_filter_output() {
        let formatter = ExplainFormatter::new(ExplainOptions::default());

        let mut node = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 100.0, 64);
        node.filter = Some("age > 18".to_string());

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: None,
            triggers: Vec::new(),
            query: "SELECT * FROM users WHERE age > 18".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("Filter: age > 18"));
    }

    #[test]
    fn test_sort_output() {
        let formatter = ExplainFormatter::new(ExplainOptions::default());

        let mut node = PlanNode::new(PlanNodeType::Sort)
            .with_costs(0.0, 150.0, 1000.0, 64);
        node.sort_key = vec!["name ASC".to_string(), "created_at DESC".to_string()];

        let child = PlanNode::new(PlanNodeType::SeqScan)
            .with_relation("users")
            .with_costs(0.0, 100.0, 1000.0, 64);
        node.add_child(child);

        let plan = ExplainPlan {
            root: node,
            planning_time: Duration::from_micros(500),
            execution_time: None,
            triggers: Vec::new(),
            query: "SELECT * FROM users ORDER BY name, created_at DESC".to_string(),
            jit: None,
            settings: HashMap::new(),
        };

        let output = formatter.format_text(&plan);
        assert!(output.contains("Sort Key: name ASC, created_at DESC"));
    }
}
