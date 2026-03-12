//! Database Triggers
//!
//! Provides trigger support for BoyoDB:
//! - BEFORE/AFTER INSERT/UPDATE/DELETE
//! - FOR EACH ROW / FOR EACH STATEMENT
//! - INSTEAD OF triggers (for views)
//! - Trigger ordering and dependencies
//! - OLD/NEW row references
//! - Conditional triggers (WHEN clause)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::procedures::{
    Block, DataType, ExecutionContext, Expression, ProcedureBody, ProcedureError,
    SqlExecutor, Statement, Value,
};

// ============================================================================
// Trigger Types
// ============================================================================

/// Trigger timing
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Trigger level
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerLevel {
    Row,
    Statement,
}

/// Trigger definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerDefinition {
    /// Trigger name
    pub name: String,
    /// Schema
    pub schema: Option<String>,
    /// Target table
    pub table: String,
    /// Timing (BEFORE/AFTER/INSTEAD OF)
    pub timing: TriggerTiming,
    /// Events that fire the trigger
    pub events: Vec<TriggerEvent>,
    /// Row-level or statement-level
    pub level: TriggerLevel,
    /// Columns that trigger UPDATE (empty = all columns)
    pub update_columns: Vec<String>,
    /// WHEN condition
    pub when_condition: Option<Expression>,
    /// Trigger function/body
    pub body: TriggerBody,
    /// Trigger is enabled
    pub enabled: bool,
    /// Execution order (lower = earlier)
    pub priority: i32,
    /// Referencing clause names
    pub referencing: TriggerReferencing,
    /// Deferrable
    pub deferrable: bool,
    /// Initially deferred
    pub initially_deferred: bool,
    /// Created timestamp
    pub created_at: i64,
    /// Owner
    pub owner: String,
    /// Comment
    pub comment: Option<String>,
}

/// Trigger body
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TriggerBody {
    /// Inline PL/SQL block
    Block(Block),
    /// Reference to a procedure
    Procedure { name: String, args: Vec<Expression> },
    /// Simple SQL statement
    Sql(String),
}

/// Referencing clause for OLD/NEW
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TriggerReferencing {
    /// Name for OLD row (default: OLD)
    pub old_row: Option<String>,
    /// Name for NEW row (default: NEW)
    pub new_row: Option<String>,
    /// Name for OLD TABLE
    pub old_table: Option<String>,
    /// Name for NEW TABLE
    pub new_table: Option<String>,
}

// ============================================================================
// Trigger Context
// ============================================================================

/// Context for trigger execution
#[derive(Clone, Debug)]
pub struct TriggerContext {
    /// Trigger being executed
    pub trigger_name: String,
    /// Table being modified
    pub table_name: String,
    /// Event that fired the trigger
    pub event: TriggerEvent,
    /// OLD row values (for UPDATE/DELETE)
    pub old_row: Option<HashMap<String, Value>>,
    /// NEW row values (for INSERT/UPDATE)
    pub new_row: Option<HashMap<String, Value>>,
    /// Column names
    pub columns: Vec<String>,
    /// Columns being updated (for UPDATE)
    pub updated_columns: Vec<String>,
    /// Transaction ID
    pub transaction_id: Option<u64>,
    /// Statement ID within transaction
    pub statement_id: u64,
    /// Trigger depth (for recursive triggers)
    pub depth: usize,
    /// Whether this is a nested trigger call
    pub is_nested: bool,
}

impl TriggerContext {
    pub fn new(
        trigger_name: String,
        table_name: String,
        event: TriggerEvent,
        columns: Vec<String>,
    ) -> Self {
        Self {
            trigger_name,
            table_name,
            event,
            old_row: None,
            new_row: None,
            columns,
            updated_columns: Vec::new(),
            transaction_id: None,
            statement_id: 0,
            depth: 0,
            is_nested: false,
        }
    }

    pub fn with_old_row(mut self, row: HashMap<String, Value>) -> Self {
        self.old_row = Some(row);
        self
    }

    pub fn with_new_row(mut self, row: HashMap<String, Value>) -> Self {
        self.new_row = Some(row);
        self
    }

    pub fn with_updated_columns(mut self, columns: Vec<String>) -> Self {
        self.updated_columns = columns;
        self
    }
}

/// Result of trigger execution
#[derive(Clone, Debug)]
pub enum TriggerResult {
    /// Continue with the operation
    Continue,
    /// Continue with modified NEW row
    ModifiedRow(HashMap<String, Value>),
    /// Skip this row (BEFORE trigger only)
    Skip,
    /// Abort the entire operation
    Abort(String),
}

// ============================================================================
// Trigger Executor
// ============================================================================

/// Configuration for trigger execution
#[derive(Clone, Debug)]
pub struct TriggerExecutorConfig {
    /// Maximum trigger recursion depth
    pub max_depth: usize,
    /// Timeout per trigger in milliseconds
    pub timeout_ms: u64,
    /// Enable recursive triggers
    pub allow_recursive: bool,
    /// Enable nested triggers
    pub allow_nested: bool,
}

impl Default for TriggerExecutorConfig {
    fn default() -> Self {
        Self {
            max_depth: 32,
            timeout_ms: 30000,
            allow_recursive: true,
            allow_nested: true,
        }
    }
}

/// Trigger executor
pub struct TriggerExecutor<E: SqlExecutor> {
    sql_executor: Arc<E>,
    config: TriggerExecutorConfig,
}

impl<E: SqlExecutor> TriggerExecutor<E> {
    pub fn new(sql_executor: Arc<E>, config: TriggerExecutorConfig) -> Self {
        Self { sql_executor, config }
    }

    /// Execute a trigger
    pub fn execute(
        &self,
        trigger: &TriggerDefinition,
        ctx: &TriggerContext,
    ) -> Result<TriggerResult, ProcedureError> {
        // Check recursion depth
        if ctx.depth > self.config.max_depth {
            return Err(ProcedureError::MaxRecursionExceeded(ctx.depth));
        }

        // Check WHEN condition
        if let Some(ref when_expr) = trigger.when_condition {
            let when_ctx = self.build_when_context(ctx);
            if !self.evaluate_when(when_expr, &when_ctx)? {
                return Ok(TriggerResult::Continue);
            }
        }

        // Execute trigger body
        match &trigger.body {
            TriggerBody::Block(block) => {
                self.execute_block(block, ctx)
            }
            TriggerBody::Procedure { name, args: _ } => {
                // Would call the procedure
                let _ = name;
                Ok(TriggerResult::Continue)
            }
            TriggerBody::Sql(sql) => {
                let result = self.sql_executor.execute(sql, &[])?;
                let _ = result;
                Ok(TriggerResult::Continue)
            }
        }
    }

    fn build_when_context(&self, ctx: &TriggerContext) -> WhenContext {
        WhenContext {
            old_row: ctx.old_row.clone(),
            new_row: ctx.new_row.clone(),
            event: ctx.event.clone(),
        }
    }

    fn evaluate_when(&self, expr: &Expression, ctx: &WhenContext) -> Result<bool, ProcedureError> {
        // Simplified expression evaluation for WHEN clause
        match expr {
            Expression::BinaryOp { left, op, right } => {
                let lval = self.evaluate_when_expr(left, ctx)?;
                let rval = self.evaluate_when_expr(right, ctx)?;

                use crate::procedures::BinaryOperator;
                match op {
                    BinaryOperator::Equal => Ok(lval == rval),
                    BinaryOperator::NotEqual => Ok(lval != rval),
                    BinaryOperator::And => {
                        let l = lval.to_bool().unwrap_or(false);
                        let r = rval.to_bool().unwrap_or(false);
                        Ok(l && r)
                    }
                    BinaryOperator::Or => {
                        let l = lval.to_bool().unwrap_or(false);
                        let r = rval.to_bool().unwrap_or(false);
                        Ok(l || r)
                    }
                    _ => Ok(false),
                }
            }
            Expression::FieldAccess { record, field } => {
                if let Expression::Variable(name) = record.as_ref() {
                    let row = match name.to_uppercase().as_str() {
                        "OLD" => &ctx.old_row,
                        "NEW" => &ctx.new_row,
                        _ => return Ok(false),
                    };
                    if let Some(r) = row {
                        return Ok(r.get(field).map(|v| !v.is_null()).unwrap_or(false));
                    }
                }
                Ok(false)
            }
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_when_expr(expr, ctx)?;
                let is_null = val.is_null();
                Ok(if *negated { !is_null } else { is_null })
            }
            _ => Ok(true),
        }
    }

    fn evaluate_when_expr(&self, expr: &Expression, ctx: &WhenContext) -> Result<Value, ProcedureError> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::FieldAccess { record, field } => {
                if let Expression::Variable(name) = record.as_ref() {
                    let row = match name.to_uppercase().as_str() {
                        "OLD" => &ctx.old_row,
                        "NEW" => &ctx.new_row,
                        _ => return Ok(Value::Null),
                    };
                    if let Some(r) = row {
                        return Ok(r.get(field).cloned().unwrap_or(Value::Null));
                    }
                }
                Ok(Value::Null)
            }
            _ => Ok(Value::Null),
        }
    }

    fn execute_block(&self, block: &Block, ctx: &TriggerContext) -> Result<TriggerResult, ProcedureError> {
        let mut modified_row = ctx.new_row.clone();

        for stmt in &block.statements {
            match stmt {
                Statement::Return { value: _ } => {
                    // RETURN in trigger means skip the row
                    return Ok(TriggerResult::Skip);
                }
                Statement::Raise { exception, message, .. } => {
                    let msg = format!("{}: {:?}",
                        exception.as_ref().map(|s| s.as_str()).unwrap_or("ERROR"),
                        message
                    );
                    return Ok(TriggerResult::Abort(msg));
                }
                Statement::Assignment { target, value } => {
                    // Handle NEW.column := value assignments
                    if let crate::procedures::AssignmentTarget::RecordField { record, field } = target {
                        if record.to_uppercase() == "NEW" {
                            if let Some(ref mut row) = modified_row {
                                // Evaluate expression (simplified)
                                let val = self.evaluate_assignment_value(value, ctx)?;
                                row.insert(field.clone(), val);
                            }
                        }
                    }
                }
                Statement::ExecuteSql { sql, .. } => {
                    let expanded = self.expand_row_references(sql, ctx);
                    self.sql_executor.execute(&expanded, &[])?;
                }
                _ => {
                    // Other statements handled by procedure executor
                }
            }
        }

        if let Some(row) = modified_row {
            if Some(&row) != ctx.new_row.as_ref() {
                return Ok(TriggerResult::ModifiedRow(row));
            }
        }

        Ok(TriggerResult::Continue)
    }

    fn evaluate_assignment_value(&self, expr: &Expression, ctx: &TriggerContext) -> Result<Value, ProcedureError> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::FieldAccess { record, field } => {
                if let Expression::Variable(name) = record.as_ref() {
                    let row = match name.to_uppercase().as_str() {
                        "OLD" => &ctx.old_row,
                        "NEW" => &ctx.new_row,
                        _ => return Ok(Value::Null),
                    };
                    if let Some(r) = row {
                        return Ok(r.get(field).cloned().unwrap_or(Value::Null));
                    }
                }
                Ok(Value::Null)
            }
            Expression::FunctionCall { name, arguments: _ } => {
                // Handle common trigger functions
                match name.to_uppercase().as_str() {
                    "NOW" | "CURRENT_TIMESTAMP" => {
                        Ok(Value::Timestamp(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_micros() as i64
                        ))
                    }
                    "CURRENT_USER" | "SESSION_USER" => {
                        Ok(Value::String("system".to_string()))
                    }
                    _ => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn expand_row_references(&self, sql: &str, ctx: &TriggerContext) -> String {
        let mut result = sql.to_string();

        // Replace :OLD.column with actual values
        if let Some(ref old_row) = ctx.old_row {
            for (col, val) in old_row {
                let placeholder = format!(":OLD.{}", col);
                let value_str = self.value_to_sql_literal(val);
                result = result.replace(&placeholder, &value_str);
            }
        }

        // Replace :NEW.column with actual values
        if let Some(ref new_row) = ctx.new_row {
            for (col, val) in new_row {
                let placeholder = format!(":NEW.{}", col);
                let value_str = self.value_to_sql_literal(val);
                result = result.replace(&placeholder, &value_str);
            }
        }

        result
    }

    fn value_to_sql_literal(&self, val: &Value) -> String {
        match val {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(d) => d.to_string(),
            Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
            _ => "NULL".to_string(),
        }
    }
}

struct WhenContext {
    old_row: Option<HashMap<String, Value>>,
    new_row: Option<HashMap<String, Value>>,
    event: TriggerEvent,
}

// ============================================================================
// Trigger Manager
// ============================================================================

/// Manages triggers for all tables
pub struct TriggerManager {
    /// Triggers by table: schema.table -> triggers
    triggers: RwLock<HashMap<String, Vec<TriggerDefinition>>>,
    /// Global trigger enable/disable
    enabled: RwLock<bool>,
    /// Execution statistics
    stats: RwLock<TriggerStats>,
}

/// Trigger execution statistics
#[derive(Clone, Debug, Default)]
pub struct TriggerStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub skipped_rows: u64,
    pub aborted_operations: u64,
    pub total_execution_time_us: u64,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            enabled: RwLock::new(true),
            stats: RwLock::new(TriggerStats::default()),
        }
    }

    /// Register a trigger
    pub fn register(&self, trigger: TriggerDefinition) -> Result<(), ProcedureError> {
        let table_key = if let Some(ref schema) = trigger.schema {
            format!("{}.{}", schema, trigger.table)
        } else {
            trigger.table.clone()
        };

        let mut triggers = self.triggers.write().unwrap();
        let table_triggers = triggers.entry(table_key).or_insert_with(Vec::new);

        // Check for duplicate name
        if table_triggers.iter().any(|t| t.name == trigger.name) {
            return Err(ProcedureError::InvalidParameter(
                format!("trigger {} already exists", trigger.name)
            ));
        }

        // Insert in priority order
        let pos = table_triggers.iter()
            .position(|t| t.priority > trigger.priority)
            .unwrap_or(table_triggers.len());

        table_triggers.insert(pos, trigger);
        Ok(())
    }

    /// Drop a trigger
    pub fn drop(&self, schema: Option<&str>, table: &str, name: &str) -> bool {
        let table_key = if let Some(s) = schema {
            format!("{}.{}", s, table)
        } else {
            table.to_string()
        };

        let mut triggers = self.triggers.write().unwrap();
        if let Some(table_triggers) = triggers.get_mut(&table_key) {
            let len_before = table_triggers.len();
            table_triggers.retain(|t| t.name != name);
            return table_triggers.len() < len_before;
        }
        false
    }

    /// Get triggers for a table and event
    pub fn get_triggers(
        &self,
        schema: Option<&str>,
        table: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
    ) -> Vec<TriggerDefinition> {
        if !*self.enabled.read().unwrap() {
            return Vec::new();
        }

        let table_key = if let Some(s) = schema {
            format!("{}.{}", s, table)
        } else {
            table.to_string()
        };

        let triggers = self.triggers.read().unwrap();
        triggers.get(&table_key)
            .map(|ts| {
                ts.iter()
                    .filter(|t| {
                        t.enabled &&
                        t.timing == timing &&
                        t.events.contains(&event)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Enable/disable a trigger
    pub fn set_enabled(&self, schema: Option<&str>, table: &str, name: &str, enabled: bool) -> bool {
        let table_key = if let Some(s) = schema {
            format!("{}.{}", s, table)
        } else {
            table.to_string()
        };

        let mut triggers = self.triggers.write().unwrap();
        if let Some(table_triggers) = triggers.get_mut(&table_key) {
            if let Some(trigger) = table_triggers.iter_mut().find(|t| t.name == name) {
                trigger.enabled = enabled;
                return true;
            }
        }
        false
    }

    /// Enable/disable all triggers
    pub fn set_all_enabled(&self, enabled: bool) {
        *self.enabled.write().unwrap() = enabled;
    }

    /// Execute triggers for an operation
    pub fn execute_triggers<E: SqlExecutor>(
        &self,
        executor: &TriggerExecutor<E>,
        schema: Option<&str>,
        table: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
        ctx: &TriggerContext,
    ) -> Result<TriggerResult, ProcedureError> {
        let triggers = self.get_triggers(schema, table, timing, event);

        if triggers.is_empty() {
            return Ok(TriggerResult::Continue);
        }

        let start = Instant::now();
        let mut current_ctx = ctx.clone();
        let mut final_result = TriggerResult::Continue;

        for trigger in triggers {
            let result = executor.execute(&trigger, &current_ctx);

            let mut stats = self.stats.write().unwrap();
            stats.total_executions += 1;

            match result {
                Ok(TriggerResult::Continue) => {
                    stats.successful_executions += 1;
                }
                Ok(TriggerResult::ModifiedRow(row)) => {
                    stats.successful_executions += 1;
                    current_ctx.new_row = Some(row.clone());
                    final_result = TriggerResult::ModifiedRow(row);
                }
                Ok(TriggerResult::Skip) => {
                    stats.skipped_rows += 1;
                    return Ok(TriggerResult::Skip);
                }
                Ok(TriggerResult::Abort(msg)) => {
                    stats.aborted_operations += 1;
                    return Ok(TriggerResult::Abort(msg));
                }
                Err(e) => {
                    stats.failed_executions += 1;
                    return Err(e);
                }
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.write().unwrap().total_execution_time_us += elapsed;

        Ok(final_result)
    }

    /// Get trigger statistics
    pub fn stats(&self) -> TriggerStats {
        self.stats.read().unwrap().clone()
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        *self.stats.write().unwrap() = TriggerStats::default();
    }

    /// List all triggers for a table
    pub fn list_triggers(&self, schema: Option<&str>, table: &str) -> Vec<TriggerDefinition> {
        let table_key = if let Some(s) = schema {
            format!("{}.{}", s, table)
        } else {
            table.to_string()
        };

        let triggers = self.triggers.read().unwrap();
        triggers.get(&table_key).cloned().unwrap_or_default()
    }

    /// List all triggers
    pub fn list_all(&self) -> Vec<(String, TriggerDefinition)> {
        let triggers = self.triggers.read().unwrap();
        let mut result = Vec::new();

        for (table, trigs) in triggers.iter() {
            for trig in trigs {
                result.push((table.clone(), trig.clone()));
            }
        }

        result
    }
}

impl Default for TriggerManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Trigger Builder (Fluent API)
// ============================================================================

/// Builder for creating triggers
pub struct TriggerBuilder {
    name: String,
    schema: Option<String>,
    table: String,
    timing: TriggerTiming,
    events: Vec<TriggerEvent>,
    level: TriggerLevel,
    update_columns: Vec<String>,
    when_condition: Option<Expression>,
    body: Option<TriggerBody>,
    enabled: bool,
    priority: i32,
    referencing: TriggerReferencing,
    deferrable: bool,
    initially_deferred: bool,
    owner: String,
    comment: Option<String>,
}

impl TriggerBuilder {
    pub fn new(name: &str, table: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: None,
            table: table.to_string(),
            timing: TriggerTiming::After,
            events: Vec::new(),
            level: TriggerLevel::Row,
            update_columns: Vec::new(),
            when_condition: None,
            body: None,
            enabled: true,
            priority: 0,
            referencing: TriggerReferencing::default(),
            deferrable: false,
            initially_deferred: false,
            owner: "system".to_string(),
            comment: None,
        }
    }

    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    pub fn before(mut self) -> Self {
        self.timing = TriggerTiming::Before;
        self
    }

    pub fn after(mut self) -> Self {
        self.timing = TriggerTiming::After;
        self
    }

    pub fn instead_of(mut self) -> Self {
        self.timing = TriggerTiming::InsteadOf;
        self
    }

    pub fn on_insert(mut self) -> Self {
        self.events.push(TriggerEvent::Insert);
        self
    }

    pub fn on_update(mut self) -> Self {
        self.events.push(TriggerEvent::Update);
        self
    }

    pub fn on_delete(mut self) -> Self {
        self.events.push(TriggerEvent::Delete);
        self
    }

    pub fn on_truncate(mut self) -> Self {
        self.events.push(TriggerEvent::Truncate);
        self
    }

    pub fn for_each_row(mut self) -> Self {
        self.level = TriggerLevel::Row;
        self
    }

    pub fn for_each_statement(mut self) -> Self {
        self.level = TriggerLevel::Statement;
        self
    }

    pub fn update_of(mut self, columns: Vec<&str>) -> Self {
        self.update_columns = columns.into_iter().map(String::from).collect();
        self
    }

    pub fn when(mut self, condition: Expression) -> Self {
        self.when_condition = Some(condition);
        self
    }

    pub fn execute_block(mut self, block: Block) -> Self {
        self.body = Some(TriggerBody::Block(block));
        self
    }

    pub fn execute_procedure(mut self, name: &str) -> Self {
        self.body = Some(TriggerBody::Procedure {
            name: name.to_string(),
            args: Vec::new(),
        });
        self
    }

    pub fn execute_sql(mut self, sql: &str) -> Self {
        self.body = Some(TriggerBody::Sql(sql.to_string()));
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    pub fn referencing_old(mut self, name: &str) -> Self {
        self.referencing.old_row = Some(name.to_string());
        self
    }

    pub fn referencing_new(mut self, name: &str) -> Self {
        self.referencing.new_row = Some(name.to_string());
        self
    }

    pub fn deferrable(mut self) -> Self {
        self.deferrable = true;
        self
    }

    pub fn initially_deferred(mut self) -> Self {
        self.initially_deferred = true;
        self.deferrable = true;
        self
    }

    pub fn owner(mut self, owner: &str) -> Self {
        self.owner = owner.to_string();
        self
    }

    pub fn comment(mut self, comment: &str) -> Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn build(self) -> Result<TriggerDefinition, ProcedureError> {
        if self.events.is_empty() {
            return Err(ProcedureError::InvalidParameter(
                "trigger must have at least one event".into()
            ));
        }

        let body = self.body.ok_or_else(|| {
            ProcedureError::InvalidParameter("trigger must have a body".into())
        })?;

        Ok(TriggerDefinition {
            name: self.name,
            schema: self.schema,
            table: self.table,
            timing: self.timing,
            events: self.events,
            level: self.level,
            update_columns: self.update_columns,
            when_condition: self.when_condition,
            body,
            enabled: self.enabled,
            priority: self.priority,
            referencing: self.referencing,
            deferrable: self.deferrable,
            initially_deferred: self.initially_deferred,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            owner: self.owner,
            comment: self.comment,
        })
    }
}

// ============================================================================
// Audit Trigger Helpers
// ============================================================================

/// Create a standard audit trigger
pub fn create_audit_trigger(
    table: &str,
    audit_table: &str,
) -> Result<TriggerDefinition, ProcedureError> {
    let sql = format!(
        "INSERT INTO {} (table_name, operation, old_data, new_data, changed_at, changed_by) \
         VALUES ('{}', TG_OP, row_to_json(OLD), row_to_json(NEW), NOW(), CURRENT_USER)",
        audit_table, table
    );

    TriggerBuilder::new(&format!("{}_audit", table), table)
        .after()
        .on_insert()
        .on_update()
        .on_delete()
        .for_each_row()
        .execute_sql(&sql)
        .comment(&format!("Audit trigger for {}", table))
        .build()
}

/// Create an updated_at timestamp trigger
pub fn create_updated_at_trigger(table: &str) -> Result<TriggerDefinition, ProcedureError> {
    TriggerBuilder::new(&format!("{}_updated_at", table), table)
        .before()
        .on_update()
        .for_each_row()
        .execute_block(Block {
            label: None,
            declarations: vec![],
            statements: vec![
                Statement::Assignment {
                    target: crate::procedures::AssignmentTarget::RecordField {
                        record: "NEW".to_string(),
                        field: "updated_at".to_string(),
                    },
                    value: Expression::FunctionCall {
                        name: "NOW".to_string(),
                        arguments: vec![],
                    },
                },
            ],
            exception_handlers: vec![],
        })
        .comment(&format!("Auto-update updated_at for {}", table))
        .build()
}

/// Create a soft delete trigger (prevent actual deletion)
pub fn create_soft_delete_trigger(table: &str) -> Result<TriggerDefinition, ProcedureError> {
    let sql = format!(
        "UPDATE {} SET deleted_at = NOW(), is_deleted = TRUE WHERE id = OLD.id",
        table
    );

    TriggerBuilder::new(&format!("{}_soft_delete", table), table)
        .instead_of()
        .on_delete()
        .for_each_row()
        .execute_sql(&sql)
        .comment(&format!("Soft delete trigger for {}", table))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockSqlExecutor;

    impl SqlExecutor for MockSqlExecutor {
        fn execute(&self, _sql: &str, _params: &[Value]) -> Result<crate::procedures::SqlResult, ProcedureError> {
            Ok(crate::procedures::SqlResult {
                rows_affected: 1,
                returning: vec![],
            })
        }

        fn execute_query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Vec<Value>>, ProcedureError> {
            Ok(vec![])
        }
    }

    #[test]
    fn test_trigger_builder() {
        let trigger = TriggerBuilder::new("test_trigger", "users")
            .schema("public")
            .before()
            .on_insert()
            .on_update()
            .for_each_row()
            .execute_sql("SELECT 1")
            .priority(10)
            .build()
            .unwrap();

        assert_eq!(trigger.name, "test_trigger");
        assert_eq!(trigger.table, "users");
        assert_eq!(trigger.timing, TriggerTiming::Before);
        assert!(trigger.events.contains(&TriggerEvent::Insert));
        assert!(trigger.events.contains(&TriggerEvent::Update));
        assert_eq!(trigger.level, TriggerLevel::Row);
    }

    #[test]
    fn test_trigger_manager() {
        let manager = TriggerManager::new();

        let trigger = TriggerBuilder::new("insert_log", "orders")
            .after()
            .on_insert()
            .for_each_row()
            .execute_sql("INSERT INTO order_log (order_id) VALUES (NEW.id)")
            .build()
            .unwrap();

        manager.register(trigger).unwrap();

        let triggers = manager.get_triggers(
            None, "orders",
            TriggerTiming::After,
            TriggerEvent::Insert,
        );
        assert_eq!(triggers.len(), 1);

        let all = manager.list_triggers(None, "orders");
        assert_eq!(all.len(), 1);

        assert!(manager.drop(None, "orders", "insert_log"));
        assert!(manager.list_triggers(None, "orders").is_empty());
    }

    #[test]
    fn test_trigger_execution() {
        let executor = TriggerExecutor::new(
            Arc::new(MockSqlExecutor),
            TriggerExecutorConfig::default(),
        );

        let trigger = TriggerBuilder::new("test", "users")
            .before()
            .on_insert()
            .for_each_row()
            .execute_sql("SELECT 1")
            .build()
            .unwrap();

        let mut new_row = HashMap::new();
        new_row.insert("id".to_string(), Value::Integer(1));
        new_row.insert("name".to_string(), Value::String("Alice".to_string()));

        let ctx = TriggerContext::new(
            "test".to_string(),
            "users".to_string(),
            TriggerEvent::Insert,
            vec!["id".to_string(), "name".to_string()],
        ).with_new_row(new_row);

        let result = executor.execute(&trigger, &ctx).unwrap();
        assert!(matches!(result, TriggerResult::Continue));
    }

    #[test]
    fn test_audit_trigger() {
        let trigger = create_audit_trigger("orders", "audit_log").unwrap();

        assert!(trigger.events.contains(&TriggerEvent::Insert));
        assert!(trigger.events.contains(&TriggerEvent::Update));
        assert!(trigger.events.contains(&TriggerEvent::Delete));
        assert_eq!(trigger.timing, TriggerTiming::After);
    }

    #[test]
    fn test_updated_at_trigger() {
        let trigger = create_updated_at_trigger("users").unwrap();

        assert!(trigger.events.contains(&TriggerEvent::Update));
        assert_eq!(trigger.timing, TriggerTiming::Before);
    }
}
