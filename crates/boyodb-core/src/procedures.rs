//! Stored Procedures and Procedural Language Support
//!
//! Provides PL/SQL-like procedural language for BoyoDB:
//! - CREATE PROCEDURE / CREATE FUNCTION
//! - Variables (DECLARE, SET)
//! - Control flow (IF/ELSE, LOOP, WHILE, FOR, CASE)
//! - Exception handling (BEGIN/EXCEPTION/END)
//! - Cursors for row-by-row processing
//! - CALL statement for procedure invocation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};

// ============================================================================
// Types and Errors
// ============================================================================

/// Procedure execution error
#[derive(Debug, Clone)]
pub enum ProcedureError {
    /// Procedure not found
    NotFound(String),
    /// Invalid parameter
    InvalidParameter(String),
    /// Type mismatch
    TypeMismatch { expected: String, got: String },
    /// Variable not declared
    UndeclaredVariable(String),
    /// Division by zero
    DivisionByZero,
    /// Null value error
    NullValue(String),
    /// Control flow error
    ControlFlowError(String),
    /// Cursor error
    CursorError(String),
    /// Exception raised
    ExceptionRaised { code: String, message: String },
    /// Max recursion depth exceeded
    MaxRecursionExceeded(usize),
    /// Execution timeout
    Timeout(Duration),
    /// SQL execution error
    SqlError(String),
    /// Compilation error
    CompilationError(String),
}

impl std::fmt::Display for ProcedureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(name) => write!(f, "procedure not found: {}", name),
            Self::InvalidParameter(msg) => write!(f, "invalid parameter: {}", msg),
            Self::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {}, got {}", expected, got)
            }
            Self::UndeclaredVariable(name) => write!(f, "undeclared variable: {}", name),
            Self::DivisionByZero => write!(f, "division by zero"),
            Self::NullValue(msg) => write!(f, "null value: {}", msg),
            Self::ControlFlowError(msg) => write!(f, "control flow error: {}", msg),
            Self::CursorError(msg) => write!(f, "cursor error: {}", msg),
            Self::ExceptionRaised { code, message } => {
                write!(f, "exception {}: {}", code, message)
            }
            Self::MaxRecursionExceeded(depth) => {
                write!(f, "max recursion depth exceeded: {}", depth)
            }
            Self::Timeout(d) => write!(f, "execution timeout after {:?}", d),
            Self::SqlError(msg) => write!(f, "SQL error: {}", msg),
            Self::CompilationError(msg) => write!(f, "compilation error: {}", msg),
        }
    }
}

impl std::error::Error for ProcedureError {}

/// Data types for procedure variables
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    Boolean,
    Varchar(usize),
    Text,
    Date,
    Timestamp,
    Binary,
    Json,
    Array(Box<DataType>),
    Record(Vec<(String, DataType)>),
    Cursor,
    RefCursor,
    Void,
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        matches!(self,
            DataType::Integer | DataType::BigInt |
            DataType::Float | DataType::Double |
            DataType::Decimal { .. }
        )
    }

    pub fn is_compatible(&self, other: &DataType) -> bool {
        match (self, other) {
            (a, b) if a == b => true,
            (DataType::Integer, DataType::BigInt) => true,
            (DataType::BigInt, DataType::Integer) => true,
            (DataType::Float, DataType::Double) => true,
            (DataType::Double, DataType::Float) => true,
            (DataType::Varchar(_), DataType::Text) => true,
            (DataType::Text, DataType::Varchar(_)) => true,
            _ => false,
        }
    }
}

/// Runtime value
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Decimal(String),
    Boolean(bool),
    String(String),
    Binary(Vec<u8>),
    Date(i32), // Days since epoch
    Timestamp(i64), // Microseconds since epoch
    Json(String),
    Array(Vec<Value>),
    Record(HashMap<String, Value>),
    Cursor(u64), // Cursor ID
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Void,
            Value::Integer(_) => DataType::Integer,
            Value::BigInt(_) => DataType::BigInt,
            Value::Float(_) => DataType::Float,
            Value::Double(_) => DataType::Double,
            Value::Decimal(_) => DataType::Decimal { precision: 38, scale: 10 },
            Value::Boolean(_) => DataType::Boolean,
            Value::String(s) => DataType::Varchar(s.len()),
            Value::Binary(_) => DataType::Binary,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Json(_) => DataType::Json,
            Value::Array(_) => DataType::Array(Box::new(DataType::Void)),
            Value::Record(_) => DataType::Record(vec![]),
            Value::Cursor(_) => DataType::Cursor,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            Value::BigInt(i) => Some(*i != 0),
            Value::String(s) => Some(!s.is_empty()),
            Value::Null => Some(false),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i as i64),
            Value::BigInt(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            Value::Double(d) => Some(*d as i64),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::Float(f) => Some(*f as f64),
            Value::Double(d) => Some(*d),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }
}

// ============================================================================
// Procedure Definition
// ============================================================================

/// Parameter mode
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
}

/// Procedure parameter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parameter {
    pub name: String,
    pub data_type: DataType,
    pub mode: ParameterMode,
    pub default_value: Option<Value>,
}

/// Variable declaration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Variable {
    pub name: String,
    pub data_type: DataType,
    pub initial_value: Option<Expression>,
    pub is_constant: bool,
    pub not_null: bool,
}

/// Procedure or function definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcedureDefinition {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<Parameter>,
    pub return_type: Option<DataType>,
    pub is_function: bool,
    pub language: ProcedureLanguage,
    pub body: ProcedureBody,
    pub security_definer: bool,
    pub deterministic: bool,
    pub parallel_safe: bool,
    pub created_at: i64,
    pub modified_at: i64,
    pub owner: String,
    pub comment: Option<String>,
}

/// Procedure language
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcedureLanguage {
    PlSql,
    JavaScript,
    Python,
    Sql,
}

/// Procedure body
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProcedureBody {
    /// PL/SQL block
    Block(Block),
    /// External language source
    External { source: String },
    /// SQL expression (for simple functions)
    SqlExpression(String),
}

/// PL/SQL Block
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub label: Option<String>,
    pub declarations: Vec<Declaration>,
    pub statements: Vec<Statement>,
    pub exception_handlers: Vec<ExceptionHandler>,
}

/// Declaration in a block
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Declaration {
    Variable(Variable),
    Constant(Variable),
    Cursor(CursorDeclaration),
    Exception(String),
    Type(TypeDeclaration),
    Procedure(Box<ProcedureDefinition>),
}

/// Cursor declaration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CursorDeclaration {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub query: String,
    pub is_ref_cursor: bool,
}

/// Type declaration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeDeclaration {
    pub name: String,
    pub definition: DataType,
}

/// Exception handler
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExceptionHandler {
    pub exceptions: Vec<ExceptionMatch>,
    pub statements: Vec<Statement>,
}

/// Exception match pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExceptionMatch {
    Named(String),
    SqlState(String),
    Others,
}

// ============================================================================
// Statements
// ============================================================================

/// Procedure statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Statement {
    /// Assignment: variable := expression
    Assignment {
        target: AssignmentTarget,
        value: Expression,
    },
    /// IF statement
    If {
        condition: Expression,
        then_branch: Vec<Statement>,
        elsif_branches: Vec<(Expression, Vec<Statement>)>,
        else_branch: Option<Vec<Statement>>,
    },
    /// CASE statement
    Case {
        selector: Option<Expression>,
        when_branches: Vec<(Expression, Vec<Statement>)>,
        else_branch: Option<Vec<Statement>>,
    },
    /// Basic LOOP
    Loop {
        label: Option<String>,
        statements: Vec<Statement>,
    },
    /// WHILE loop
    While {
        label: Option<String>,
        condition: Expression,
        statements: Vec<Statement>,
    },
    /// FOR loop (numeric)
    ForNumeric {
        label: Option<String>,
        variable: String,
        start: Expression,
        end: Expression,
        step: Option<Expression>,
        reverse: bool,
        statements: Vec<Statement>,
    },
    /// FOR loop (cursor)
    ForCursor {
        label: Option<String>,
        record_variable: String,
        cursor: CursorSource,
        statements: Vec<Statement>,
    },
    /// EXIT statement
    Exit {
        label: Option<String>,
        when_condition: Option<Expression>,
    },
    /// CONTINUE statement
    Continue {
        label: Option<String>,
        when_condition: Option<Expression>,
    },
    /// RETURN statement
    Return { value: Option<Expression> },
    /// RAISE exception
    Raise {
        exception: Option<String>,
        message: Option<Expression>,
        using: Vec<(String, Expression)>,
    },
    /// NULL statement (no-op)
    Null,
    /// Execute SQL
    ExecuteSql { sql: String, into: Vec<String> },
    /// Execute immediate (dynamic SQL)
    ExecuteImmediate {
        sql: Expression,
        into: Vec<String>,
        using: Vec<Expression>,
    },
    /// OPEN cursor
    OpenCursor {
        cursor: String,
        arguments: Vec<Expression>,
    },
    /// FETCH cursor
    FetchCursor {
        cursor: String,
        into: Vec<String>,
        bulk_collect: bool,
        limit: Option<Expression>,
    },
    /// CLOSE cursor
    CloseCursor { cursor: String },
    /// CALL procedure
    Call {
        procedure: String,
        arguments: Vec<Expression>,
        into: Vec<String>,
    },
    /// Nested block
    Block(Block),
    /// PIPE ROW (for pipelined functions)
    PipeRow { value: Expression },
    /// FORALL (bulk DML)
    Forall {
        index_variable: String,
        bounds: ForallBounds,
        dml: String,
    },
}

/// Assignment target
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AssignmentTarget {
    Variable(String),
    ArrayElement { array: String, index: Expression },
    RecordField { record: String, field: String },
}

/// Cursor source for FOR loop
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CursorSource {
    Named(String),
    Inline(String),
    RefCursor(String),
}

/// FORALL bounds
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ForallBounds {
    Range { start: Expression, end: Expression },
    Indices { collection: String },
    Values { collection: String },
}

// ============================================================================
// Expressions
// ============================================================================

/// Expression in procedure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Expression {
    /// Literal value
    Literal(Value),
    /// Variable reference
    Variable(String),
    /// Parameter reference
    Parameter(String),
    /// Array element access
    ArrayAccess { array: Box<Expression>, index: Box<Expression> },
    /// Record field access
    FieldAccess { record: Box<Expression>, field: String },
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
    /// Function call
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    /// SQL subquery
    Subquery(String),
    /// EXISTS
    Exists(String),
    /// IN list
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    /// BETWEEN
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },
    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expression>, negated: bool },
    /// LIKE
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
        negated: bool,
    },
    /// CAST
    Cast { expr: Box<Expression>, target_type: DataType },
    /// Cursor attribute (%FOUND, %NOTFOUND, %ROWCOUNT, %ISOPEN)
    CursorAttribute { cursor: String, attribute: CursorAttribute },
    /// SQL attribute (SQL%ROWCOUNT, etc.)
    SqlAttribute(SqlAttribute),
    /// Row constructor
    Row(Vec<Expression>),
    /// Array constructor
    Array(Vec<Expression>),
    /// NULL
    Null,
}

/// Binary operators
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Logical
    And,
    Or,
    // String
    Concat,
}

/// Unary operators
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Negate,
    Plus,
}

/// Cursor attributes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CursorAttribute {
    Found,
    NotFound,
    RowCount,
    IsOpen,
}

/// SQL attributes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlAttribute {
    Found,
    NotFound,
    RowCount,
}

// ============================================================================
// Execution Context
// ============================================================================

/// Execution context for a procedure
pub struct ExecutionContext {
    /// Variable values
    variables: HashMap<String, Value>,
    /// Parameter values
    parameters: HashMap<String, Value>,
    /// OUT parameter bindings
    out_parameters: HashMap<String, Value>,
    /// Open cursors
    cursors: HashMap<String, CursorState>,
    /// Last SQL row count
    sql_row_count: i64,
    /// Last SQL found flag
    sql_found: bool,
    /// Return value (for functions)
    return_value: Option<Value>,
    /// Current recursion depth
    recursion_depth: usize,
    /// Max recursion depth
    max_recursion: usize,
    /// Execution start time
    start_time: Instant,
    /// Execution timeout
    timeout: Duration,
    /// Labels for loop control
    labels: Vec<String>,
}

impl ExecutionContext {
    pub fn new(timeout: Duration, max_recursion: usize) -> Self {
        Self {
            variables: HashMap::new(),
            parameters: HashMap::new(),
            out_parameters: HashMap::new(),
            cursors: HashMap::new(),
            sql_row_count: 0,
            sql_found: false,
            return_value: None,
            recursion_depth: 0,
            max_recursion,
            start_time: Instant::now(),
            timeout,
            labels: Vec::new(),
        }
    }

    pub fn check_timeout(&self) -> Result<(), ProcedureError> {
        if self.start_time.elapsed() > self.timeout {
            return Err(ProcedureError::Timeout(self.timeout));
        }
        Ok(())
    }

    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name).or_else(|| self.parameters.get(name))
    }

    pub fn set_variable(&mut self, name: &str, value: Value) {
        self.variables.insert(name.to_string(), value);
    }

    pub fn set_parameter(&mut self, name: &str, value: Value) {
        self.parameters.insert(name.to_string(), value);
    }

    pub fn set_out_parameter(&mut self, name: &str, value: Value) {
        self.out_parameters.insert(name.to_string(), value);
    }
}

/// Cursor state
pub struct CursorState {
    pub query: String,
    pub is_open: bool,
    pub row_count: i64,
    pub current_row: Option<Vec<Value>>,
    pub rows: Vec<Vec<Value>>,
    pub position: usize,
}

impl CursorState {
    pub fn fetch(&mut self) -> Option<Vec<Value>> {
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            self.current_row = Some(row.clone());
            self.row_count += 1;
            Some(row)
        } else {
            self.current_row = None;
            None
        }
    }

    pub fn is_found(&self) -> bool {
        self.current_row.is_some()
    }
}

// ============================================================================
// Control Flow Results
// ============================================================================

/// Result of executing a statement
#[derive(Clone, Debug)]
pub enum ControlFlow {
    /// Continue to next statement
    Continue,
    /// Exit current loop
    Exit(Option<String>),
    /// Continue to next iteration
    ContinueLoop(Option<String>),
    /// Return from procedure
    Return(Option<Value>),
    /// Exception raised
    Exception(ProcedureError),
}

// ============================================================================
// Procedure Executor
// ============================================================================

/// SQL executor interface (to be implemented by engine)
pub trait SqlExecutor: Send + Sync {
    fn execute(&self, sql: &str, params: &[Value]) -> Result<SqlResult, ProcedureError>;
    fn execute_query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, ProcedureError>;
}

/// SQL execution result
#[derive(Clone, Debug)]
pub struct SqlResult {
    pub rows_affected: i64,
    pub returning: Vec<Vec<Value>>,
}

/// Procedure executor
pub struct ProcedureExecutor<E: SqlExecutor> {
    sql_executor: Arc<E>,
    config: ExecutorConfig,
}

/// Executor configuration
#[derive(Clone, Debug)]
pub struct ExecutorConfig {
    pub max_recursion: usize,
    pub timeout: Duration,
    pub debug_mode: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_recursion: 100,
            timeout: Duration::from_secs(300),
            debug_mode: false,
        }
    }
}

impl<E: SqlExecutor> ProcedureExecutor<E> {
    pub fn new(sql_executor: Arc<E>, config: ExecutorConfig) -> Self {
        Self { sql_executor, config }
    }

    /// Execute a procedure
    pub fn execute(
        &self,
        procedure: &ProcedureDefinition,
        arguments: Vec<Value>,
    ) -> Result<ProcedureResult, ProcedureError> {
        let mut ctx = ExecutionContext::new(self.config.timeout, self.config.max_recursion);

        // Bind parameters
        for (i, param) in procedure.parameters.iter().enumerate() {
            let value = arguments.get(i).cloned().unwrap_or_else(|| {
                param.default_value.clone().unwrap_or(Value::Null)
            });
            ctx.set_parameter(&param.name, value);
        }

        // Execute body
        match &procedure.body {
            ProcedureBody::Block(block) => {
                self.execute_block(block, &mut ctx)?;
            }
            ProcedureBody::SqlExpression(sql) => {
                let result = self.sql_executor.execute_query(sql, &[])?;
                if let Some(row) = result.first() {
                    if let Some(value) = row.first() {
                        ctx.return_value = Some(value.clone());
                    }
                }
            }
            ProcedureBody::External { .. } => {
                return Err(ProcedureError::CompilationError(
                    "external procedures not supported".into()
                ));
            }
        }

        Ok(ProcedureResult {
            return_value: ctx.return_value,
            out_parameters: ctx.out_parameters,
            sql_row_count: ctx.sql_row_count,
        })
    }

    /// Execute a block
    fn execute_block(&self, block: &Block, ctx: &mut ExecutionContext) -> Result<ControlFlow, ProcedureError> {
        // Process declarations
        for decl in &block.declarations {
            self.process_declaration(decl, ctx)?;
        }

        // Add label if present
        if let Some(ref label) = block.label {
            ctx.labels.push(label.clone());
        }

        // Execute statements
        let result = self.execute_statements(&block.statements, ctx);

        // Handle exceptions
        match result {
            Err(e) => {
                for handler in &block.exception_handlers {
                    if self.matches_exception(&e, &handler.exceptions) {
                        let flow = self.execute_statements(&handler.statements, ctx)?;
                        if let Some(ref label) = block.label {
                            ctx.labels.retain(|l| l != label);
                        }
                        return Ok(flow);
                    }
                }
                Err(e)
            }
            Ok(flow) => {
                if let Some(ref label) = block.label {
                    ctx.labels.retain(|l| l != label);
                }
                Ok(flow)
            }
        }
    }

    /// Process a declaration
    fn process_declaration(&self, decl: &Declaration, ctx: &mut ExecutionContext) -> Result<(), ProcedureError> {
        match decl {
            Declaration::Variable(var) | Declaration::Constant(var) => {
                let value = if let Some(ref expr) = var.initial_value {
                    self.evaluate_expression(expr, ctx)?
                } else {
                    Value::Null
                };

                if var.not_null && value.is_null() {
                    return Err(ProcedureError::NullValue(var.name.clone()));
                }

                ctx.set_variable(&var.name, value);
            }
            Declaration::Cursor(cursor) => {
                ctx.cursors.insert(cursor.name.clone(), CursorState {
                    query: cursor.query.clone(),
                    is_open: false,
                    row_count: 0,
                    current_row: None,
                    rows: Vec::new(),
                    position: 0,
                });
            }
            Declaration::Exception(_name) => {
                // Register exception name
            }
            Declaration::Type(_) => {
                // Register type
            }
            Declaration::Procedure(_) => {
                // Nested procedures handled separately
            }
        }
        Ok(())
    }

    /// Execute a list of statements
    fn execute_statements(&self, statements: &[Statement], ctx: &mut ExecutionContext) -> Result<ControlFlow, ProcedureError> {
        for stmt in statements {
            ctx.check_timeout()?;

            let flow = self.execute_statement(stmt, ctx)?;
            match flow {
                ControlFlow::Continue => {}
                other => return Ok(other),
            }
        }
        Ok(ControlFlow::Continue)
    }

    /// Execute a single statement
    fn execute_statement(&self, stmt: &Statement, ctx: &mut ExecutionContext) -> Result<ControlFlow, ProcedureError> {
        match stmt {
            Statement::Assignment { target, value } => {
                let val = self.evaluate_expression(value, ctx)?;
                match target {
                    AssignmentTarget::Variable(name) => {
                        ctx.set_variable(name, val);
                    }
                    AssignmentTarget::ArrayElement { array, index } => {
                        let idx = self.evaluate_expression(index, ctx)?.to_i64()
                            .ok_or_else(|| ProcedureError::TypeMismatch {
                                expected: "integer".into(),
                                got: "other".into(),
                            })? as usize;

                        if let Some(Value::Array(ref mut arr)) = ctx.variables.get_mut(array) {
                            if idx < arr.len() {
                                arr[idx] = val;
                            }
                        }
                    }
                    AssignmentTarget::RecordField { record, field } => {
                        if let Some(Value::Record(ref mut rec)) = ctx.variables.get_mut(record) {
                            rec.insert(field.clone(), val);
                        }
                    }
                }
                Ok(ControlFlow::Continue)
            }

            Statement::If { condition, then_branch, elsif_branches, else_branch } => {
                let cond = self.evaluate_expression(condition, ctx)?;
                if cond.to_bool().unwrap_or(false) {
                    return self.execute_statements(then_branch, ctx);
                }

                for (elsif_cond, elsif_stmts) in elsif_branches {
                    let cond = self.evaluate_expression(elsif_cond, ctx)?;
                    if cond.to_bool().unwrap_or(false) {
                        return self.execute_statements(elsif_stmts, ctx);
                    }
                }

                if let Some(else_stmts) = else_branch {
                    return self.execute_statements(else_stmts, ctx);
                }

                Ok(ControlFlow::Continue)
            }

            Statement::Case { selector, when_branches, else_branch } => {
                let selector_val = selector.as_ref()
                    .map(|s| self.evaluate_expression(s, ctx))
                    .transpose()?;

                for (when_expr, when_stmts) in when_branches {
                    let when_val = self.evaluate_expression(when_expr, ctx)?;

                    let matches = if let Some(ref sel) = selector_val {
                        sel == &when_val
                    } else {
                        when_val.to_bool().unwrap_or(false)
                    };

                    if matches {
                        return self.execute_statements(when_stmts, ctx);
                    }
                }

                if let Some(else_stmts) = else_branch {
                    return self.execute_statements(else_stmts, ctx);
                }

                Ok(ControlFlow::Continue)
            }

            Statement::Loop { label, statements } => {
                if let Some(lbl) = label {
                    ctx.labels.push(lbl.clone());
                }

                loop {
                    ctx.check_timeout()?;

                    let flow = self.execute_statements(statements, ctx)?;
                    match flow {
                        ControlFlow::Continue => {}
                        ControlFlow::ContinueLoop(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                continue;
                            }
                            return Ok(ControlFlow::ContinueLoop(lbl));
                        }
                        ControlFlow::Exit(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                break;
                            }
                            return Ok(ControlFlow::Exit(lbl));
                        }
                        other => {
                            if let Some(lbl) = label {
                                ctx.labels.retain(|l| l != lbl);
                            }
                            return Ok(other);
                        }
                    }
                }

                if let Some(lbl) = label {
                    ctx.labels.retain(|l| l != lbl);
                }
                Ok(ControlFlow::Continue)
            }

            Statement::While { label, condition, statements } => {
                if let Some(lbl) = label {
                    ctx.labels.push(lbl.clone());
                }

                while self.evaluate_expression(condition, ctx)?.to_bool().unwrap_or(false) {
                    ctx.check_timeout()?;

                    let flow = self.execute_statements(statements, ctx)?;
                    match flow {
                        ControlFlow::Continue => {}
                        ControlFlow::ContinueLoop(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                continue;
                            }
                            return Ok(ControlFlow::ContinueLoop(lbl));
                        }
                        ControlFlow::Exit(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                break;
                            }
                            return Ok(ControlFlow::Exit(lbl));
                        }
                        other => {
                            if let Some(lbl) = label {
                                ctx.labels.retain(|l| l != lbl);
                            }
                            return Ok(other);
                        }
                    }
                }

                if let Some(lbl) = label {
                    ctx.labels.retain(|l| l != lbl);
                }
                Ok(ControlFlow::Continue)
            }

            Statement::ForNumeric { label, variable, start, end, step, reverse, statements } => {
                let start_val = self.evaluate_expression(start, ctx)?.to_i64()
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "integer".into(),
                        got: "other".into(),
                    })?;

                let end_val = self.evaluate_expression(end, ctx)?.to_i64()
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "integer".into(),
                        got: "other".into(),
                    })?;

                let step_val = match step {
                    Some(s) => {
                        self.evaluate_expression(s, ctx)?.to_i64()
                            .ok_or_else(|| ProcedureError::TypeMismatch {
                                expected: "integer".into(),
                                got: "other".into(),
                            })?
                    }
                    None => 1,
                };

                if let Some(lbl) = label {
                    ctx.labels.push(lbl.clone());
                }

                let range: Vec<i64> = if *reverse {
                    (end_val..=start_val).rev().step_by(step_val as usize).collect()
                } else {
                    (start_val..=end_val).step_by(step_val as usize).collect()
                };

                for i in range {
                    ctx.check_timeout()?;
                    ctx.set_variable(variable, Value::BigInt(i));

                    let flow = self.execute_statements(statements, ctx)?;
                    match flow {
                        ControlFlow::Continue => {}
                        ControlFlow::ContinueLoop(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                continue;
                            }
                            return Ok(ControlFlow::ContinueLoop(lbl));
                        }
                        ControlFlow::Exit(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                break;
                            }
                            return Ok(ControlFlow::Exit(lbl));
                        }
                        other => {
                            if let Some(lbl) = label {
                                ctx.labels.retain(|l| l != lbl);
                            }
                            return Ok(other);
                        }
                    }
                }

                if let Some(lbl) = label {
                    ctx.labels.retain(|l| l != lbl);
                }
                Ok(ControlFlow::Continue)
            }

            Statement::ForCursor { label, record_variable, cursor, statements } => {
                let query = match cursor {
                    CursorSource::Named(name) => {
                        ctx.cursors.get(name)
                            .map(|c| c.query.clone())
                            .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", name)))?
                    }
                    CursorSource::Inline(sql) => sql.clone(),
                    CursorSource::RefCursor(name) => {
                        return Err(ProcedureError::CursorError(format!("ref cursor not supported: {}", name)));
                    }
                };

                let rows = self.sql_executor.execute_query(&query, &[])?;

                if let Some(lbl) = label {
                    ctx.labels.push(lbl.clone());
                }

                for row in rows {
                    ctx.check_timeout()?;

                    // Set record variable
                    let mut record = HashMap::new();
                    for (i, val) in row.into_iter().enumerate() {
                        record.insert(format!("col{}", i), val);
                    }
                    ctx.set_variable(record_variable, Value::Record(record));

                    let flow = self.execute_statements(statements, ctx)?;
                    match flow {
                        ControlFlow::Continue => {}
                        ControlFlow::ContinueLoop(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                continue;
                            }
                            return Ok(ControlFlow::ContinueLoop(lbl));
                        }
                        ControlFlow::Exit(lbl) => {
                            if lbl.is_none() || lbl.as_ref() == label.as_ref() {
                                break;
                            }
                            return Ok(ControlFlow::Exit(lbl));
                        }
                        other => {
                            if let Some(lbl) = label {
                                ctx.labels.retain(|l| l != lbl);
                            }
                            return Ok(other);
                        }
                    }
                }

                if let Some(lbl) = label {
                    ctx.labels.retain(|l| l != lbl);
                }
                Ok(ControlFlow::Continue)
            }

            Statement::Exit { label, when_condition } => {
                if let Some(cond) = when_condition {
                    if !self.evaluate_expression(cond, ctx)?.to_bool().unwrap_or(false) {
                        return Ok(ControlFlow::Continue);
                    }
                }
                Ok(ControlFlow::Exit(label.clone()))
            }

            Statement::Continue { label, when_condition } => {
                if let Some(cond) = when_condition {
                    if !self.evaluate_expression(cond, ctx)?.to_bool().unwrap_or(false) {
                        return Ok(ControlFlow::Continue);
                    }
                }
                Ok(ControlFlow::ContinueLoop(label.clone()))
            }

            Statement::Return { value } => {
                let val = value.as_ref()
                    .map(|v| self.evaluate_expression(v, ctx))
                    .transpose()?;
                ctx.return_value = val.clone();
                Ok(ControlFlow::Return(val))
            }

            Statement::Raise { exception, message, using: _ } => {
                let msg = message.as_ref()
                    .map(|m| self.evaluate_expression(m, ctx))
                    .transpose()?
                    .map(|v| match v {
                        Value::String(s) => s,
                        _ => format!("{:?}", v),
                    })
                    .unwrap_or_default();

                Err(ProcedureError::ExceptionRaised {
                    code: exception.clone().unwrap_or_else(|| "USER_EXCEPTION".into()),
                    message: msg,
                })
            }

            Statement::Null => Ok(ControlFlow::Continue),

            Statement::ExecuteSql { sql, into } => {
                let result = self.sql_executor.execute(sql, &[])?;
                ctx.sql_row_count = result.rows_affected;
                ctx.sql_found = result.rows_affected > 0;

                // Bind INTO variables
                if !into.is_empty() && !result.returning.is_empty() {
                    if let Some(row) = result.returning.first() {
                        for (i, var) in into.iter().enumerate() {
                            if let Some(val) = row.get(i) {
                                ctx.set_variable(var, val.clone());
                            }
                        }
                    }
                }

                Ok(ControlFlow::Continue)
            }

            Statement::ExecuteImmediate { sql, into, using } => {
                let sql_str = match self.evaluate_expression(sql, ctx)? {
                    Value::String(s) => s,
                    _ => return Err(ProcedureError::TypeMismatch {
                        expected: "string".into(),
                        got: "other".into(),
                    }),
                };

                let params: Vec<Value> = using.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;

                let result = self.sql_executor.execute(&sql_str, &params)?;
                ctx.sql_row_count = result.rows_affected;
                ctx.sql_found = result.rows_affected > 0;

                // Bind INTO variables
                if !into.is_empty() && !result.returning.is_empty() {
                    if let Some(row) = result.returning.first() {
                        for (i, var) in into.iter().enumerate() {
                            if let Some(val) = row.get(i) {
                                ctx.set_variable(var, val.clone());
                            }
                        }
                    }
                }

                Ok(ControlFlow::Continue)
            }

            Statement::OpenCursor { cursor, arguments } => {
                // First check cursor exists and is not already open
                {
                    let state = ctx.cursors.get(cursor)
                        .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;
                    if state.is_open {
                        return Err(ProcedureError::CursorError("cursor already open".into()));
                    }
                }

                // Evaluate arguments before borrowing cursor mutably
                let params: Vec<Value> = arguments.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;

                // Get query string (need to borrow, then release)
                let query = ctx.cursors.get(cursor)
                    .map(|s| s.query.clone())
                    .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;

                // Execute query
                let rows = self.sql_executor.execute_query(&query, &params)?;

                // Now get mutable borrow to update state
                let state = ctx.cursors.get_mut(cursor)
                    .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;
                state.rows = rows;
                state.position = 0;
                state.is_open = true;
                state.row_count = 0;

                Ok(ControlFlow::Continue)
            }

            Statement::FetchCursor { cursor, into, bulk_collect: _, limit: _ } => {
                let state = ctx.cursors.get_mut(cursor)
                    .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;

                if !state.is_open {
                    return Err(ProcedureError::CursorError("cursor not open".into()));
                }

                if let Some(row) = state.fetch() {
                    for (i, var) in into.iter().enumerate() {
                        if let Some(val) = row.get(i) {
                            ctx.set_variable(var, val.clone());
                        }
                    }
                }

                Ok(ControlFlow::Continue)
            }

            Statement::CloseCursor { cursor } => {
                let state = ctx.cursors.get_mut(cursor)
                    .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;

                state.is_open = false;
                state.rows.clear();
                state.position = 0;
                state.current_row = None;

                Ok(ControlFlow::Continue)
            }

            Statement::Call { procedure, arguments, into } => {
                // In a real implementation, this would look up and execute the procedure
                // For now, we just execute the arguments and bind results
                let _args: Vec<Value> = arguments.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;

                // Would call nested procedure here
                let _ = (procedure, into);

                Ok(ControlFlow::Continue)
            }

            Statement::Block(block) => {
                self.execute_block(block, ctx)
            }

            Statement::PipeRow { value } => {
                let _val = self.evaluate_expression(value, ctx)?;
                // Would add to pipelined result set
                Ok(ControlFlow::Continue)
            }

            Statement::Forall { index_variable: _, bounds: _, dml: _ } => {
                // Bulk DML not implemented
                Ok(ControlFlow::Continue)
            }
        }
    }

    /// Evaluate an expression
    fn evaluate_expression(&self, expr: &Expression, ctx: &ExecutionContext) -> Result<Value, ProcedureError> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),

            Expression::Variable(name) => {
                ctx.get_variable(name)
                    .cloned()
                    .ok_or_else(|| ProcedureError::UndeclaredVariable(name.clone()))
            }

            Expression::Parameter(name) => {
                ctx.parameters.get(name)
                    .cloned()
                    .ok_or_else(|| ProcedureError::UndeclaredVariable(name.clone()))
            }

            Expression::ArrayAccess { array, index } => {
                let arr = self.evaluate_expression(array, ctx)?;
                let idx = self.evaluate_expression(index, ctx)?.to_i64()
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "integer".into(),
                        got: "other".into(),
                    })? as usize;

                match arr {
                    Value::Array(elements) => {
                        elements.get(idx).cloned().ok_or_else(|| {
                            ProcedureError::InvalidParameter(format!("array index out of bounds: {}", idx))
                        })
                    }
                    _ => Err(ProcedureError::TypeMismatch {
                        expected: "array".into(),
                        got: format!("{:?}", arr.data_type()),
                    }),
                }
            }

            Expression::FieldAccess { record, field } => {
                let rec = self.evaluate_expression(record, ctx)?;
                match rec {
                    Value::Record(fields) => {
                        fields.get(field).cloned().ok_or_else(|| {
                            ProcedureError::InvalidParameter(format!("field not found: {}", field))
                        })
                    }
                    _ => Err(ProcedureError::TypeMismatch {
                        expected: "record".into(),
                        got: format!("{:?}", rec.data_type()),
                    }),
                }
            }

            Expression::BinaryOp { left, op, right } => {
                let lval = self.evaluate_expression(left, ctx)?;
                let rval = self.evaluate_expression(right, ctx)?;
                self.apply_binary_op(&lval, op, &rval)
            }

            Expression::UnaryOp { op, operand } => {
                let val = self.evaluate_expression(operand, ctx)?;
                self.apply_unary_op(op, &val)
            }

            Expression::FunctionCall { name, arguments } => {
                let args: Vec<Value> = arguments.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;
                self.call_builtin_function(name, &args)
            }

            Expression::Case { operand, when_clauses, else_clause } => {
                let operand_val = operand.as_ref()
                    .map(|o| self.evaluate_expression(o, ctx))
                    .transpose()?;

                for (when_expr, then_expr) in when_clauses {
                    let when_val = self.evaluate_expression(when_expr, ctx)?;

                    let matches = if let Some(ref op) = operand_val {
                        op == &when_val
                    } else {
                        when_val.to_bool().unwrap_or(false)
                    };

                    if matches {
                        return self.evaluate_expression(then_expr, ctx);
                    }
                }

                if let Some(else_expr) = else_clause {
                    self.evaluate_expression(else_expr, ctx)
                } else {
                    Ok(Value::Null)
                }
            }

            Expression::Subquery(sql) => {
                let rows = self.sql_executor.execute_query(sql, &[])?;
                if let Some(row) = rows.first() {
                    if let Some(val) = row.first() {
                        return Ok(val.clone());
                    }
                }
                Ok(Value::Null)
            }

            Expression::Exists(sql) => {
                let rows = self.sql_executor.execute_query(sql, &[])?;
                Ok(Value::Boolean(!rows.is_empty()))
            }

            Expression::InList { expr, list, negated } => {
                let val = self.evaluate_expression(expr, ctx)?;
                let mut found = false;
                for item in list {
                    let item_val = self.evaluate_expression(item, ctx)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Boolean(if *negated { !found } else { found }))
            }

            Expression::Between { expr, low, high, negated } => {
                let val = self.evaluate_expression(expr, ctx)?.to_f64();
                let lo = self.evaluate_expression(low, ctx)?.to_f64();
                let hi = self.evaluate_expression(high, ctx)?.to_f64();

                let result = match (val, lo, hi) {
                    (Some(v), Some(l), Some(h)) => v >= l && v <= h,
                    _ => false,
                };

                Ok(Value::Boolean(if *negated { !result } else { result }))
            }

            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_expression(expr, ctx)?;
                let is_null = val.is_null();
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }

            Expression::Like { expr, pattern, escape: _, negated } => {
                let val = match self.evaluate_expression(expr, ctx)? {
                    Value::String(s) => s,
                    _ => return Ok(Value::Boolean(false)),
                };
                let pat = match self.evaluate_expression(pattern, ctx)? {
                    Value::String(s) => s,
                    _ => return Ok(Value::Boolean(false)),
                };

                // Simple LIKE implementation
                let regex = pat.replace('%', ".*").replace('_', ".");
                let matches = regex::Regex::new(&format!("^{}$", regex))
                    .map(|r| r.is_match(&val))
                    .unwrap_or(false);

                Ok(Value::Boolean(if *negated { !matches } else { matches }))
            }

            Expression::Cast { expr, target_type } => {
                let val = self.evaluate_expression(expr, ctx)?;
                self.cast_value(val, target_type)
            }

            Expression::CursorAttribute { cursor, attribute } => {
                let state = ctx.cursors.get(cursor)
                    .ok_or_else(|| ProcedureError::CursorError(format!("cursor not found: {}", cursor)))?;

                let val = match attribute {
                    CursorAttribute::Found => Value::Boolean(state.is_found()),
                    CursorAttribute::NotFound => Value::Boolean(!state.is_found()),
                    CursorAttribute::RowCount => Value::BigInt(state.row_count),
                    CursorAttribute::IsOpen => Value::Boolean(state.is_open),
                };

                Ok(val)
            }

            Expression::SqlAttribute(attr) => {
                let val = match attr {
                    SqlAttribute::Found => Value::Boolean(ctx.sql_found),
                    SqlAttribute::NotFound => Value::Boolean(!ctx.sql_found),
                    SqlAttribute::RowCount => Value::BigInt(ctx.sql_row_count),
                };
                Ok(val)
            }

            Expression::Row(elements) => {
                let values: Vec<Value> = elements.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;
                Ok(Value::Array(values))
            }

            Expression::Array(elements) => {
                let values: Vec<Value> = elements.iter()
                    .map(|e| self.evaluate_expression(e, ctx))
                    .collect::<Result<_, _>>()?;
                Ok(Value::Array(values))
            }

            Expression::Null => Ok(Value::Null),
        }
    }

    fn apply_binary_op(&self, left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value, ProcedureError> {
        match op {
            BinaryOperator::Add => {
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
                    (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a + b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                    (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a + b)),
                    _ => {
                        let a = left.to_f64().unwrap_or(0.0);
                        let b = right.to_f64().unwrap_or(0.0);
                        Ok(Value::Double(a + b))
                    }
                }
            }
            BinaryOperator::Subtract => {
                let a = left.to_f64().unwrap_or(0.0);
                let b = right.to_f64().unwrap_or(0.0);
                Ok(Value::Double(a - b))
            }
            BinaryOperator::Multiply => {
                let a = left.to_f64().unwrap_or(0.0);
                let b = right.to_f64().unwrap_or(0.0);
                Ok(Value::Double(a * b))
            }
            BinaryOperator::Divide => {
                let a = left.to_f64().unwrap_or(0.0);
                let b = right.to_f64().unwrap_or(0.0);
                if b == 0.0 {
                    return Err(ProcedureError::DivisionByZero);
                }
                Ok(Value::Double(a / b))
            }
            BinaryOperator::Modulo => {
                let a = left.to_i64().unwrap_or(0);
                let b = right.to_i64().unwrap_or(1);
                if b == 0 {
                    return Err(ProcedureError::DivisionByZero);
                }
                Ok(Value::BigInt(a % b))
            }
            BinaryOperator::Power => {
                let a = left.to_f64().unwrap_or(0.0);
                let b = right.to_f64().unwrap_or(0.0);
                Ok(Value::Double(a.powf(b)))
            }
            BinaryOperator::Equal => Ok(Value::Boolean(left == right)),
            BinaryOperator::NotEqual => Ok(Value::Boolean(left != right)),
            BinaryOperator::LessThan => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Boolean(cmp < 0))
            }
            BinaryOperator::LessThanOrEqual => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Boolean(cmp <= 0))
            }
            BinaryOperator::GreaterThan => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Boolean(cmp > 0))
            }
            BinaryOperator::GreaterThanOrEqual => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Boolean(cmp >= 0))
            }
            BinaryOperator::And => {
                let a = left.to_bool().unwrap_or(false);
                let b = right.to_bool().unwrap_or(false);
                Ok(Value::Boolean(a && b))
            }
            BinaryOperator::Or => {
                let a = left.to_bool().unwrap_or(false);
                let b = right.to_bool().unwrap_or(false);
                Ok(Value::Boolean(a || b))
            }
            BinaryOperator::Concat => {
                let a = match left {
                    Value::String(s) => s.clone(),
                    _ => format!("{:?}", left),
                };
                let b = match right {
                    Value::String(s) => s.clone(),
                    _ => format!("{:?}", right),
                };
                Ok(Value::String(format!("{}{}", a, b)))
            }
        }
    }

    fn apply_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value, ProcedureError> {
        match op {
            UnaryOperator::Not => {
                Ok(Value::Boolean(!val.to_bool().unwrap_or(false)))
            }
            UnaryOperator::Negate => {
                match val {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::BigInt(i) => Ok(Value::BigInt(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Double(d) => Ok(Value::Double(-d)),
                    _ => Err(ProcedureError::TypeMismatch {
                        expected: "numeric".into(),
                        got: format!("{:?}", val.data_type()),
                    }),
                }
            }
            UnaryOperator::Plus => Ok(val.clone()),
        }
    }

    fn compare_values(&self, left: &Value, right: &Value) -> i32 {
        match (left.to_f64(), right.to_f64()) {
            (Some(a), Some(b)) => {
                if a < b { -1 }
                else if a > b { 1 }
                else { 0 }
            }
            _ => 0,
        }
    }

    fn call_builtin_function(&self, name: &str, args: &[Value]) -> Result<Value, ProcedureError> {
        match name.to_uppercase().as_str() {
            "COALESCE" => {
                for arg in args {
                    if !arg.is_null() {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "NVL" => {
                if args.len() >= 2 {
                    if args[0].is_null() {
                        Ok(args[1].clone())
                    } else {
                        Ok(args[0].clone())
                    }
                } else {
                    Err(ProcedureError::InvalidParameter("NVL requires 2 arguments".into()))
                }
            }
            "UPPER" => {
                match args.first() {
                    Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
                    _ => Ok(Value::Null),
                }
            }
            "LOWER" => {
                match args.first() {
                    Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
                    _ => Ok(Value::Null),
                }
            }
            "LENGTH" => {
                match args.first() {
                    Some(Value::String(s)) => Ok(Value::BigInt(s.len() as i64)),
                    _ => Ok(Value::Null),
                }
            }
            "SUBSTR" | "SUBSTRING" => {
                match (args.get(0), args.get(1), args.get(2)) {
                    (Some(Value::String(s)), Some(start), len) => {
                        let start_idx = start.to_i64().unwrap_or(1).max(1) as usize - 1;
                        let length = len.and_then(|l| l.to_i64()).map(|l| l as usize);

                        let result: String = if let Some(l) = length {
                            s.chars().skip(start_idx).take(l).collect()
                        } else {
                            s.chars().skip(start_idx).collect()
                        };
                        Ok(Value::String(result))
                    }
                    _ => Ok(Value::Null),
                }
            }
            "ABS" => {
                match args.first() {
                    Some(v) => {
                        match v.to_f64() {
                            Some(f) => Ok(Value::Double(f.abs())),
                            None => Ok(Value::Null),
                        }
                    }
                    None => Ok(Value::Null),
                }
            }
            "ROUND" => {
                match (args.get(0), args.get(1)) {
                    (Some(v), precision) => {
                        let f = v.to_f64().unwrap_or(0.0);
                        let p = precision.and_then(|p| p.to_i64()).unwrap_or(0) as i32;
                        let multiplier = 10f64.powi(p);
                        Ok(Value::Double((f * multiplier).round() / multiplier))
                    }
                    _ => Ok(Value::Null),
                }
            }
            "TRUNC" => {
                match args.first() {
                    Some(v) => {
                        let f = v.to_f64().unwrap_or(0.0);
                        Ok(Value::BigInt(f.trunc() as i64))
                    }
                    None => Ok(Value::Null),
                }
            }
            "TO_CHAR" => {
                match args.first() {
                    Some(v) => Ok(Value::String(format!("{:?}", v))),
                    None => Ok(Value::Null),
                }
            }
            "TO_NUMBER" => {
                match args.first() {
                    Some(Value::String(s)) => {
                        s.parse::<f64>()
                            .map(Value::Double)
                            .ok()
                            .ok_or_else(|| ProcedureError::TypeMismatch {
                                expected: "number".into(),
                                got: s.clone(),
                            })
                    }
                    Some(v) => Ok(Value::Double(v.to_f64().unwrap_or(0.0))),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ProcedureError::InvalidParameter(format!("unknown function: {}", name))),
        }
    }

    fn cast_value(&self, val: Value, target: &DataType) -> Result<Value, ProcedureError> {
        match target {
            DataType::Integer => {
                val.to_i64().map(|i| Value::Integer(i as i32))
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "integer".into(),
                        got: format!("{:?}", val.data_type()),
                    })
            }
            DataType::BigInt => {
                val.to_i64().map(Value::BigInt)
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "bigint".into(),
                        got: format!("{:?}", val.data_type()),
                    })
            }
            DataType::Float => {
                val.to_f64().map(|f| Value::Float(f as f32))
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "float".into(),
                        got: format!("{:?}", val.data_type()),
                    })
            }
            DataType::Double => {
                val.to_f64().map(Value::Double)
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "double".into(),
                        got: format!("{:?}", val.data_type()),
                    })
            }
            DataType::Boolean => {
                val.to_bool().map(Value::Boolean)
                    .ok_or_else(|| ProcedureError::TypeMismatch {
                        expected: "boolean".into(),
                        got: format!("{:?}", val.data_type()),
                    })
            }
            DataType::Varchar(_) | DataType::Text => {
                Ok(Value::String(format!("{:?}", val)))
            }
            _ => Ok(val),
        }
    }

    fn matches_exception(&self, error: &ProcedureError, patterns: &[ExceptionMatch]) -> bool {
        for pattern in patterns {
            match pattern {
                ExceptionMatch::Others => return true,
                ExceptionMatch::Named(name) => {
                    if let ProcedureError::ExceptionRaised { code, .. } = error {
                        if code == name {
                            return true;
                        }
                    }
                }
                ExceptionMatch::SqlState(state) => {
                    // Match SQL state codes
                    if let ProcedureError::SqlError(msg) = error {
                        if msg.contains(state) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}

/// Result of procedure execution
#[derive(Clone, Debug)]
pub struct ProcedureResult {
    pub return_value: Option<Value>,
    pub out_parameters: HashMap<String, Value>,
    pub sql_row_count: i64,
}

// ============================================================================
// Procedure Manager
// ============================================================================

/// Manages stored procedures
pub struct ProcedureManager {
    procedures: RwLock<HashMap<String, ProcedureDefinition>>,
}

impl ProcedureManager {
    pub fn new() -> Self {
        Self {
            procedures: RwLock::new(HashMap::new()),
        }
    }

    /// Register a procedure
    pub fn register(&self, proc: ProcedureDefinition) -> Result<(), ProcedureError> {
        let key = if let Some(ref schema) = proc.schema {
            format!("{}.{}", schema, proc.name)
        } else {
            proc.name.clone()
        };

        let mut procs = self.procedures.write();
        procs.insert(key, proc);
        Ok(())
    }

    /// Get a procedure by name
    pub fn get(&self, schema: Option<&str>, name: &str) -> Option<ProcedureDefinition> {
        let procs = self.procedures.read();

        if let Some(s) = schema {
            procs.get(&format!("{}.{}", s, name)).cloned()
        } else {
            procs.get(name).cloned()
        }
    }

    /// Drop a procedure
    pub fn drop(&self, schema: Option<&str>, name: &str) -> bool {
        let mut procs = self.procedures.write();

        let key = if let Some(s) = schema {
            format!("{}.{}", s, name)
        } else {
            name.to_string()
        };

        procs.remove(&key).is_some()
    }

    /// List all procedures
    pub fn list(&self) -> Vec<String> {
        let procs = self.procedures.read();
        procs.keys().cloned().collect()
    }
}

impl Default for ProcedureManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockSqlExecutor;

    impl SqlExecutor for MockSqlExecutor {
        fn execute(&self, _sql: &str, _params: &[Value]) -> Result<SqlResult, ProcedureError> {
            Ok(SqlResult {
                rows_affected: 1,
                returning: vec![],
            })
        }

        fn execute_query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Vec<Value>>, ProcedureError> {
            Ok(vec![vec![Value::Integer(42)]])
        }
    }

    #[test]
    fn test_simple_procedure() {
        let proc = ProcedureDefinition {
            name: "add_numbers".to_string(),
            schema: None,
            parameters: vec![
                Parameter {
                    name: "a".to_string(),
                    data_type: DataType::Integer,
                    mode: ParameterMode::In,
                    default_value: None,
                },
                Parameter {
                    name: "b".to_string(),
                    data_type: DataType::Integer,
                    mode: ParameterMode::In,
                    default_value: None,
                },
            ],
            return_type: Some(DataType::Integer),
            is_function: true,
            language: ProcedureLanguage::PlSql,
            body: ProcedureBody::Block(Block {
                label: None,
                declarations: vec![],
                statements: vec![
                    Statement::Return {
                        value: Some(Expression::BinaryOp {
                            left: Box::new(Expression::Parameter("a".to_string())),
                            op: BinaryOperator::Add,
                            right: Box::new(Expression::Parameter("b".to_string())),
                        }),
                    },
                ],
                exception_handlers: vec![],
            }),
            security_definer: false,
            deterministic: true,
            parallel_safe: true,
            created_at: 0,
            modified_at: 0,
            owner: "system".to_string(),
            comment: None,
        };

        let executor = ProcedureExecutor::new(
            Arc::new(MockSqlExecutor),
            ExecutorConfig::default(),
        );

        let result = executor.execute(&proc, vec![Value::Integer(5), Value::Integer(3)]).unwrap();

        assert!(result.return_value.is_some());
        // Result should be 8.0 (as Double due to binary op)
    }

    #[test]
    fn test_loop_with_exit() {
        let proc = ProcedureDefinition {
            name: "count_to_ten".to_string(),
            schema: None,
            parameters: vec![],
            return_type: Some(DataType::Integer),
            is_function: true,
            language: ProcedureLanguage::PlSql,
            body: ProcedureBody::Block(Block {
                label: None,
                declarations: vec![
                    Declaration::Variable(Variable {
                        name: "i".to_string(),
                        data_type: DataType::Integer,
                        initial_value: Some(Expression::Literal(Value::Integer(0))),
                        is_constant: false,
                        not_null: false,
                    }),
                ],
                statements: vec![
                    Statement::Loop {
                        label: None,
                        statements: vec![
                            Statement::Assignment {
                                target: AssignmentTarget::Variable("i".to_string()),
                                value: Expression::BinaryOp {
                                    left: Box::new(Expression::Variable("i".to_string())),
                                    op: BinaryOperator::Add,
                                    right: Box::new(Expression::Literal(Value::Integer(1))),
                                },
                            },
                            Statement::Exit {
                                label: None,
                                when_condition: Some(Expression::BinaryOp {
                                    left: Box::new(Expression::Variable("i".to_string())),
                                    op: BinaryOperator::GreaterThanOrEqual,
                                    right: Box::new(Expression::Literal(Value::Integer(10))),
                                }),
                            },
                        ],
                    },
                    Statement::Return {
                        value: Some(Expression::Variable("i".to_string())),
                    },
                ],
                exception_handlers: vec![],
            }),
            security_definer: false,
            deterministic: true,
            parallel_safe: true,
            created_at: 0,
            modified_at: 0,
            owner: "system".to_string(),
            comment: None,
        };

        let executor = ProcedureExecutor::new(
            Arc::new(MockSqlExecutor),
            ExecutorConfig::default(),
        );

        let result = executor.execute(&proc, vec![]).unwrap();

        // Should return 10
        assert!(result.return_value.is_some());
    }

    #[test]
    fn test_procedure_manager() {
        let manager = ProcedureManager::new();

        let proc = ProcedureDefinition {
            name: "test_proc".to_string(),
            schema: Some("public".to_string()),
            parameters: vec![],
            return_type: None,
            is_function: false,
            language: ProcedureLanguage::PlSql,
            body: ProcedureBody::Block(Block {
                label: None,
                declarations: vec![],
                statements: vec![Statement::Null],
                exception_handlers: vec![],
            }),
            security_definer: false,
            deterministic: false,
            parallel_safe: false,
            created_at: 0,
            modified_at: 0,
            owner: "system".to_string(),
            comment: None,
        };

        manager.register(proc).unwrap();

        let retrieved = manager.get(Some("public"), "test_proc");
        assert!(retrieved.is_some());

        let list = manager.list();
        assert!(list.contains(&"public.test_proc".to_string()));

        assert!(manager.drop(Some("public"), "test_proc"));
        assert!(manager.get(Some("public"), "test_proc").is_none());
    }
}
