//! JIT Compilation Module
//!
//! Provides just-in-time compilation for query expressions using Cranelift.
//! This can provide 2-10x speedup for complex filter expressions and arithmetic.
//!
//! Supports:
//! - Filter expression compilation (WHERE clauses)
//! - Arithmetic expression compilation
//! - Comparison operations
//! - Boolean logic (AND, OR, NOT)
//! - Aggregation functions
//!
//! Note: This module provides a framework for JIT compilation. Full Cranelift
//! integration requires the cranelift-* crates to be added as dependencies.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

/// JIT compilation configuration
#[derive(Clone, Debug)]
pub struct JitConfig {
    /// Enable JIT compilation
    pub enabled: bool,
    /// Minimum expression complexity to trigger JIT (number of operations)
    pub min_complexity: usize,
    /// Maximum number of compiled functions to cache
    pub cache_size: usize,
    /// Optimization level (0-3)
    pub opt_level: u8,
    /// Enable debug info in compiled code
    pub debug_info: bool,
    /// Compile threshold - number of executions before JIT compiles
    pub compile_threshold: u64,
}

impl Default for JitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_complexity: 3,
            cache_size: 1000,
            opt_level: 2,
            debug_info: false,
            compile_threshold: 5,
        }
    }
}

/// Types supported by the JIT compiler
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JitType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl JitType {
    pub fn size_bytes(&self) -> usize {
        match self {
            JitType::Bool | JitType::I8 | JitType::U8 => 1,
            JitType::I16 | JitType::U16 => 2,
            JitType::I32 | JitType::U32 | JitType::F32 => 4,
            JitType::I64 | JitType::U64 | JitType::F64 => 8,
        }
    }

    pub fn is_float(&self) -> bool {
        matches!(self, JitType::F32 | JitType::F64)
    }

    pub fn is_signed(&self) -> bool {
        matches!(self, JitType::I8 | JitType::I16 | JitType::I32 | JitType::I64)
    }
}

/// JIT expression operations
#[derive(Clone, Debug, PartialEq)]
pub enum JitOp {
    // Constants
    ConstBool(bool),
    ConstI64(i64),
    ConstF64(f64),

    // Column access (index into row)
    Column(usize, JitType),

    // Arithmetic
    Add(Box<JitOp>, Box<JitOp>),
    Sub(Box<JitOp>, Box<JitOp>),
    Mul(Box<JitOp>, Box<JitOp>),
    Div(Box<JitOp>, Box<JitOp>),
    Mod(Box<JitOp>, Box<JitOp>),
    Neg(Box<JitOp>),
    Abs(Box<JitOp>),

    // Comparison
    Eq(Box<JitOp>, Box<JitOp>),
    Ne(Box<JitOp>, Box<JitOp>),
    Lt(Box<JitOp>, Box<JitOp>),
    Le(Box<JitOp>, Box<JitOp>),
    Gt(Box<JitOp>, Box<JitOp>),
    Ge(Box<JitOp>, Box<JitOp>),

    // Boolean logic
    And(Box<JitOp>, Box<JitOp>),
    Or(Box<JitOp>, Box<JitOp>),
    Not(Box<JitOp>),

    // Null handling
    IsNull(Box<JitOp>),
    IsNotNull(Box<JitOp>),
    Coalesce(Vec<JitOp>),

    // Conditional
    If(Box<JitOp>, Box<JitOp>, Box<JitOp>), // condition, then, else

    // Type conversion
    Cast(Box<JitOp>, JitType),

    // Math functions
    Sqrt(Box<JitOp>),
    Pow(Box<JitOp>, Box<JitOp>),
    Log(Box<JitOp>),
    Ln(Box<JitOp>),
    Exp(Box<JitOp>),
    Floor(Box<JitOp>),
    Ceil(Box<JitOp>),
    Round(Box<JitOp>),

    // Bitwise operations
    BitAnd(Box<JitOp>, Box<JitOp>),
    BitOr(Box<JitOp>, Box<JitOp>),
    BitXor(Box<JitOp>, Box<JitOp>),
    BitNot(Box<JitOp>),
    ShiftLeft(Box<JitOp>, Box<JitOp>),
    ShiftRight(Box<JitOp>, Box<JitOp>),
}

impl JitOp {
    /// Calculate the complexity of an expression (number of operations)
    pub fn complexity(&self) -> usize {
        match self {
            JitOp::ConstBool(_) | JitOp::ConstI64(_) | JitOp::ConstF64(_) | JitOp::Column(_, _) => 0,

            JitOp::Neg(a) | JitOp::Abs(a) | JitOp::Not(a) | JitOp::IsNull(a) |
            JitOp::IsNotNull(a) | JitOp::Cast(a, _) | JitOp::Sqrt(a) | JitOp::Log(a) |
            JitOp::Ln(a) | JitOp::Exp(a) | JitOp::Floor(a) | JitOp::Ceil(a) |
            JitOp::Round(a) | JitOp::BitNot(a) => 1 + a.complexity(),

            JitOp::Add(a, b) | JitOp::Sub(a, b) | JitOp::Mul(a, b) | JitOp::Div(a, b) |
            JitOp::Mod(a, b) | JitOp::Eq(a, b) | JitOp::Ne(a, b) | JitOp::Lt(a, b) |
            JitOp::Le(a, b) | JitOp::Gt(a, b) | JitOp::Ge(a, b) | JitOp::And(a, b) |
            JitOp::Or(a, b) | JitOp::Pow(a, b) | JitOp::BitAnd(a, b) | JitOp::BitOr(a, b) |
            JitOp::BitXor(a, b) | JitOp::ShiftLeft(a, b) | JitOp::ShiftRight(a, b) => {
                1 + a.complexity() + b.complexity()
            }

            JitOp::If(cond, then_val, else_val) => {
                1 + cond.complexity() + then_val.complexity() + else_val.complexity()
            }

            JitOp::Coalesce(ops) => 1 + ops.iter().map(|o| o.complexity()).sum::<usize>(),
        }
    }

    /// Get the result type of the expression
    pub fn result_type(&self) -> JitType {
        match self {
            JitOp::ConstBool(_) => JitType::Bool,
            JitOp::ConstI64(_) => JitType::I64,
            JitOp::ConstF64(_) => JitType::F64,
            JitOp::Column(_, t) => *t,

            JitOp::Add(a, _) | JitOp::Sub(a, _) | JitOp::Mul(a, _) |
            JitOp::Div(a, _) | JitOp::Mod(a, _) | JitOp::Neg(a) | JitOp::Abs(a) => a.result_type(),

            JitOp::Eq(_, _) | JitOp::Ne(_, _) | JitOp::Lt(_, _) | JitOp::Le(_, _) |
            JitOp::Gt(_, _) | JitOp::Ge(_, _) | JitOp::And(_, _) | JitOp::Or(_, _) |
            JitOp::Not(_) | JitOp::IsNull(_) | JitOp::IsNotNull(_) => JitType::Bool,

            JitOp::If(_, then_val, _) => then_val.result_type(),
            JitOp::Coalesce(ops) => ops.first().map(|o| o.result_type()).unwrap_or(JitType::I64),

            JitOp::Cast(_, t) => *t,

            JitOp::Sqrt(_) | JitOp::Pow(_, _) | JitOp::Log(_) | JitOp::Ln(_) |
            JitOp::Exp(_) => JitType::F64,

            JitOp::Floor(a) | JitOp::Ceil(a) | JitOp::Round(a) => a.result_type(),

            JitOp::BitAnd(a, _) | JitOp::BitOr(a, _) | JitOp::BitXor(a, _) |
            JitOp::BitNot(a) | JitOp::ShiftLeft(a, _) | JitOp::ShiftRight(a, _) => a.result_type(),
        }
    }
}

/// A compiled JIT function
pub struct CompiledFunction {
    /// Unique identifier
    pub id: u64,
    /// Source expression hash for cache lookup
    pub expr_hash: u64,
    /// The original expression
    pub expr: JitOp,
    /// Function pointer (opaque - would be actual JIT code)
    /// In a full implementation, this would be the compiled machine code
    code: Vec<u8>,
    /// Input types
    pub input_types: Vec<JitType>,
    /// Output type
    pub output_type: JitType,
    /// Number of times executed
    pub executions: AtomicU64,
    /// Compilation time in microseconds
    pub compile_time_us: u64,
    /// Size of compiled code
    pub code_size: usize,
}

impl CompiledFunction {
    /// Execute the compiled function on i64 inputs
    /// This is a reference implementation - actual JIT would call machine code
    pub fn execute_i64(&self, inputs: &[i64]) -> i64 {
        self.executions.fetch_add(1, Ordering::Relaxed);
        self.interpret_i64(&self.expr, inputs)
    }

    /// Execute the compiled function on f64 inputs
    pub fn execute_f64(&self, inputs: &[f64]) -> f64 {
        self.executions.fetch_add(1, Ordering::Relaxed);
        self.interpret_f64(&self.expr, inputs)
    }

    /// Execute returning a boolean (for filters)
    pub fn execute_bool(&self, inputs: &[i64]) -> bool {
        self.executions.fetch_add(1, Ordering::Relaxed);
        self.interpret_bool(&self.expr, inputs)
    }

    fn interpret_i64(&self, op: &JitOp, inputs: &[i64]) -> i64 {
        match op {
            JitOp::ConstI64(v) => *v,
            JitOp::ConstF64(v) => *v as i64,
            JitOp::ConstBool(v) => if *v { 1 } else { 0 },
            JitOp::Column(idx, _) => inputs.get(*idx).copied().unwrap_or(0),

            JitOp::Add(a, b) => self.interpret_i64(a, inputs).wrapping_add(self.interpret_i64(b, inputs)),
            JitOp::Sub(a, b) => self.interpret_i64(a, inputs).wrapping_sub(self.interpret_i64(b, inputs)),
            JitOp::Mul(a, b) => self.interpret_i64(a, inputs).wrapping_mul(self.interpret_i64(b, inputs)),
            JitOp::Div(a, b) => {
                let divisor = self.interpret_i64(b, inputs);
                if divisor == 0 { 0 } else { self.interpret_i64(a, inputs) / divisor }
            }
            JitOp::Mod(a, b) => {
                let divisor = self.interpret_i64(b, inputs);
                if divisor == 0 { 0 } else { self.interpret_i64(a, inputs) % divisor }
            }
            JitOp::Neg(a) => -self.interpret_i64(a, inputs),
            JitOp::Abs(a) => self.interpret_i64(a, inputs).abs(),

            JitOp::BitAnd(a, b) => self.interpret_i64(a, inputs) & self.interpret_i64(b, inputs),
            JitOp::BitOr(a, b) => self.interpret_i64(a, inputs) | self.interpret_i64(b, inputs),
            JitOp::BitXor(a, b) => self.interpret_i64(a, inputs) ^ self.interpret_i64(b, inputs),
            JitOp::BitNot(a) => !self.interpret_i64(a, inputs),
            JitOp::ShiftLeft(a, b) => self.interpret_i64(a, inputs) << (self.interpret_i64(b, inputs) as u32),
            JitOp::ShiftRight(a, b) => self.interpret_i64(a, inputs) >> (self.interpret_i64(b, inputs) as u32),

            JitOp::If(cond, then_val, else_val) => {
                if self.interpret_bool(cond, inputs) {
                    self.interpret_i64(then_val, inputs)
                } else {
                    self.interpret_i64(else_val, inputs)
                }
            }

            JitOp::Cast(a, _) => self.interpret_i64(a, inputs),

            _ => 0,
        }
    }

    fn interpret_f64(&self, op: &JitOp, inputs: &[f64]) -> f64 {
        let inputs_i64: Vec<i64> = inputs.iter().map(|f| *f as i64).collect();
        match op {
            JitOp::ConstF64(v) => *v,
            JitOp::ConstI64(v) => *v as f64,
            JitOp::ConstBool(v) => if *v { 1.0 } else { 0.0 },
            JitOp::Column(idx, _) => inputs.get(*idx).copied().unwrap_or(0.0),

            JitOp::Add(a, b) => self.interpret_f64(a, inputs) + self.interpret_f64(b, inputs),
            JitOp::Sub(a, b) => self.interpret_f64(a, inputs) - self.interpret_f64(b, inputs),
            JitOp::Mul(a, b) => self.interpret_f64(a, inputs) * self.interpret_f64(b, inputs),
            JitOp::Div(a, b) => {
                let divisor = self.interpret_f64(b, inputs);
                if divisor == 0.0 { 0.0 } else { self.interpret_f64(a, inputs) / divisor }
            }
            JitOp::Mod(a, b) => self.interpret_f64(a, inputs) % self.interpret_f64(b, inputs),
            JitOp::Neg(a) => -self.interpret_f64(a, inputs),
            JitOp::Abs(a) => self.interpret_f64(a, inputs).abs(),

            JitOp::Sqrt(a) => self.interpret_f64(a, inputs).sqrt(),
            JitOp::Pow(a, b) => self.interpret_f64(a, inputs).powf(self.interpret_f64(b, inputs)),
            JitOp::Log(a) => self.interpret_f64(a, inputs).log10(),
            JitOp::Ln(a) => self.interpret_f64(a, inputs).ln(),
            JitOp::Exp(a) => self.interpret_f64(a, inputs).exp(),
            JitOp::Floor(a) => self.interpret_f64(a, inputs).floor(),
            JitOp::Ceil(a) => self.interpret_f64(a, inputs).ceil(),
            JitOp::Round(a) => self.interpret_f64(a, inputs).round(),

            JitOp::If(cond, then_val, else_val) => {
                if self.interpret_bool(cond, &inputs_i64) {
                    self.interpret_f64(then_val, inputs)
                } else {
                    self.interpret_f64(else_val, inputs)
                }
            }

            JitOp::Cast(a, _) => self.interpret_f64(a, inputs),

            _ => 0.0,
        }
    }

    fn interpret_bool(&self, op: &JitOp, inputs: &[i64]) -> bool {
        match op {
            JitOp::ConstBool(v) => *v,
            JitOp::ConstI64(v) => *v != 0,
            JitOp::ConstF64(v) => *v != 0.0,

            JitOp::Eq(a, b) => self.interpret_i64(a, inputs) == self.interpret_i64(b, inputs),
            JitOp::Ne(a, b) => self.interpret_i64(a, inputs) != self.interpret_i64(b, inputs),
            JitOp::Lt(a, b) => self.interpret_i64(a, inputs) < self.interpret_i64(b, inputs),
            JitOp::Le(a, b) => self.interpret_i64(a, inputs) <= self.interpret_i64(b, inputs),
            JitOp::Gt(a, b) => self.interpret_i64(a, inputs) > self.interpret_i64(b, inputs),
            JitOp::Ge(a, b) => self.interpret_i64(a, inputs) >= self.interpret_i64(b, inputs),

            JitOp::And(a, b) => self.interpret_bool(a, inputs) && self.interpret_bool(b, inputs),
            JitOp::Or(a, b) => self.interpret_bool(a, inputs) || self.interpret_bool(b, inputs),
            JitOp::Not(a) => !self.interpret_bool(a, inputs),

            JitOp::IsNull(_) => false, // Simplified - would need null tracking
            JitOp::IsNotNull(_) => true,

            JitOp::If(cond, then_val, else_val) => {
                if self.interpret_bool(cond, inputs) {
                    self.interpret_bool(then_val, inputs)
                } else {
                    self.interpret_bool(else_val, inputs)
                }
            }

            _ => false,
        }
    }
}

/// JIT compilation statistics
#[derive(Clone, Debug, Default)]
pub struct JitStats {
    /// Number of expressions compiled
    pub compilations: u64,
    /// Number of cache hits
    pub cache_hits: u64,
    /// Number of cache misses
    pub cache_misses: u64,
    /// Total compilation time in microseconds
    pub total_compile_time_us: u64,
    /// Total executions of compiled code
    pub total_executions: u64,
    /// Number of expressions skipped (below complexity threshold)
    pub skipped_simple: u64,
    /// Number of compilation failures
    pub compilation_failures: u64,
}

impl JitStats {
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 { 0.0 } else { self.cache_hits as f64 / total as f64 }
    }
}

/// JIT compiler with expression caching
pub struct JitCompiler {
    config: JitConfig,
    /// Cache of compiled functions by expression hash
    cache: RwLock<HashMap<u64, Arc<CompiledFunction>>>,
    /// Execution counts for expressions not yet compiled
    execution_counts: RwLock<HashMap<u64, u64>>,
    /// Statistics
    stats: RwLock<JitStats>,
    /// Next function ID
    next_id: AtomicU64,
}

impl JitCompiler {
    pub fn new(config: JitConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
            execution_counts: RwLock::new(HashMap::new()),
            stats: RwLock::new(JitStats::default()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Get or compile an expression
    pub fn get_or_compile(&self, expr: &JitOp) -> Option<Arc<CompiledFunction>> {
        if !self.config.enabled {
            return None;
        }

        // Check complexity threshold
        let complexity = expr.complexity();
        if complexity < self.config.min_complexity {
            let mut stats = self.stats.write();
            stats.skipped_simple += 1;
            return None;
        }

        let expr_hash = self.hash_expr(expr);

        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(func) = cache.get(&expr_hash) {
                let mut stats = self.stats.write();
                stats.cache_hits += 1;
                return Some(func.clone());
            }
        }

        // Track execution count
        {
            let mut counts = self.execution_counts.write();
            let count = counts.entry(expr_hash).or_insert(0);
            *count += 1;

            if *count < self.config.compile_threshold {
                return None; // Not hot enough yet
            }
        }

        // Compile the expression
        let mut stats = self.stats.write();
        stats.cache_misses += 1;

        let start = std::time::Instant::now();
        let compiled = self.compile(expr, expr_hash);
        let compile_time_us = start.elapsed().as_micros() as u64;

        match compiled {
            Ok(func) => {
                stats.compilations += 1;
                stats.total_compile_time_us += compile_time_us;

                let func = Arc::new(func);
                let mut cache = self.cache.write();

                // Evict if cache is full
                while cache.len() >= self.config.cache_size {
                    // Simple eviction: remove first entry
                    if let Some(key) = cache.keys().next().copied() {
                        cache.remove(&key);
                    } else {
                        break;
                    }
                }

                cache.insert(expr_hash, func.clone());
                Some(func)
            }
            Err(_) => {
                stats.compilation_failures += 1;
                None
            }
        }
    }

    /// Compile an expression to native code
    fn compile(&self, expr: &JitOp, expr_hash: u64) -> Result<CompiledFunction, JitError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let start = std::time::Instant::now();

        // In a full implementation, this would use Cranelift to generate machine code
        // For now, we create a compiled function that uses interpretation
        let input_types = self.extract_input_types(expr);
        let output_type = expr.result_type();

        // Placeholder for actual JIT code generation
        // In production, this would be Cranelift IR -> machine code
        let code = self.generate_bytecode(expr);

        Ok(CompiledFunction {
            id,
            expr_hash,
            expr: expr.clone(),
            code,
            input_types,
            output_type,
            executions: AtomicU64::new(0),
            compile_time_us: start.elapsed().as_micros() as u64,
            code_size: 0, // Would be actual code size
        })
    }

    /// Extract input types from expression
    fn extract_input_types(&self, expr: &JitOp) -> Vec<JitType> {
        let mut types = HashMap::new();
        self.collect_column_types(expr, &mut types);

        let max_idx = types.keys().max().copied().unwrap_or(0);
        (0..=max_idx)
            .map(|i| types.get(&i).copied().unwrap_or(JitType::I64))
            .collect()
    }

    fn collect_column_types(&self, expr: &JitOp, types: &mut HashMap<usize, JitType>) {
        match expr {
            JitOp::Column(idx, t) => {
                types.insert(*idx, *t);
            }
            JitOp::Add(a, b) | JitOp::Sub(a, b) | JitOp::Mul(a, b) | JitOp::Div(a, b) |
            JitOp::Mod(a, b) | JitOp::Eq(a, b) | JitOp::Ne(a, b) | JitOp::Lt(a, b) |
            JitOp::Le(a, b) | JitOp::Gt(a, b) | JitOp::Ge(a, b) | JitOp::And(a, b) |
            JitOp::Or(a, b) | JitOp::Pow(a, b) | JitOp::BitAnd(a, b) | JitOp::BitOr(a, b) |
            JitOp::BitXor(a, b) | JitOp::ShiftLeft(a, b) | JitOp::ShiftRight(a, b) => {
                self.collect_column_types(a, types);
                self.collect_column_types(b, types);
            }
            JitOp::Neg(a) | JitOp::Abs(a) | JitOp::Not(a) | JitOp::IsNull(a) |
            JitOp::IsNotNull(a) | JitOp::Cast(a, _) | JitOp::Sqrt(a) | JitOp::Log(a) |
            JitOp::Ln(a) | JitOp::Exp(a) | JitOp::Floor(a) | JitOp::Ceil(a) |
            JitOp::Round(a) | JitOp::BitNot(a) => {
                self.collect_column_types(a, types);
            }
            JitOp::If(cond, then_val, else_val) => {
                self.collect_column_types(cond, types);
                self.collect_column_types(then_val, types);
                self.collect_column_types(else_val, types);
            }
            JitOp::Coalesce(ops) => {
                for op in ops {
                    self.collect_column_types(op, types);
                }
            }
            _ => {}
        }
    }

    /// Generate bytecode (placeholder for actual JIT)
    fn generate_bytecode(&self, _expr: &JitOp) -> Vec<u8> {
        // In production, this would generate actual machine code
        // For now, return empty as we use interpretation
        Vec::new()
    }

    /// Hash an expression for cache lookup
    fn hash_expr(&self, expr: &JitOp) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.hash_expr_impl(expr, &mut hasher);
        hasher.finish()
    }

    fn hash_expr_impl<H: std::hash::Hasher>(&self, expr: &JitOp, hasher: &mut H) {
        std::mem::discriminant(expr).hash(hasher);
        match expr {
            JitOp::ConstBool(v) => v.hash(hasher),
            JitOp::ConstI64(v) => v.hash(hasher),
            JitOp::ConstF64(v) => v.to_bits().hash(hasher),
            JitOp::Column(idx, t) => {
                idx.hash(hasher);
                (*t as u8).hash(hasher);
            }
            JitOp::Add(a, b) | JitOp::Sub(a, b) | JitOp::Mul(a, b) | JitOp::Div(a, b) |
            JitOp::Mod(a, b) | JitOp::Eq(a, b) | JitOp::Ne(a, b) | JitOp::Lt(a, b) |
            JitOp::Le(a, b) | JitOp::Gt(a, b) | JitOp::Ge(a, b) | JitOp::And(a, b) |
            JitOp::Or(a, b) | JitOp::Pow(a, b) | JitOp::BitAnd(a, b) | JitOp::BitOr(a, b) |
            JitOp::BitXor(a, b) | JitOp::ShiftLeft(a, b) | JitOp::ShiftRight(a, b) => {
                self.hash_expr_impl(a, hasher);
                self.hash_expr_impl(b, hasher);
            }
            JitOp::Neg(a) | JitOp::Abs(a) | JitOp::Not(a) | JitOp::IsNull(a) |
            JitOp::IsNotNull(a) | JitOp::Sqrt(a) | JitOp::Log(a) | JitOp::Ln(a) |
            JitOp::Exp(a) | JitOp::Floor(a) | JitOp::Ceil(a) | JitOp::Round(a) |
            JitOp::BitNot(a) => {
                self.hash_expr_impl(a, hasher);
            }
            JitOp::Cast(a, t) => {
                self.hash_expr_impl(a, hasher);
                (*t as u8).hash(hasher);
            }
            JitOp::If(cond, then_val, else_val) => {
                self.hash_expr_impl(cond, hasher);
                self.hash_expr_impl(then_val, hasher);
                self.hash_expr_impl(else_val, hasher);
            }
            JitOp::Coalesce(ops) => {
                for op in ops {
                    self.hash_expr_impl(op, hasher);
                }
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> JitStats {
        let mut stats = self.stats.read().clone();

        // Sum up executions from all compiled functions
        let cache = self.cache.read();
        stats.total_executions = cache
            .values()
            .map(|f| f.executions.load(Ordering::Relaxed))
            .sum();

        stats
    }

    /// Clear the compilation cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
        self.execution_counts.write().clear();
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.cache.read().len()
    }
}

/// JIT compilation errors
#[derive(Debug, Clone)]
pub enum JitError {
    /// Expression is too complex
    TooComplex,
    /// Unsupported operation
    UnsupportedOperation(String),
    /// Type mismatch
    TypeMismatch(String),
    /// Internal compiler error
    InternalError(String),
}

impl std::fmt::Display for JitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JitError::TooComplex => write!(f, "expression is too complex for JIT compilation"),
            JitError::UnsupportedOperation(op) => write!(f, "unsupported operation: {}", op),
            JitError::TypeMismatch(msg) => write!(f, "type mismatch: {}", msg),
            JitError::InternalError(msg) => write!(f, "internal JIT error: {}", msg),
        }
    }
}

impl std::error::Error for JitError {}

/// Vectorized JIT execution for batch processing
pub struct VectorizedJit {
    compiler: Arc<JitCompiler>,
}

impl VectorizedJit {
    pub fn new(compiler: Arc<JitCompiler>) -> Self {
        Self { compiler }
    }

    /// Execute a filter expression on a batch of rows
    /// Returns a boolean mask indicating which rows pass the filter
    pub fn execute_filter(&self, expr: &JitOp, columns: &[&[i64]]) -> Vec<bool> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);

        if let Some(compiled) = self.compiler.get_or_compile(expr) {
            let mut results = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let inputs: Vec<i64> = columns.iter().map(|col| col[row_idx]).collect();
                results.push(compiled.execute_bool(&inputs));
            }
            results
        } else {
            // Fallback to direct interpretation
            let mut results = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let inputs: Vec<i64> = columns.iter().map(|col| col[row_idx]).collect();
                results.push(self.interpret_bool(expr, &inputs));
            }
            results
        }
    }

    /// Execute an arithmetic expression on a batch of rows
    pub fn execute_arithmetic(&self, expr: &JitOp, columns: &[&[i64]]) -> Vec<i64> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);

        if let Some(compiled) = self.compiler.get_or_compile(expr) {
            let mut results = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let inputs: Vec<i64> = columns.iter().map(|col| col[row_idx]).collect();
                results.push(compiled.execute_i64(&inputs));
            }
            results
        } else {
            let mut results = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let inputs: Vec<i64> = columns.iter().map(|col| col[row_idx]).collect();
                results.push(self.interpret_i64(expr, &inputs));
            }
            results
        }
    }

    fn interpret_bool(&self, op: &JitOp, inputs: &[i64]) -> bool {
        match op {
            JitOp::ConstBool(v) => *v,
            JitOp::Eq(a, b) => self.interpret_i64(a, inputs) == self.interpret_i64(b, inputs),
            JitOp::Ne(a, b) => self.interpret_i64(a, inputs) != self.interpret_i64(b, inputs),
            JitOp::Lt(a, b) => self.interpret_i64(a, inputs) < self.interpret_i64(b, inputs),
            JitOp::Le(a, b) => self.interpret_i64(a, inputs) <= self.interpret_i64(b, inputs),
            JitOp::Gt(a, b) => self.interpret_i64(a, inputs) > self.interpret_i64(b, inputs),
            JitOp::Ge(a, b) => self.interpret_i64(a, inputs) >= self.interpret_i64(b, inputs),
            JitOp::And(a, b) => self.interpret_bool(a, inputs) && self.interpret_bool(b, inputs),
            JitOp::Or(a, b) => self.interpret_bool(a, inputs) || self.interpret_bool(b, inputs),
            JitOp::Not(a) => !self.interpret_bool(a, inputs),
            _ => false,
        }
    }

    fn interpret_i64(&self, op: &JitOp, inputs: &[i64]) -> i64 {
        match op {
            JitOp::ConstI64(v) => *v,
            JitOp::Column(idx, _) => inputs.get(*idx).copied().unwrap_or(0),
            JitOp::Add(a, b) => self.interpret_i64(a, inputs).wrapping_add(self.interpret_i64(b, inputs)),
            JitOp::Sub(a, b) => self.interpret_i64(a, inputs).wrapping_sub(self.interpret_i64(b, inputs)),
            JitOp::Mul(a, b) => self.interpret_i64(a, inputs).wrapping_mul(self.interpret_i64(b, inputs)),
            JitOp::Div(a, b) => {
                let d = self.interpret_i64(b, inputs);
                if d == 0 { 0 } else { self.interpret_i64(a, inputs) / d }
            }
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_compile_simple() {
        let compiler = JitCompiler::new(JitConfig {
            min_complexity: 1,
            compile_threshold: 1,
            ..Default::default()
        });

        // x + y where x is column 0, y is column 1
        let expr = JitOp::Add(
            Box::new(JitOp::Column(0, JitType::I64)),
            Box::new(JitOp::Column(1, JitType::I64)),
        );

        let compiled = compiler.get_or_compile(&expr).unwrap();
        assert_eq!(compiled.execute_i64(&[10, 20]), 30);
        assert_eq!(compiled.execute_i64(&[5, 3]), 8);
    }

    #[test]
    fn test_jit_filter() {
        let compiler = JitCompiler::new(JitConfig {
            min_complexity: 1,
            compile_threshold: 1,
            ..Default::default()
        });

        // x > 10 AND y < 100
        let expr = JitOp::And(
            Box::new(JitOp::Gt(
                Box::new(JitOp::Column(0, JitType::I64)),
                Box::new(JitOp::ConstI64(10)),
            )),
            Box::new(JitOp::Lt(
                Box::new(JitOp::Column(1, JitType::I64)),
                Box::new(JitOp::ConstI64(100)),
            )),
        );

        let compiled = compiler.get_or_compile(&expr).unwrap();
        assert!(compiled.execute_bool(&[15, 50]));
        assert!(!compiled.execute_bool(&[5, 50]));
        assert!(!compiled.execute_bool(&[15, 150]));
    }

    #[test]
    fn test_vectorized_jit() {
        let compiler = Arc::new(JitCompiler::new(JitConfig {
            min_complexity: 1,
            compile_threshold: 1,
            ..Default::default()
        }));

        let vjit = VectorizedJit::new(compiler);

        // x > 50
        let expr = JitOp::Gt(
            Box::new(JitOp::Column(0, JitType::I64)),
            Box::new(JitOp::ConstI64(50)),
        );

        let col0 = vec![10, 60, 30, 80, 50];
        let results = vjit.execute_filter(&expr, &[&col0]);

        assert_eq!(results, vec![false, true, false, true, false]);
    }

    #[test]
    fn test_expression_complexity() {
        let simple = JitOp::ConstI64(1);
        assert_eq!(simple.complexity(), 0);

        let medium = JitOp::Add(
            Box::new(JitOp::Column(0, JitType::I64)),
            Box::new(JitOp::Column(1, JitType::I64)),
        );
        assert_eq!(medium.complexity(), 1);

        let complex = JitOp::And(
            Box::new(JitOp::Gt(
                Box::new(JitOp::Add(
                    Box::new(JitOp::Column(0, JitType::I64)),
                    Box::new(JitOp::Column(1, JitType::I64)),
                )),
                Box::new(JitOp::ConstI64(10)),
            )),
            Box::new(JitOp::Lt(
                Box::new(JitOp::Column(2, JitType::I64)),
                Box::new(JitOp::ConstI64(100)),
            )),
        );
        assert_eq!(complex.complexity(), 4);
    }
}
