//! WebAssembly User-Defined Functions
//!
//! Portable, sandboxed UDFs using WebAssembly for secure function execution.
//! Supports WASM modules compiled from Rust, Go, AssemblyScript, etc.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// WASM value type
#[derive(Debug, Clone, PartialEq)]
pub enum WasmValue {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Null,
    Array(Vec<WasmValue>),
}

impl WasmValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            WasmValue::I32(v) => Some(*v as i64),
            WasmValue::I64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            WasmValue::F32(v) => Some(*v as f64),
            WasmValue::F64(v) => Some(*v),
            WasmValue::I32(v) => Some(*v as f64),
            WasmValue::I64(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            WasmValue::String(s) => Some(s),
            _ => None,
        }
    }
}

/// WASM function signature
#[derive(Debug, Clone)]
pub struct WasmSignature {
    /// Function name
    pub name: String,
    /// Parameter types
    pub params: Vec<WasmType>,
    /// Return type
    pub returns: WasmType,
    /// Is deterministic (same input = same output)
    pub deterministic: bool,
}

/// WASM type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmType {
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
    Void,
}

impl WasmType {
    pub fn to_sql_type(&self) -> &'static str {
        match self {
            WasmType::I32 => "INT32",
            WasmType::I64 => "INT64",
            WasmType::F32 => "FLOAT32",
            WasmType::F64 => "FLOAT64",
            WasmType::String => "STRING",
            WasmType::Bytes => "BINARY",
            WasmType::Void => "VOID",
        }
    }
}

/// WASM module metadata
#[derive(Debug, Clone)]
pub struct WasmModule {
    /// Module name
    pub name: String,
    /// Module version
    pub version: String,
    /// Module bytecode (compiled WASM)
    pub bytecode: Vec<u8>,
    /// Exported functions
    pub functions: Vec<WasmSignature>,
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// Fuel limit (execution steps)
    pub fuel_limit: u64,
    /// Description
    pub description: String,
}

/// WASM execution context
pub struct WasmContext {
    /// Memory pages allocated
    pub memory_pages: u32,
    /// Fuel consumed
    pub fuel_consumed: u64,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Host function calls
    pub host_calls: u32,
}

impl Default for WasmContext {
    fn default() -> Self {
        Self {
            memory_pages: 1,
            fuel_consumed: 0,
            execution_time_us: 0,
            host_calls: 0,
        }
    }
}

/// WASM UDF registry
pub struct WasmUdfRegistry {
    /// Registered modules
    modules: RwLock<HashMap<String, Arc<WasmModule>>>,
    /// Function to module mapping
    function_map: RwLock<HashMap<String, String>>,
    /// Default memory limit (16MB)
    default_memory_limit: usize,
    /// Default fuel limit (1M instructions)
    default_fuel_limit: u64,
}

impl WasmUdfRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            modules: RwLock::new(HashMap::new()),
            function_map: RwLock::new(HashMap::new()),
            default_memory_limit: 16 * 1024 * 1024,
            default_fuel_limit: 1_000_000,
        };

        // Register built-in WASM modules
        registry.register_builtins();
        registry
    }

    fn register_builtins(&mut self) {
        // Register example built-in module (simulated)
        let math_module = WasmModule {
            name: "math_utils".into(),
            version: "1.0.0".into(),
            bytecode: vec![], // Would contain actual WASM bytecode
            functions: vec![
                WasmSignature {
                    name: "wasm_fibonacci".into(),
                    params: vec![WasmType::I64],
                    returns: WasmType::I64,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_factorial".into(),
                    params: vec![WasmType::I64],
                    returns: WasmType::I64,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_gcd".into(),
                    params: vec![WasmType::I64, WasmType::I64],
                    returns: WasmType::I64,
                    deterministic: true,
                },
            ],
            memory_limit: 1024 * 1024,
            fuel_limit: 100_000,
            description: "Mathematical utility functions".into(),
        };

        let string_module = WasmModule {
            name: "string_utils".into(),
            version: "1.0.0".into(),
            bytecode: vec![],
            functions: vec![
                WasmSignature {
                    name: "wasm_levenshtein".into(),
                    params: vec![WasmType::String, WasmType::String],
                    returns: WasmType::I32,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_soundex".into(),
                    params: vec![WasmType::String],
                    returns: WasmType::String,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_metaphone".into(),
                    params: vec![WasmType::String],
                    returns: WasmType::String,
                    deterministic: true,
                },
            ],
            memory_limit: 1024 * 1024,
            fuel_limit: 100_000,
            description: "String processing functions".into(),
        };

        let crypto_module = WasmModule {
            name: "crypto_utils".into(),
            version: "1.0.0".into(),
            bytecode: vec![],
            functions: vec![
                WasmSignature {
                    name: "wasm_sha256".into(),
                    params: vec![WasmType::Bytes],
                    returns: WasmType::Bytes,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_md5".into(),
                    params: vec![WasmType::Bytes],
                    returns: WasmType::Bytes,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_base64_encode".into(),
                    params: vec![WasmType::Bytes],
                    returns: WasmType::String,
                    deterministic: true,
                },
                WasmSignature {
                    name: "wasm_base64_decode".into(),
                    params: vec![WasmType::String],
                    returns: WasmType::Bytes,
                    deterministic: true,
                },
            ],
            memory_limit: 2 * 1024 * 1024,
            fuel_limit: 200_000,
            description: "Cryptographic functions".into(),
        };

        self.register_module(math_module).ok();
        self.register_module(string_module).ok();
        self.register_module(crypto_module).ok();
    }

    /// Register a WASM module
    pub fn register_module(&self, module: WasmModule) -> Result<(), WasmError> {
        let mut modules = self.modules.write();
        let mut function_map = self.function_map.write();

        // Check for function name conflicts
        for func in &module.functions {
            if let Some(existing) = function_map.get(&func.name) {
                return Err(WasmError::FunctionExists(
                    func.name.clone(),
                    existing.clone(),
                ));
            }
        }

        // Register functions
        for func in &module.functions {
            function_map.insert(func.name.clone(), module.name.clone());
        }

        modules.insert(module.name.clone(), Arc::new(module));
        Ok(())
    }

    /// Unregister a WASM module
    pub fn unregister_module(&self, name: &str) -> Result<(), WasmError> {
        let mut modules = self.modules.write();
        let mut function_map = self.function_map.write();

        let module = modules
            .remove(name)
            .ok_or_else(|| WasmError::ModuleNotFound(name.into()))?;

        for func in &module.functions {
            function_map.remove(&func.name);
        }

        Ok(())
    }

    /// Get a module by name
    pub fn get_module(&self, name: &str) -> Option<Arc<WasmModule>> {
        self.modules.read().get(name).cloned()
    }

    /// Get function signature
    pub fn get_function(&self, name: &str) -> Option<WasmSignature> {
        let function_map = self.function_map.read();
        let modules = self.modules.read();

        let module_name = function_map.get(name)?;
        let module = modules.get(module_name)?;

        module.functions.iter().find(|f| f.name == name).cloned()
    }

    /// Check if function exists
    pub fn has_function(&self, name: &str) -> bool {
        self.function_map.read().contains_key(name)
    }

    /// List all functions
    pub fn list_functions(&self) -> Vec<WasmSignature> {
        let modules = self.modules.read();
        modules
            .values()
            .flat_map(|m| m.functions.iter().cloned())
            .collect()
    }

    /// List all modules
    pub fn list_modules(&self) -> Vec<(String, String)> {
        self.modules
            .read()
            .values()
            .map(|m| (m.name.clone(), m.description.clone()))
            .collect()
    }

    /// Execute a WASM function (simulated for now)
    pub fn execute(
        &self,
        func_name: &str,
        args: Vec<WasmValue>,
    ) -> Result<(WasmValue, WasmContext), WasmError> {
        let sig = self
            .get_function(func_name)
            .ok_or_else(|| WasmError::FunctionNotFound(func_name.into()))?;

        // Validate argument count
        if args.len() != sig.params.len() {
            return Err(WasmError::ArgumentMismatch {
                expected: sig.params.len(),
                got: args.len(),
            });
        }

        // Simulated execution for built-in functions
        let start = std::time::Instant::now();
        let result = self.execute_builtin(func_name, &args)?;
        let elapsed = start.elapsed();

        let ctx = WasmContext {
            memory_pages: 1,
            fuel_consumed: 1000,
            execution_time_us: elapsed.as_micros() as u64,
            host_calls: 0,
        };

        Ok((result, ctx))
    }

    fn execute_builtin(&self, func_name: &str, args: &[WasmValue]) -> Result<WasmValue, WasmError> {
        match func_name {
            "wasm_fibonacci" => {
                let n = args[0].as_i64().ok_or(WasmError::TypeMismatch)?;
                Ok(WasmValue::I64(self.fibonacci(n as u64)))
            }
            "wasm_factorial" => {
                let n = args[0].as_i64().ok_or(WasmError::TypeMismatch)?;
                Ok(WasmValue::I64(self.factorial(n as u64)))
            }
            "wasm_gcd" => {
                let a = args[0].as_i64().ok_or(WasmError::TypeMismatch)?;
                let b = args[1].as_i64().ok_or(WasmError::TypeMismatch)?;
                Ok(WasmValue::I64(self.gcd(a, b)))
            }
            "wasm_levenshtein" => {
                let s1 = args[0].as_string().ok_or(WasmError::TypeMismatch)?;
                let s2 = args[1].as_string().ok_or(WasmError::TypeMismatch)?;
                Ok(WasmValue::I32(self.levenshtein(s1, s2) as i32))
            }
            "wasm_soundex" => {
                let s = args[0].as_string().ok_or(WasmError::TypeMismatch)?;
                Ok(WasmValue::String(self.soundex(s)))
            }
            "wasm_base64_encode" => {
                if let WasmValue::Bytes(data) = &args[0] {
                    use base64::{engine::general_purpose::STANDARD, Engine as _};
                    Ok(WasmValue::String(STANDARD.encode(data)))
                } else {
                    Err(WasmError::TypeMismatch)
                }
            }
            _ => Err(WasmError::FunctionNotFound(func_name.into())),
        }
    }

    fn fibonacci(&self, n: u64) -> i64 {
        if n <= 1 {
            return n as i64;
        }
        let mut a = 0i64;
        let mut b = 1i64;
        for _ in 2..=n {
            let c = a.saturating_add(b);
            a = b;
            b = c;
        }
        b
    }

    fn factorial(&self, n: u64) -> i64 {
        (1..=n).fold(1i64, |acc, x| acc.saturating_mul(x as i64))
    }

    fn gcd(&self, mut a: i64, mut b: i64) -> i64 {
        a = a.abs();
        b = b.abs();
        while b != 0 {
            let t = b;
            b = a % b;
            a = t;
        }
        a
    }

    fn levenshtein(&self, s1: &str, s2: &str) -> usize {
        let s1: Vec<char> = s1.chars().collect();
        let s2: Vec<char> = s2.chars().collect();
        let m = s1.len();
        let n = s2.len();

        if m == 0 {
            return n;
        }
        if n == 0 {
            return m;
        }

        let mut prev: Vec<usize> = (0..=n).collect();
        let mut curr = vec![0; n + 1];

        for i in 1..=m {
            curr[0] = i;
            for j in 1..=n {
                let cost = if s1[i - 1] == s2[j - 1] { 0 } else { 1 };
                curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
            }
            std::mem::swap(&mut prev, &mut curr);
        }

        prev[n]
    }

    fn soundex(&self, s: &str) -> String {
        if s.is_empty() {
            return "0000".into();
        }

        let s = s.to_uppercase();
        let chars: Vec<char> = s.chars().filter(|c| c.is_alphabetic()).collect();

        if chars.is_empty() {
            return "0000".into();
        }

        let mut result = String::with_capacity(4);
        result.push(chars[0]);

        let code = |c: char| -> char {
            match c {
                'B' | 'F' | 'P' | 'V' => '1',
                'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
                'D' | 'T' => '3',
                'L' => '4',
                'M' | 'N' => '5',
                'R' => '6',
                _ => '0',
            }
        };

        let mut prev = code(chars[0]);
        for c in chars.iter().skip(1) {
            if result.len() >= 4 {
                break;
            }
            let curr = code(*c);
            if curr != '0' && curr != prev {
                result.push(curr);
            }
            prev = curr;
        }

        while result.len() < 4 {
            result.push('0');
        }

        result
    }
}

impl Default for WasmUdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// WASM error
#[derive(Debug, Clone)]
pub enum WasmError {
    ModuleNotFound(String),
    FunctionNotFound(String),
    FunctionExists(String, String),
    CompilationFailed(String),
    ExecutionFailed(String),
    OutOfFuel,
    OutOfMemory,
    TypeMismatch,
    ArgumentMismatch { expected: usize, got: usize },
    Timeout,
}

impl std::fmt::Display for WasmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ModuleNotFound(n) => write!(f, "WASM module '{}' not found", n),
            Self::FunctionNotFound(n) => write!(f, "WASM function '{}' not found", n),
            Self::FunctionExists(n, m) => {
                write!(f, "Function '{}' already exists in module '{}'", n, m)
            }
            Self::CompilationFailed(s) => write!(f, "WASM compilation failed: {}", s),
            Self::ExecutionFailed(s) => write!(f, "WASM execution failed: {}", s),
            Self::OutOfFuel => write!(f, "WASM execution ran out of fuel"),
            Self::OutOfMemory => write!(f, "WASM execution ran out of memory"),
            Self::TypeMismatch => write!(f, "WASM type mismatch"),
            Self::ArgumentMismatch { expected, got } => {
                write!(f, "Expected {} arguments, got {}", expected, got)
            }
            Self::Timeout => write!(f, "WASM execution timed out"),
        }
    }
}

impl std::error::Error for WasmError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_registry() {
        let registry = WasmUdfRegistry::new();

        assert!(registry.has_function("wasm_fibonacci"));
        assert!(registry.has_function("wasm_factorial"));
        assert!(registry.has_function("wasm_levenshtein"));
    }

    #[test]
    fn test_fibonacci() {
        let registry = WasmUdfRegistry::new();

        let (result, ctx) = registry
            .execute("wasm_fibonacci", vec![WasmValue::I64(10)])
            .unwrap();
        assert_eq!(result, WasmValue::I64(55));
    }

    #[test]
    fn test_factorial() {
        let registry = WasmUdfRegistry::new();

        let (result, _) = registry
            .execute("wasm_factorial", vec![WasmValue::I64(5)])
            .unwrap();
        assert_eq!(result, WasmValue::I64(120));
    }

    #[test]
    fn test_gcd() {
        let registry = WasmUdfRegistry::new();

        let (result, _) = registry
            .execute("wasm_gcd", vec![WasmValue::I64(48), WasmValue::I64(18)])
            .unwrap();
        assert_eq!(result, WasmValue::I64(6));
    }

    #[test]
    fn test_levenshtein() {
        let registry = WasmUdfRegistry::new();

        let (result, _) = registry
            .execute(
                "wasm_levenshtein",
                vec![
                    WasmValue::String("kitten".into()),
                    WasmValue::String("sitting".into()),
                ],
            )
            .unwrap();
        assert_eq!(result, WasmValue::I32(3));
    }

    #[test]
    fn test_soundex() {
        let registry = WasmUdfRegistry::new();

        let (result, _) = registry
            .execute("wasm_soundex", vec![WasmValue::String("Robert".into())])
            .unwrap();
        assert_eq!(result, WasmValue::String("R163".into()));
    }

    #[test]
    fn test_list_functions() {
        let registry = WasmUdfRegistry::new();
        let functions = registry.list_functions();

        assert!(functions.len() >= 7);
        assert!(functions.iter().any(|f| f.name == "wasm_fibonacci"));
    }
}
