//! Constraint Validation for Financial-Grade Data Integrity
//!
//! This module provides validation for:
//! - NOT NULL constraints
//! - PRIMARY KEY constraints (uniqueness + not null)
//! - UNIQUE constraints
//! - CHECK constraints (expression evaluation)
//! - FOREIGN KEY constraints (referential integrity)

use crate::engine::EngineError;
use crate::replication::TableMeta;
use crate::sql::TableConstraint;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow::compute;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Result of constraint validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether all constraints passed
    pub valid: bool,

    /// Violations found (constraint name -> error messages)
    pub violations: Vec<ConstraintViolation>,

    /// Number of rows validated
    pub rows_validated: usize,
}

impl ValidationResult {
    /// Create a successful validation result
    pub fn success(rows: usize) -> Self {
        ValidationResult {
            valid: true,
            violations: Vec::new(),
            rows_validated: rows,
        }
    }

    /// Create a failed validation result
    pub fn failure(violations: Vec<ConstraintViolation>, rows: usize) -> Self {
        ValidationResult {
            valid: false,
            violations,
            rows_validated: rows,
        }
    }

    /// Merge another validation result into this one
    pub fn merge(&mut self, other: ValidationResult) {
        if !other.valid {
            self.valid = false;
            self.violations.extend(other.violations);
        }
    }
}

/// A specific constraint violation
#[derive(Debug, Clone)]
pub struct ConstraintViolation {
    /// Name of the violated constraint
    pub constraint_name: String,

    /// Type of constraint
    pub constraint_type: ConstraintType,

    /// Affected column(s)
    pub columns: Vec<String>,

    /// Row indices that violated the constraint
    pub row_indices: Vec<usize>,

    /// Human-readable error message
    pub message: String,
}

/// Type of constraint
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintType {
    NotNull,
    PrimaryKey,
    Unique,
    Check,
    ForeignKey,
    Default,
}

impl std::fmt::Display for ConstraintType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstraintType::NotNull => write!(f, "NOT NULL"),
            ConstraintType::PrimaryKey => write!(f, "PRIMARY KEY"),
            ConstraintType::Unique => write!(f, "UNIQUE"),
            ConstraintType::Check => write!(f, "CHECK"),
            ConstraintType::ForeignKey => write!(f, "FOREIGN KEY"),
            ConstraintType::Default => write!(f, "DEFAULT"),
        }
    }
}

/// Configuration for constraint validation
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Whether to stop at first violation
    pub fail_fast: bool,

    /// Maximum violations to report
    pub max_violations: usize,

    /// Whether to check foreign keys
    pub check_foreign_keys: bool,

    /// Whether to validate deferred constraints
    pub validate_deferred: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        ValidationConfig {
            fail_fast: false,
            max_violations: 100,
            check_foreign_keys: true,
            validate_deferred: false,
        }
    }
}

/// Constraint validator for checking data integrity
#[derive(Debug)]
pub struct ConstraintValidator {
    config: ValidationConfig,
}

impl ConstraintValidator {
    /// Create a new constraint validator
    pub fn new() -> Self {
        Self::with_config(ValidationConfig::default())
    }

    /// Create a validator with custom config
    pub fn with_config(config: ValidationConfig) -> Self {
        ConstraintValidator { config }
    }

    /// Validate all constraints for a batch of data
    pub fn validate_constraints(
        &self,
        table_meta: &TableMeta,
        batch: &RecordBatch,
        existing_data: Option<&dyn ExistingDataProvider>,
    ) -> Result<ValidationResult, EngineError> {
        let mut result = ValidationResult::success(batch.num_rows());

        for constraint in &table_meta.constraints {
            let constraint_result = self.validate_constraint(
                constraint,
                batch,
                existing_data,
                &table_meta.database,
                &table_meta.name,
            )?;

            result.merge(constraint_result);

            if self.config.fail_fast && !result.valid {
                break;
            }

            if result.violations.len() >= self.config.max_violations {
                break;
            }
        }

        Ok(result)
    }

    /// Validate a single constraint
    pub fn validate_constraint(
        &self,
        constraint: &TableConstraint,
        batch: &RecordBatch,
        existing_data: Option<&dyn ExistingDataProvider>,
        database: &str,
        table: &str,
    ) -> Result<ValidationResult, EngineError> {
        match constraint {
            TableConstraint::NotNull { column } => self.validate_not_null(batch, column, None),
            TableConstraint::PrimaryKey { columns, name } => {
                // Primary key = NOT NULL + UNIQUE
                let mut result = ValidationResult::success(batch.num_rows());

                // Check NOT NULL for each column
                for col in columns {
                    let not_null_result = self.validate_not_null(batch, col, name.clone())?;
                    result.merge(not_null_result);
                }

                // Check uniqueness
                if result.valid {
                    let unique_result = self.validate_unique(
                        batch,
                        columns,
                        name.clone(),
                        existing_data,
                        database,
                        table,
                    )?;
                    result.merge(unique_result);
                }

                Ok(result)
            }
            TableConstraint::Unique { columns, name } => {
                self.validate_unique(batch, columns, name.clone(), existing_data, database, table)
            }
            TableConstraint::Check { expr, name } => self.validate_check(batch, expr, name.clone()),
            TableConstraint::Default { .. } => {
                // Default constraints don't need validation
                Ok(ValidationResult::success(batch.num_rows()))
            }
            TableConstraint::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
                name,
                on_delete: _,
                on_update: _,
            } => {
                if self.config.check_foreign_keys {
                    self.validate_foreign_key(
                        batch,
                        columns,
                        referenced_table,
                        referenced_columns,
                        name.clone(),
                        existing_data,
                        database,
                    )
                } else {
                    Ok(ValidationResult::success(batch.num_rows()))
                }
            }
        }
    }

    /// Validate NOT NULL constraint
    fn validate_not_null(
        &self,
        batch: &RecordBatch,
        column: &str,
        constraint_name: Option<String>,
    ) -> Result<ValidationResult, EngineError> {
        let col_idx = batch.schema().index_of(column).map_err(|_| {
            EngineError::NotFound(format!("Column '{}' not found in batch", column))
        })?;

        let array = batch.column(col_idx);
        let null_count = array.null_count();

        if null_count == 0 {
            return Ok(ValidationResult::success(batch.num_rows()));
        }

        // Find null row indices
        let mut null_rows = Vec::new();
        for i in 0..batch.num_rows() {
            if array.is_null(i) {
                null_rows.push(i);
                if null_rows.len() >= self.config.max_violations {
                    break;
                }
            }
        }

        let name = constraint_name.unwrap_or_else(|| format!("{}_not_null", column));
        let violation = ConstraintViolation {
            constraint_name: name,
            constraint_type: ConstraintType::NotNull,
            columns: vec![column.to_string()],
            row_indices: null_rows.clone(),
            message: format!(
                "Column '{}' cannot contain NULL values ({} rows violated)",
                column,
                null_rows.len()
            ),
        };

        Ok(ValidationResult::failure(vec![violation], batch.num_rows()))
    }

    /// Validate UNIQUE constraint
    fn validate_unique(
        &self,
        batch: &RecordBatch,
        columns: &[String],
        constraint_name: Option<String>,
        existing_data: Option<&dyn ExistingDataProvider>,
        database: &str,
        table: &str,
    ) -> Result<ValidationResult, EngineError> {
        // Build composite key for each row
        let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut duplicate_rows = Vec::new();

        // Get column arrays
        let col_arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|col| {
                let idx = batch
                    .schema()
                    .index_of(col)
                    .map_err(|_| EngineError::NotFound(format!("Column '{}' not found", col)))?;
                Ok(batch.column(idx).clone())
            })
            .collect::<Result<_, EngineError>>()?;

        for row_idx in 0..batch.num_rows() {
            // Build key from column values
            let mut key = Vec::new();
            for array in &col_arrays {
                // Serialize value to bytes for comparison
                // This is simplified - a real implementation would handle types properly
                let value_bytes = self.array_value_to_bytes(array, row_idx);
                key.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
                key.extend_from_slice(&value_bytes);
            }

            // Check for duplicates within batch
            if let Some(&existing_row) = seen.get(&key) {
                duplicate_rows.push((row_idx, existing_row));
                if duplicate_rows.len() >= self.config.max_violations {
                    break;
                }
            } else {
                seen.insert(key.clone(), row_idx);
            }

            // Check against existing data
            if let Some(provider) = existing_data {
                if provider.key_exists(database, table, columns, &key)? {
                    duplicate_rows.push((row_idx, usize::MAX)); // MAX indicates existing data
                    if duplicate_rows.len() >= self.config.max_violations {
                        break;
                    }
                }
            }
        }

        if duplicate_rows.is_empty() {
            return Ok(ValidationResult::success(batch.num_rows()));
        }

        let name = constraint_name.unwrap_or_else(|| format!("{}_unique", columns.join("_")));
        let violation = ConstraintViolation {
            constraint_name: name,
            constraint_type: ConstraintType::Unique,
            columns: columns.to_vec(),
            row_indices: duplicate_rows.iter().map(|(r, _)| *r).collect(),
            message: format!(
                "Duplicate values found for columns [{}] ({} duplicates)",
                columns.join(", "),
                duplicate_rows.len()
            ),
        };

        Ok(ValidationResult::failure(vec![violation], batch.num_rows()))
    }

    /// Validate CHECK constraint
    fn validate_check(
        &self,
        batch: &RecordBatch,
        expr: &str,
        constraint_name: Option<String>,
    ) -> Result<ValidationResult, EngineError> {
        // Parse and evaluate the check expression
        // This is a simplified implementation - a real one would use the SQL expression parser

        // For now, support simple expressions like "column > 0" or "column IS NOT NULL"
        let failing_rows = self.evaluate_check_expr(batch, expr)?;

        if failing_rows.is_empty() {
            return Ok(ValidationResult::success(batch.num_rows()));
        }

        let name = constraint_name.unwrap_or_else(|| "check_constraint".to_string());
        let violation = ConstraintViolation {
            constraint_name: name,
            constraint_type: ConstraintType::Check,
            columns: vec![], // Would extract from expression
            row_indices: failing_rows.clone(),
            message: format!(
                "CHECK constraint '{}' violated by {} rows",
                expr,
                failing_rows.len()
            ),
        };

        Ok(ValidationResult::failure(vec![violation], batch.num_rows()))
    }

    /// Validate FOREIGN KEY constraint
    fn validate_foreign_key(
        &self,
        batch: &RecordBatch,
        columns: &[String],
        referenced_table: &str,
        referenced_columns: &[String],
        constraint_name: Option<String>,
        existing_data: Option<&dyn ExistingDataProvider>,
        database: &str,
    ) -> Result<ValidationResult, EngineError> {
        let provider = existing_data.ok_or_else(|| {
            EngineError::Internal(
                "Foreign key validation requires existing data provider".to_string(),
            )
        })?;

        let col_arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|col| {
                let idx = batch
                    .schema()
                    .index_of(col)
                    .map_err(|_| EngineError::NotFound(format!("Column '{}' not found", col)))?;
                Ok(batch.column(idx).clone())
            })
            .collect::<Result<_, EngineError>>()?;

        let mut missing_refs = Vec::new();

        for row_idx in 0..batch.num_rows() {
            // Skip if any column is NULL (NULLs don't violate FK)
            let has_null = col_arrays.iter().any(|a| a.is_null(row_idx));
            if has_null {
                continue;
            }

            // Build reference key
            let mut key = Vec::new();
            for array in &col_arrays {
                let value_bytes = self.array_value_to_bytes(array, row_idx);
                key.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
                key.extend_from_slice(&value_bytes);
            }

            // Check if referenced row exists
            if !provider.key_exists(database, referenced_table, referenced_columns, &key)? {
                missing_refs.push(row_idx);
                if missing_refs.len() >= self.config.max_violations {
                    break;
                }
            }
        }

        if missing_refs.is_empty() {
            return Ok(ValidationResult::success(batch.num_rows()));
        }

        let name = constraint_name
            .unwrap_or_else(|| format!("fk_{}_{}", columns.join("_"), referenced_table));
        let violation = ConstraintViolation {
            constraint_name: name,
            constraint_type: ConstraintType::ForeignKey,
            columns: columns.to_vec(),
            row_indices: missing_refs.clone(),
            message: format!(
                "Foreign key constraint violated: {} rows reference non-existent values in {}.{}",
                missing_refs.len(),
                referenced_table,
                referenced_columns.join(", ")
            ),
        };

        Ok(ValidationResult::failure(vec![violation], batch.num_rows()))
    }

    // Helper methods

    fn array_value_to_bytes(&self, array: &ArrayRef, row: usize) -> Vec<u8> {
        if array.is_null(row) {
            return vec![0u8]; // NULL marker
        }

        // Simplified serialization - real implementation would be more robust
        use arrow::datatypes::DataType;
        match array.data_type() {
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                arr.value(row).to_le_bytes().to_vec()
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                arr.value(row).to_le_bytes().to_vec()
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                arr.value(row).as_bytes().to_vec()
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                arr.value(row).to_le_bytes().to_vec()
            }
            _ => {
                // Fallback: use debug representation
                format!("{:?}", array).into_bytes()
            }
        }
    }

    fn evaluate_check_expr(
        &self,
        batch: &RecordBatch,
        expr: &str,
    ) -> Result<Vec<usize>, EngineError> {
        let mut failing_rows = Vec::new();

        // Simple expression parser for common patterns
        let expr = expr.trim();

        // Handle "column > value" pattern
        if let Some((col, op, val)) = self.parse_comparison(expr) {
            let col_idx = batch
                .schema()
                .index_of(&col)
                .map_err(|_| EngineError::NotFound(format!("Column '{}' not found", col)))?;
            let array = batch.column(col_idx);

            for row in 0..batch.num_rows() {
                if !self.check_comparison(array, row, &op, &val)? {
                    failing_rows.push(row);
                    if failing_rows.len() >= self.config.max_violations {
                        break;
                    }
                }
            }
        } else if expr.to_uppercase().contains("IS NOT NULL") {
            // Handle "column IS NOT NULL"
            let col = expr
                .to_uppercase()
                .replace("IS NOT NULL", "")
                .trim()
                .to_string();
            if let Ok(col_idx) = batch.schema().index_of(&col) {
                let array = batch.column(col_idx);
                for row in 0..batch.num_rows() {
                    if array.is_null(row) {
                        failing_rows.push(row);
                    }
                }
            }
        } else if expr.to_uppercase().contains("IN") {
            // Handle "column IN (value1, value2, ...)"
            // Simplified - would need full parser
            tracing::debug!("Complex CHECK expression not fully evaluated: {}", expr);
        } else {
            tracing::warn!("Unsupported CHECK expression: {}", expr);
        }

        Ok(failing_rows)
    }

    fn parse_comparison(&self, expr: &str) -> Option<(String, String, String)> {
        let operators = [">=", "<=", "!=", "<>", ">", "<", "="];
        for op in operators {
            if let Some(pos) = expr.find(op) {
                let col = expr[..pos].trim().to_string();
                let val = expr[pos + op.len()..].trim().to_string();
                return Some((col, op.to_string(), val));
            }
        }
        None
    }

    fn check_comparison(
        &self,
        array: &ArrayRef,
        row: usize,
        op: &str,
        val: &str,
    ) -> Result<bool, EngineError> {
        if array.is_null(row) {
            return Ok(false); // NULL comparisons are false
        }

        use arrow::datatypes::DataType;
        match array.data_type() {
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let v = arr.value(row);
                let compare_val: i64 = val.parse().unwrap_or(0);
                Ok(match op {
                    ">" => v > compare_val,
                    ">=" => v >= compare_val,
                    "<" => v < compare_val,
                    "<=" => v <= compare_val,
                    "=" => v == compare_val,
                    "!=" | "<>" => v != compare_val,
                    _ => true,
                })
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let v = arr.value(row);
                let compare_val: f64 = val.parse().unwrap_or(0.0);
                Ok(match op {
                    ">" => v > compare_val,
                    ">=" => v >= compare_val,
                    "<" => v < compare_val,
                    "<=" => v <= compare_val,
                    "=" => (v - compare_val).abs() < f64::EPSILON,
                    "!=" | "<>" => (v - compare_val).abs() >= f64::EPSILON,
                    _ => true,
                })
            }
            _ => Ok(true), // Unsupported type, assume passes
        }
    }
}

impl Default for ConstraintValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for providing existing data for constraint validation
pub trait ExistingDataProvider {
    /// Check if a key exists in the table
    fn key_exists(
        &self,
        database: &str,
        table: &str,
        columns: &[String],
        key: &[u8],
    ) -> Result<bool, EngineError>;

    /// Get values for foreign key validation
    fn get_values(
        &self,
        database: &str,
        table: &str,
        columns: &[String],
    ) -> Result<HashSet<Vec<u8>>, EngineError>;
}

/// Simple in-memory data provider for testing
pub struct InMemoryDataProvider {
    data: HashMap<(String, String), HashSet<Vec<u8>>>,
}

impl InMemoryDataProvider {
    pub fn new() -> Self {
        InMemoryDataProvider {
            data: HashMap::new(),
        }
    }

    pub fn add_key(&mut self, database: &str, table: &str, key: Vec<u8>) {
        self.data
            .entry((database.to_string(), table.to_string()))
            .or_insert_with(HashSet::new)
            .insert(key);
    }
}

impl Default for InMemoryDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExistingDataProvider for InMemoryDataProvider {
    fn key_exists(
        &self,
        database: &str,
        table: &str,
        _columns: &[String],
        key: &[u8],
    ) -> Result<bool, EngineError> {
        Ok(self
            .data
            .get(&(database.to_string(), table.to_string()))
            .map(|keys| keys.contains(key))
            .unwrap_or(false))
    }

    fn get_values(
        &self,
        database: &str,
        table: &str,
        _columns: &[String],
    ) -> Result<HashSet<Vec<u8>>, EngineError> {
        Ok(self
            .data
            .get(&(database.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let id = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let name = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            None,
            Some("Dave"),
            Some("Eve"),
        ]));
        let value = Arc::new(Int64Array::from(vec![
            Some(100),
            Some(200),
            Some(300),
            None,
            Some(500),
        ]));

        RecordBatch::try_new(schema, vec![id, name, value]).unwrap()
    }

    fn create_test_table_meta() -> TableMeta {
        TableMeta {
            database: "test".to_string(),
            name: "users".to_string(),
            schema_json: None,
            compression: None,
            deduplication: None,
            constraints: vec![],
        }
    }

    #[test]
    fn test_not_null_pass() {
        let validator = ConstraintValidator::new();
        let batch = create_test_batch();

        let result = validator.validate_not_null(&batch, "id", None).unwrap();

        assert!(result.valid);
    }

    #[test]
    fn test_not_null_fail() {
        let validator = ConstraintValidator::new();
        let batch = create_test_batch();

        let result = validator.validate_not_null(&batch, "name", None).unwrap();

        assert!(!result.valid);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].row_indices, vec![2]);
    }

    #[test]
    fn test_unique_pass() {
        let validator = ConstraintValidator::new();
        let batch = create_test_batch();

        let result = validator
            .validate_unique(&batch, &["id".to_string()], None, None, "test", "users")
            .unwrap();

        assert!(result.valid);
    }

    #[test]
    fn test_unique_fail_duplicates() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let id = Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2])); // Duplicates
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

        let validator = ConstraintValidator::new();
        let result = validator
            .validate_unique(&batch, &["id".to_string()], None, None, "test", "users")
            .unwrap();

        assert!(!result.valid);
        assert!(!result.violations.is_empty());
    }

    #[test]
    fn test_check_constraint() {
        let validator = ConstraintValidator::new();

        // Create a batch without NULL values for this test
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let value = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        let batch = RecordBatch::try_new(schema, vec![value]).unwrap();

        // All values >= 0
        let result = validator
            .validate_check(&batch, "value >= 0", None)
            .unwrap();
        assert!(result.valid);

        // Check for > 200 (some fail: 100, 200)
        let result = validator
            .validate_check(&batch, "value > 200", None)
            .unwrap();
        assert!(!result.valid);
        assert_eq!(result.violations[0].row_indices.len(), 2); // rows 0 and 1 fail
    }

    #[test]
    fn test_full_constraint_validation() {
        let validator = ConstraintValidator::new();
        let batch = create_test_batch();
        let mut meta = create_test_table_meta();

        meta.constraints = vec![
            TableConstraint::NotNull {
                column: "id".to_string(),
            },
            TableConstraint::Unique {
                columns: vec!["id".to_string()],
                name: Some("pk_id".to_string()),
            },
        ];

        let result = validator.validate_constraints(&meta, &batch, None).unwrap();

        assert!(result.valid);
    }

    #[test]
    fn test_foreign_key() {
        let validator = ConstraintValidator::new();

        // Helper to build key in the same format as validate_foreign_key
        fn build_fk_key(value: i64) -> Vec<u8> {
            let value_bytes = value.to_le_bytes();
            let mut key = Vec::new();
            key.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            key.extend_from_slice(&value_bytes);
            key
        }

        // Create parent table data with keys in the correct format
        let mut provider = InMemoryDataProvider::new();
        provider.add_key("test", "parent", build_fk_key(1));
        provider.add_key("test", "parent", build_fk_key(2));

        // Create child batch with FK references
        let schema = Arc::new(Schema::new(vec![Field::new(
            "parent_id",
            DataType::Int64,
            true,
        )]));
        let parent_id = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(999), // Invalid reference
        ]));
        let batch = RecordBatch::try_new(schema, vec![parent_id]).unwrap();

        let result = validator
            .validate_foreign_key(
                &batch,
                &["parent_id".to_string()],
                "parent",
                &["id".to_string()],
                None,
                Some(&provider),
                "test",
            )
            .unwrap();

        assert!(!result.valid);
        assert_eq!(result.violations[0].row_indices, vec![2]);
    }
}
