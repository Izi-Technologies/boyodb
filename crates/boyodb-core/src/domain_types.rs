//! Domain Types - CREATE DOMAIN with Constraints
//!
//! This module provides:
//! - Custom domain type definitions
//! - Domain constraints (CHECK, NOT NULL, DEFAULT)
//! - Domain inheritance and composition

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// DOMAIN DEFINITION
// ============================================================================

/// Domain type definition
#[derive(Debug, Clone)]
pub struct Domain {
    /// Schema name
    pub schema: String,
    /// Domain name
    pub name: String,
    /// Base data type
    pub base_type: String,
    /// Type modifier (e.g., length for varchar)
    pub type_mod: Option<i32>,
    /// Collation
    pub collation: Option<String>,
    /// Default value expression
    pub default_expr: Option<String>,
    /// NOT NULL constraint
    pub not_null: bool,
    /// CHECK constraints
    pub constraints: Vec<DomainConstraint>,
    /// Owner
    pub owner: Option<String>,
    /// Comment
    pub comment: Option<String>,
}

impl Domain {
    /// Create a new domain
    pub fn new(schema: &str, name: &str, base_type: &str) -> Self {
        Self {
            schema: schema.to_string(),
            name: name.to_string(),
            base_type: base_type.to_string(),
            type_mod: None,
            collation: None,
            default_expr: None,
            not_null: false,
            constraints: Vec::new(),
            owner: None,
            comment: None,
        }
    }

    /// Get qualified name
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Set NOT NULL
    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    /// Set default value
    pub fn with_default(mut self, expr: &str) -> Self {
        self.default_expr = Some(expr.to_string());
        self
    }

    /// Add CHECK constraint
    pub fn with_check(mut self, name: &str, expression: &str) -> Self {
        self.constraints
            .push(DomainConstraint::check(name, expression));
        self
    }

    /// Set collation
    pub fn with_collation(mut self, collation: &str) -> Self {
        self.collation = Some(collation.to_string());
        self
    }

    /// Set type modifier
    pub fn with_type_mod(mut self, type_mod: i32) -> Self {
        self.type_mod = Some(type_mod);
        self
    }

    /// Validate a value against domain constraints
    pub fn validate(&self, value: &DomainValue) -> Result<(), DomainError> {
        // Check NOT NULL
        if self.not_null && value.is_null {
            return Err(DomainError::NullViolation {
                domain: self.qualified_name(),
            });
        }

        // Check constraints
        for constraint in &self.constraints {
            if !constraint.evaluate(value)? {
                return Err(DomainError::ConstraintViolation {
                    domain: self.qualified_name(),
                    constraint: constraint.name.clone(),
                });
            }
        }

        Ok(())
    }

    /// Generate CREATE DOMAIN SQL
    pub fn to_sql(&self) -> String {
        let mut sql = format!(
            "CREATE DOMAIN {}.{} AS {}",
            self.schema, self.name, self.base_type
        );

        if let Some(type_mod) = self.type_mod {
            sql.push_str(&format!("({})", type_mod));
        }

        if let Some(ref collation) = self.collation {
            sql.push_str(&format!(" COLLATE {}", collation));
        }

        if let Some(ref default_expr) = self.default_expr {
            sql.push_str(&format!(" DEFAULT {}", default_expr));
        }

        if self.not_null {
            sql.push_str(" NOT NULL");
        }

        for constraint in &self.constraints {
            sql.push_str(&format!(
                " CONSTRAINT {} CHECK ({})",
                constraint.name, constraint.expression
            ));
        }

        sql
    }
}

/// Domain constraint
#[derive(Debug, Clone)]
pub struct DomainConstraint {
    /// Constraint name
    pub name: String,
    /// CHECK expression
    pub expression: String,
    /// Is NOT VALID (not enforced on existing data)
    pub not_valid: bool,
}

impl DomainConstraint {
    /// Create a CHECK constraint
    pub fn check(name: &str, expression: &str) -> Self {
        Self {
            name: name.to_string(),
            expression: expression.to_string(),
            not_valid: false,
        }
    }

    /// Mark as NOT VALID
    pub fn not_valid(mut self) -> Self {
        self.not_valid = true;
        self
    }

    /// Check if value satisfies constraint
    pub fn evaluate(&self, value: &DomainValue) -> Result<bool, DomainError> {
        if value.is_null {
            // NULL passes CHECK constraints
            return Ok(true);
        }

        // Simple expression evaluation
        // In real implementation, would use a proper expression evaluator
        let expr = self.expression.to_uppercase();

        // Handle VALUE keyword replacement
        if let Some(ref val) = value.value {
            // Simple pattern matching for common constraints
            if expr.contains("VALUE >") {
                if let Some(threshold) = extract_number(&expr, "VALUE >") {
                    if let Ok(v) = val.parse::<f64>() {
                        return Ok(v > threshold);
                    }
                }
            }
            if expr.contains("VALUE >=") {
                if let Some(threshold) = extract_number(&expr, "VALUE >=") {
                    if let Ok(v) = val.parse::<f64>() {
                        return Ok(v >= threshold);
                    }
                }
            }
            if expr.contains("VALUE <") {
                if let Some(threshold) = extract_number(&expr, "VALUE <") {
                    if let Ok(v) = val.parse::<f64>() {
                        return Ok(v < threshold);
                    }
                }
            }
            if expr.contains("VALUE <=") {
                if let Some(threshold) = extract_number(&expr, "VALUE <=") {
                    if let Ok(v) = val.parse::<f64>() {
                        return Ok(v <= threshold);
                    }
                }
            }
            if expr.contains("LENGTH(VALUE)") {
                // Handle length constraints
                let len = val.len();
                if expr.contains("<=") {
                    if let Some(max) = extract_number(&expr, "<=") {
                        return Ok(len <= max as usize);
                    }
                }
            }
            if expr.contains("VALUE ~") || expr.contains("VALUE LIKE") {
                // Pattern matching - simplified
                return Ok(true);
            }
        }

        // Default to true for unhandled expressions
        Ok(true)
    }
}

fn extract_number(expr: &str, prefix: &str) -> Option<f64> {
    if let Some(pos) = expr.find(prefix) {
        let rest = &expr[pos + prefix.len()..];
        let num_str: String = rest
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_numeric() || *c == '.' || *c == '-')
            .collect();
        num_str.parse().ok()
    } else {
        None
    }
}

/// Value being checked against domain
#[derive(Debug, Clone)]
pub struct DomainValue {
    /// String representation of value
    pub value: Option<String>,
    /// Is NULL
    pub is_null: bool,
}

impl DomainValue {
    /// Create a non-null value
    pub fn value(v: &str) -> Self {
        Self {
            value: Some(v.to_string()),
            is_null: false,
        }
    }

    /// Create a NULL value
    pub fn null() -> Self {
        Self {
            value: None,
            is_null: true,
        }
    }
}

// ============================================================================
// COMMON DOMAIN TEMPLATES
// ============================================================================

/// Common domain type templates
pub struct DomainTemplates;

impl DomainTemplates {
    /// Email address domain
    pub fn email(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "text").not_null().with_check(
            "valid_email",
            "VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'",
        )
    }

    /// Positive integer domain
    pub fn positive_int(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "integer").with_check("positive", "VALUE > 0")
    }

    /// Non-negative integer domain
    pub fn non_negative_int(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "integer").with_check("non_negative", "VALUE >= 0")
    }

    /// Percentage domain (0-100)
    pub fn percentage(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "numeric")
            .with_check("valid_percentage", "VALUE >= 0 AND VALUE <= 100")
    }

    /// URL domain
    pub fn url(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "text").with_check("valid_url", "VALUE ~ '^https?://'")
    }

    /// Phone number domain
    pub fn phone(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "text").with_check("valid_phone", "VALUE ~ '^[+]?[0-9]{10,15}$'")
    }

    /// ISO country code domain
    pub fn country_code(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "char")
            .with_type_mod(2)
            .with_check(
                "valid_country",
                "LENGTH(VALUE) = 2 AND VALUE = UPPER(VALUE)",
            )
    }

    /// Currency code domain
    pub fn currency_code(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "char")
            .with_type_mod(3)
            .with_check(
                "valid_currency",
                "LENGTH(VALUE) = 3 AND VALUE = UPPER(VALUE)",
            )
    }

    /// UUID domain
    pub fn uuid(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "uuid").not_null()
    }

    /// Non-empty text domain
    pub fn non_empty_text(schema: &str, name: &str) -> Domain {
        Domain::new(schema, name, "text")
            .not_null()
            .with_check("non_empty", "LENGTH(TRIM(VALUE)) > 0")
    }
}

// ============================================================================
// DOMAIN MANAGER
// ============================================================================

/// Domain manager
pub struct DomainManager {
    /// Domains by qualified name
    domains: RwLock<HashMap<String, Domain>>,
}

impl DomainManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            domains: RwLock::new(HashMap::new()),
        }
    }

    /// Create a domain
    pub fn create_domain(&self, domain: Domain) -> Result<(), DomainError> {
        let qualified_name = domain.qualified_name();

        let mut domains = self.domains.write();

        if domains.contains_key(&qualified_name) {
            return Err(DomainError::AlreadyExists(qualified_name));
        }

        domains.insert(qualified_name, domain);
        Ok(())
    }

    /// Get a domain
    pub fn get_domain(&self, schema: &str, name: &str) -> Option<Domain> {
        let qualified_name = format!("{}.{}", schema, name);
        let domains = self.domains.read();
        domains.get(&qualified_name).cloned()
    }

    /// Drop a domain
    pub fn drop_domain(&self, schema: &str, name: &str, cascade: bool) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();

        if !domains.contains_key(&qualified_name) {
            return Err(DomainError::NotFound(qualified_name));
        }

        // In real implementation, would check for dependencies if not cascade
        if !cascade {
            // Check if any columns use this domain
            // For now, just proceed
        }

        domains.remove(&qualified_name);
        Ok(())
    }

    /// Alter domain - set default
    pub fn alter_set_default(
        &self,
        schema: &str,
        name: &str,
        default_expr: &str,
    ) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        domain.default_expr = Some(default_expr.to_string());
        Ok(())
    }

    /// Alter domain - drop default
    pub fn alter_drop_default(&self, schema: &str, name: &str) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        domain.default_expr = None;
        Ok(())
    }

    /// Alter domain - set NOT NULL
    pub fn alter_set_not_null(&self, schema: &str, name: &str) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        domain.not_null = true;
        Ok(())
    }

    /// Alter domain - drop NOT NULL
    pub fn alter_drop_not_null(&self, schema: &str, name: &str) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        domain.not_null = false;
        Ok(())
    }

    /// Alter domain - add constraint
    pub fn alter_add_constraint(
        &self,
        schema: &str,
        name: &str,
        constraint: DomainConstraint,
    ) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        // Check for duplicate constraint name
        if domain.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(DomainError::ConstraintExists(constraint.name));
        }

        domain.constraints.push(constraint);
        Ok(())
    }

    /// Alter domain - drop constraint
    pub fn alter_drop_constraint(
        &self,
        schema: &str,
        name: &str,
        constraint_name: &str,
    ) -> Result<(), DomainError> {
        let qualified_name = format!("{}.{}", schema, name);

        let mut domains = self.domains.write();
        let domain = domains
            .get_mut(&qualified_name)
            .ok_or_else(|| DomainError::NotFound(qualified_name))?;

        let orig_len = domain.constraints.len();
        domain.constraints.retain(|c| c.name != constraint_name);

        if domain.constraints.len() == orig_len {
            return Err(DomainError::ConstraintNotFound(constraint_name.to_string()));
        }

        Ok(())
    }

    /// Validate value against domain
    pub fn validate(
        &self,
        schema: &str,
        name: &str,
        value: &DomainValue,
    ) -> Result<(), DomainError> {
        let domain = self
            .get_domain(schema, name)
            .ok_or_else(|| DomainError::NotFound(format!("{}.{}", schema, name)))?;

        domain.validate(value)
    }

    /// List all domains in a schema
    pub fn list_domains(&self, schema: &str) -> Vec<Domain> {
        let domains = self.domains.read();
        domains
            .values()
            .filter(|d| d.schema == schema)
            .cloned()
            .collect()
    }

    /// List all domains
    pub fn list_all_domains(&self) -> Vec<Domain> {
        let domains = self.domains.read();
        domains.values().cloned().collect()
    }
}

impl Default for DomainManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Domain error
#[derive(Debug, Clone)]
pub enum DomainError {
    /// Domain not found
    NotFound(String),
    /// Domain already exists
    AlreadyExists(String),
    /// NULL value violates NOT NULL
    NullViolation { domain: String },
    /// Value violates CHECK constraint
    ConstraintViolation { domain: String, constraint: String },
    /// Constraint already exists
    ConstraintExists(String),
    /// Constraint not found
    ConstraintNotFound(String),
    /// Type mismatch
    TypeMismatch { expected: String, got: String },
    /// Domain in use (cannot drop)
    InUse {
        domain: String,
        used_by: Vec<String>,
    },
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DomainError::NotFound(d) => write!(f, "domain \"{}\" does not exist", d),
            DomainError::AlreadyExists(d) => write!(f, "domain \"{}\" already exists", d),
            DomainError::NullViolation { domain } => {
                write!(f, "domain \"{}\" does not allow null values", domain)
            }
            DomainError::ConstraintViolation { domain, constraint } => {
                write!(
                    f,
                    "value violates check constraint \"{}\" for domain \"{}\"",
                    constraint, domain
                )
            }
            DomainError::ConstraintExists(c) => {
                write!(f, "constraint \"{}\" already exists", c)
            }
            DomainError::ConstraintNotFound(c) => {
                write!(f, "constraint \"{}\" does not exist", c)
            }
            DomainError::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {}, got {}", expected, got)
            }
            DomainError::InUse { domain, used_by } => {
                write!(
                    f,
                    "domain \"{}\" is used by: {}",
                    domain,
                    used_by.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for DomainError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_creation() {
        let domain = Domain::new("public", "positive_int", "integer")
            .not_null()
            .with_check("positive", "VALUE > 0");

        assert_eq!(domain.qualified_name(), "public.positive_int");
        assert!(domain.not_null);
        assert_eq!(domain.constraints.len(), 1);
    }

    #[test]
    fn test_domain_validation_not_null() {
        let domain = Domain::new("public", "test", "integer").not_null();

        assert!(domain.validate(&DomainValue::value("42")).is_ok());
        assert!(domain.validate(&DomainValue::null()).is_err());
    }

    #[test]
    fn test_domain_validation_check() {
        let domain = Domain::new("public", "test", "integer").with_check("positive", "VALUE > 0");

        assert!(domain.validate(&DomainValue::value("42")).is_ok());
        assert!(domain.validate(&DomainValue::value("-1")).is_err());
    }

    #[test]
    fn test_domain_manager() {
        let manager = DomainManager::new();

        let domain = Domain::new("public", "email", "text").not_null();

        manager.create_domain(domain).unwrap();

        let retrieved = manager.get_domain("public", "email").unwrap();
        assert_eq!(retrieved.name, "email");
    }

    #[test]
    fn test_domain_manager_duplicate() {
        let manager = DomainManager::new();

        let domain = Domain::new("public", "test", "integer");
        manager.create_domain(domain.clone()).unwrap();

        let result = manager.create_domain(domain);
        assert!(matches!(result, Err(DomainError::AlreadyExists(_))));
    }

    #[test]
    fn test_domain_alter_default() {
        let manager = DomainManager::new();

        let domain = Domain::new("public", "test", "integer");
        manager.create_domain(domain).unwrap();

        manager.alter_set_default("public", "test", "0").unwrap();

        let retrieved = manager.get_domain("public", "test").unwrap();
        assert_eq!(retrieved.default_expr, Some("0".to_string()));

        manager.alter_drop_default("public", "test").unwrap();
        let retrieved = manager.get_domain("public", "test").unwrap();
        assert!(retrieved.default_expr.is_none());
    }

    #[test]
    fn test_domain_alter_constraint() {
        let manager = DomainManager::new();

        let domain = Domain::new("public", "test", "integer");
        manager.create_domain(domain).unwrap();

        let constraint = DomainConstraint::check("positive", "VALUE > 0");
        manager
            .alter_add_constraint("public", "test", constraint)
            .unwrap();

        let retrieved = manager.get_domain("public", "test").unwrap();
        assert_eq!(retrieved.constraints.len(), 1);

        manager
            .alter_drop_constraint("public", "test", "positive")
            .unwrap();
        let retrieved = manager.get_domain("public", "test").unwrap();
        assert_eq!(retrieved.constraints.len(), 0);
    }

    #[test]
    fn test_domain_templates() {
        let email = DomainTemplates::email("public", "email_address");
        assert!(email.not_null);
        assert!(!email.constraints.is_empty());

        let percentage = DomainTemplates::percentage("public", "pct");
        assert_eq!(percentage.base_type, "numeric");
    }

    #[test]
    fn test_domain_to_sql() {
        let domain = Domain::new("public", "positive_int", "integer")
            .not_null()
            .with_default("0")
            .with_check("positive", "VALUE > 0");

        let sql = domain.to_sql();
        assert!(sql.contains("CREATE DOMAIN public.positive_int"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("DEFAULT 0"));
        assert!(sql.contains("CHECK"));
    }

    #[test]
    fn test_domain_drop() {
        let manager = DomainManager::new();

        let domain = Domain::new("public", "test", "integer");
        manager.create_domain(domain).unwrap();

        manager.drop_domain("public", "test", false).unwrap();

        assert!(manager.get_domain("public", "test").is_none());
    }

    #[test]
    fn test_list_domains() {
        let manager = DomainManager::new();

        manager
            .create_domain(Domain::new("public", "d1", "integer"))
            .unwrap();
        manager
            .create_domain(Domain::new("public", "d2", "text"))
            .unwrap();
        manager
            .create_domain(Domain::new("other", "d3", "boolean"))
            .unwrap();

        let public_domains = manager.list_domains("public");
        assert_eq!(public_domains.len(), 2);

        let all_domains = manager.list_all_domains();
        assert_eq!(all_domains.len(), 3);
    }
}
