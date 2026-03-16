//! Column Masking Module
//!
//! Provides dynamic data masking for sensitive columns:
//! - Full masking (replace with constant)
//! - Partial masking (show first/last N characters)
//! - Email masking (show domain only)
//! - Credit card masking (show last 4 digits)
//! - Phone number masking
//! - Custom regex-based masking
//! - Role-based masking policies

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Masking configuration
#[derive(Clone, Debug)]
pub struct MaskingConfig {
    /// Enable masking
    pub enabled: bool,
    /// Default mask character
    pub mask_char: char,
    /// Default mask for NULL values
    pub null_mask: String,
    /// Log masked access
    pub log_access: bool,
}

impl Default for MaskingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mask_char: '*',
            null_mask: "NULL".to_string(),
            log_access: false,
        }
    }
}

/// Masking function type
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MaskingFunction {
    /// Full masking - replace entire value
    Full,
    /// Partial masking - show first N and last M characters
    Partial { show_first: usize, show_last: usize },
    /// Email masking - show domain only
    Email,
    /// Credit card - show last 4 digits
    CreditCard,
    /// Phone number - show last 4 digits
    Phone,
    /// SSN/Tax ID - show last 4 digits
    SocialSecurity,
    /// Date - show year only
    DateYearOnly,
    /// Numeric - round to nearest N
    NumericRound { precision: i32 },
    /// Numeric - randomize within range
    NumericRandomize { variance_percent: f64 },
    /// Hash the value (one-way)
    Hash,
    /// Custom regex replacement
    Regex {
        pattern: String,
        replacement: String,
    },
    /// Replace with constant
    Constant { value: String },
    /// Shuffle characters
    Shuffle,
    /// Tokenize (reversible with key)
    Tokenize,
}

/// Column masking policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaskingPolicy {
    /// Policy name
    pub name: String,
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Column name
    pub column: String,
    /// Masking function
    pub function: MaskingFunction,
    /// Roles that can see unmasked data
    pub exempt_roles: Vec<String>,
    /// Users that can see unmasked data
    pub exempt_users: Vec<String>,
    /// Priority (higher = applied first)
    pub priority: i32,
    /// Enable policy
    pub enabled: bool,
    /// Description
    pub description: Option<String>,
}

/// Result of masking operation
#[derive(Clone, Debug)]
pub struct MaskResult {
    /// Original value (for logging)
    pub original_hash: Option<u64>,
    /// Masked value
    pub masked_value: String,
    /// Whether masking was applied
    pub was_masked: bool,
    /// Policy that was applied
    pub policy_name: Option<String>,
}

/// Column masking engine
pub struct MaskingEngine {
    config: MaskingConfig,
    /// Policies by (database.table.column)
    policies: RwLock<HashMap<String, Vec<MaskingPolicy>>>,
    /// Compiled regex cache
    regex_cache: RwLock<HashMap<String, Regex>>,
    /// Tokenization keys (for reversible masking)
    token_keys: RwLock<HashMap<String, Vec<u8>>>,
}

impl MaskingEngine {
    pub fn new(config: MaskingConfig) -> Self {
        Self {
            config,
            policies: RwLock::new(HashMap::new()),
            regex_cache: RwLock::new(HashMap::new()),
            token_keys: RwLock::new(HashMap::new()),
        }
    }

    /// Add a masking policy
    pub fn add_policy(&self, policy: MaskingPolicy) {
        let key = format!("{}.{}.{}", policy.database, policy.table, policy.column);
        let mut policies = self.policies.write();
        let entry = policies.entry(key).or_insert_with(Vec::new);
        entry.push(policy);
        entry.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a masking policy
    pub fn remove_policy(&self, database: &str, table: &str, column: &str, policy_name: &str) {
        let key = format!("{}.{}.{}", database, table, column);
        let mut policies = self.policies.write();
        if let Some(entry) = policies.get_mut(&key) {
            entry.retain(|p| p.name != policy_name);
        }
    }

    /// Get policies for a column
    pub fn get_policies(&self, database: &str, table: &str, column: &str) -> Vec<MaskingPolicy> {
        let key = format!("{}.{}.{}", database, table, column);
        self.policies.read().get(&key).cloned().unwrap_or_default()
    }

    /// Apply masking to a value
    pub fn mask(
        &self,
        database: &str,
        table: &str,
        column: &str,
        value: &str,
        user: Option<&str>,
        roles: &[String],
    ) -> MaskResult {
        if !self.config.enabled {
            return MaskResult {
                original_hash: None,
                masked_value: value.to_string(),
                was_masked: false,
                policy_name: None,
            };
        }

        let key = format!("{}.{}.{}", database, table, column);
        let policies = self.policies.read();

        if let Some(column_policies) = policies.get(&key) {
            for policy in column_policies {
                if !policy.enabled {
                    continue;
                }

                // Check exemptions
                if let Some(u) = user {
                    if policy.exempt_users.contains(&u.to_string()) {
                        continue;
                    }
                }

                if roles.iter().any(|r| policy.exempt_roles.contains(r)) {
                    continue;
                }

                // Apply masking
                let masked = self.apply_function(&policy.function, value);
                return MaskResult {
                    original_hash: Some(self.hash_value(value)),
                    masked_value: masked,
                    was_masked: true,
                    policy_name: Some(policy.name.clone()),
                };
            }
        }

        MaskResult {
            original_hash: None,
            masked_value: value.to_string(),
            was_masked: false,
            policy_name: None,
        }
    }

    /// Apply masking function to a value
    fn apply_function(&self, function: &MaskingFunction, value: &str) -> String {
        match function {
            MaskingFunction::Full => self
                .config
                .mask_char
                .to_string()
                .repeat(value.len().min(10)),

            MaskingFunction::Partial {
                show_first,
                show_last,
            } => {
                let len = value.len();
                if len <= show_first + show_last {
                    return self.config.mask_char.to_string().repeat(len);
                }
                let first: String = value.chars().take(*show_first).collect();
                let last: String = value.chars().skip(len - show_last).collect();
                let middle = self
                    .config
                    .mask_char
                    .to_string()
                    .repeat(len - show_first - show_last);
                format!("{}{}{}", first, middle, last)
            }

            MaskingFunction::Email => {
                if let Some(at_pos) = value.find('@') {
                    let domain = &value[at_pos..];
                    let masked_local = self.config.mask_char.to_string().repeat(at_pos.min(5));
                    format!("{}{}", masked_local, domain)
                } else {
                    self.config
                        .mask_char
                        .to_string()
                        .repeat(value.len().min(10))
                }
            }

            MaskingFunction::CreditCard => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    let last_four = &digits[digits.len() - 4..];
                    format!("****-****-****-{}", last_four)
                } else {
                    "****-****-****-****".to_string()
                }
            }

            MaskingFunction::Phone => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    let last_four = &digits[digits.len() - 4..];
                    format!("(***) ***-{}", last_four)
                } else {
                    "(***) ***-****".to_string()
                }
            }

            MaskingFunction::SocialSecurity => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    let last_four = &digits[digits.len() - 4..];
                    format!("***-**-{}", last_four)
                } else {
                    "***-**-****".to_string()
                }
            }

            MaskingFunction::DateYearOnly => {
                // Extract year from various date formats
                if value.len() >= 4 {
                    let year = &value[..4];
                    if year.chars().all(|c| c.is_ascii_digit()) {
                        return format!("{}-**-**", year);
                    }
                }
                "****-**-**".to_string()
            }

            MaskingFunction::NumericRound { precision } => {
                if let Ok(num) = value.parse::<f64>() {
                    let factor = 10f64.powi(*precision);
                    let rounded = (num / factor).round() * factor;
                    format!("{}", rounded as i64)
                } else {
                    value.to_string()
                }
            }

            MaskingFunction::NumericRandomize { variance_percent } => {
                if let Ok(num) = value.parse::<f64>() {
                    // Deterministic "random" based on hash
                    let hash = self.hash_value(value);
                    let variance = num * variance_percent / 100.0;
                    let offset = ((hash % 1000) as f64 / 500.0 - 1.0) * variance;
                    format!("{:.2}", num + offset)
                } else {
                    value.to_string()
                }
            }

            MaskingFunction::Hash => {
                let hash = self.hash_value(value);
                format!("{:016x}", hash)
            }

            MaskingFunction::Regex {
                pattern,
                replacement,
            } => {
                let mut cache = self.regex_cache.write();
                let re = cache.entry(pattern.clone()).or_insert_with(|| {
                    Regex::new(pattern).unwrap_or_else(|_| Regex::new(".*").unwrap())
                });
                re.replace_all(value, replacement.as_str()).to_string()
            }

            MaskingFunction::Constant { value: constant } => constant.clone(),

            MaskingFunction::Shuffle => {
                // Deterministic shuffle based on hash
                let mut chars: Vec<char> = value.chars().collect();
                let hash = self.hash_value(value);
                let len = chars.len();
                if len > 1 {
                    for i in 0..len {
                        let j = ((hash >> (i % 8)) as usize + i) % len;
                        chars.swap(i, j);
                    }
                }
                chars.into_iter().collect()
            }

            MaskingFunction::Tokenize => {
                // Generate deterministic token
                let hash = self.hash_value(value);
                format!("TOK_{:016x}", hash)
            }
        }
    }

    /// Hash a value for logging/comparison
    fn hash_value(&self, value: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Get all policies
    pub fn list_policies(&self) -> Vec<MaskingPolicy> {
        self.policies
            .read()
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect()
    }

    /// Check if a column has masking policies
    pub fn has_policy(&self, database: &str, table: &str, column: &str) -> bool {
        let key = format!("{}.{}.{}", database, table, column);
        self.policies
            .read()
            .get(&key)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
    }
}

/// Predefined masking policies for common data types
pub struct PredefinedPolicies;

impl PredefinedPolicies {
    /// Create email masking policy
    pub fn email(database: &str, table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            name: format!("{}_email_mask", column),
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            function: MaskingFunction::Email,
            exempt_roles: vec!["admin".to_string(), "dba".to_string()],
            exempt_users: vec![],
            priority: 100,
            enabled: true,
            description: Some("Mask email addresses, showing domain only".to_string()),
        }
    }

    /// Create credit card masking policy
    pub fn credit_card(database: &str, table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            name: format!("{}_cc_mask", column),
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            function: MaskingFunction::CreditCard,
            exempt_roles: vec!["billing_admin".to_string()],
            exempt_users: vec![],
            priority: 200,
            enabled: true,
            description: Some("Mask credit card numbers, showing last 4 digits".to_string()),
        }
    }

    /// Create SSN masking policy
    pub fn ssn(database: &str, table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            name: format!("{}_ssn_mask", column),
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            function: MaskingFunction::SocialSecurity,
            exempt_roles: vec!["hr_admin".to_string()],
            exempt_users: vec![],
            priority: 200,
            enabled: true,
            description: Some("Mask SSN, showing last 4 digits".to_string()),
        }
    }

    /// Create phone masking policy
    pub fn phone(database: &str, table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            name: format!("{}_phone_mask", column),
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            function: MaskingFunction::Phone,
            exempt_roles: vec!["support".to_string()],
            exempt_users: vec![],
            priority: 100,
            enabled: true,
            description: Some("Mask phone numbers, showing last 4 digits".to_string()),
        }
    }

    /// Create name masking policy (partial)
    pub fn name(database: &str, table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            name: format!("{}_name_mask", column),
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            function: MaskingFunction::Partial {
                show_first: 1,
                show_last: 0,
            },
            exempt_roles: vec![],
            exempt_users: vec![],
            priority: 50,
            enabled: true,
            description: Some("Mask names, showing first initial".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_masking() {
        let engine = MaskingEngine::new(MaskingConfig::default());

        engine.add_policy(MaskingPolicy {
            name: "test_full".to_string(),
            database: "db".to_string(),
            table: "users".to_string(),
            column: "password".to_string(),
            function: MaskingFunction::Full,
            exempt_roles: vec![],
            exempt_users: vec![],
            priority: 100,
            enabled: true,
            description: None,
        });

        let result = engine.mask("db", "users", "password", "secret123", None, &[]);
        assert!(result.was_masked);
        assert!(!result.masked_value.contains("secret"));
    }

    #[test]
    fn test_email_masking() {
        let engine = MaskingEngine::new(MaskingConfig::default());
        engine.add_policy(PredefinedPolicies::email("db", "users", "email"));

        let result = engine.mask("db", "users", "email", "john.doe@example.com", None, &[]);
        assert!(result.was_masked);
        assert!(result.masked_value.contains("@example.com"));
        assert!(!result.masked_value.contains("john"));
    }

    #[test]
    fn test_credit_card_masking() {
        let engine = MaskingEngine::new(MaskingConfig::default());
        engine.add_policy(PredefinedPolicies::credit_card(
            "db",
            "payments",
            "card_number",
        ));

        let result = engine.mask(
            "db",
            "payments",
            "card_number",
            "4111111111111234",
            None,
            &[],
        );
        assert!(result.was_masked);
        assert!(result.masked_value.contains("1234"));
        assert!(result.masked_value.contains("****"));
    }

    #[test]
    fn test_exempt_role() {
        let engine = MaskingEngine::new(MaskingConfig::default());

        engine.add_policy(MaskingPolicy {
            name: "test".to_string(),
            database: "db".to_string(),
            table: "users".to_string(),
            column: "ssn".to_string(),
            function: MaskingFunction::Full,
            exempt_roles: vec!["admin".to_string()],
            exempt_users: vec![],
            priority: 100,
            enabled: true,
            description: None,
        });

        // Regular user - should be masked
        let result = engine.mask(
            "db",
            "users",
            "ssn",
            "123-45-6789",
            None,
            &["user".to_string()],
        );
        assert!(result.was_masked);

        // Admin - should not be masked
        let result = engine.mask(
            "db",
            "users",
            "ssn",
            "123-45-6789",
            None,
            &["admin".to_string()],
        );
        assert!(!result.was_masked);
        assert_eq!(result.masked_value, "123-45-6789");
    }

    #[test]
    fn test_partial_masking() {
        let engine = MaskingEngine::new(MaskingConfig::default());

        engine.add_policy(MaskingPolicy {
            name: "test".to_string(),
            database: "db".to_string(),
            table: "users".to_string(),
            column: "name".to_string(),
            function: MaskingFunction::Partial {
                show_first: 2,
                show_last: 1,
            },
            exempt_roles: vec![],
            exempt_users: vec![],
            priority: 100,
            enabled: true,
            description: None,
        });

        let result = engine.mask("db", "users", "name", "Johnson", None, &[]);
        assert!(result.was_masked);
        assert!(result.masked_value.starts_with("Jo"));
        assert!(result.masked_value.ends_with("n"));
        assert!(result.masked_value.contains("*"));
    }
}
