//! Rules System - PostgreSQL-style Rewrite Rules
//!
//! This module provides:
//! - INSTEAD OF rules for views
//! - SELECT/INSERT/UPDATE/DELETE rules
//! - Rule-based query rewriting
//! - Conditional rules with WHERE clauses

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// RULE TYPES
// ============================================================================

/// Rule event type (what triggers the rule)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleEvent {
    Select,
    Insert,
    Update,
    Delete,
}

impl std::fmt::Display for RuleEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            RuleEvent::Select => "SELECT",
            RuleEvent::Insert => "INSERT",
            RuleEvent::Update => "UPDATE",
            RuleEvent::Delete => "DELETE",
        };
        write!(f, "{}", name)
    }
}

impl std::str::FromStr for RuleEvent {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SELECT" => Ok(RuleEvent::Select),
            "INSERT" => Ok(RuleEvent::Insert),
            "UPDATE" => Ok(RuleEvent::Update),
            "DELETE" => Ok(RuleEvent::Delete),
            _ => Err(format!("invalid rule event: {}", s)),
        }
    }
}

/// Rule type (how the rule behaves)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleType {
    /// Execute commands ALSO (in addition to original)
    Also,
    /// Execute commands INSTEAD of original
    Instead,
}

impl Default for RuleType {
    fn default() -> Self {
        RuleType::Also
    }
}

impl std::fmt::Display for RuleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleType::Also => write!(f, "ALSO"),
            RuleType::Instead => write!(f, "INSTEAD"),
        }
    }
}

// ============================================================================
// RULE DEFINITION
// ============================================================================

/// Rewrite rule definition
#[derive(Debug, Clone)]
pub struct Rule {
    /// Rule name
    pub name: String,
    /// Table/view the rule applies to
    pub relation: String,
    /// Schema of the relation
    pub schema: String,
    /// Event that triggers the rule
    pub event: RuleEvent,
    /// Rule type (ALSO or INSTEAD)
    pub rule_type: RuleType,
    /// Condition for rule execution
    pub condition: Option<String>,
    /// Commands to execute (empty for INSTEAD NOTHING)
    pub commands: Vec<String>,
    /// Is this rule enabled
    pub enabled: bool,
    /// Is this a system rule (e.g., for views)
    pub is_system: bool,
    /// Comment
    pub comment: Option<String>,
}

impl Rule {
    /// Create a new rule
    pub fn new(name: &str, schema: &str, relation: &str, event: RuleEvent) -> Self {
        Self {
            name: name.to_string(),
            relation: relation.to_string(),
            schema: schema.to_string(),
            event,
            rule_type: RuleType::default(),
            condition: None,
            commands: Vec::new(),
            enabled: true,
            is_system: false,
            comment: None,
        }
    }

    /// Set rule type to INSTEAD
    pub fn instead(mut self) -> Self {
        self.rule_type = RuleType::Instead;
        self
    }

    /// Set rule type to ALSO
    pub fn also(mut self) -> Self {
        self.rule_type = RuleType::Also;
        self
    }

    /// Add condition (WHERE clause)
    pub fn where_clause(mut self, condition: &str) -> Self {
        self.condition = Some(condition.to_string());
        self
    }

    /// Add command to execute
    pub fn do_command(mut self, command: &str) -> Self {
        self.commands.push(command.to_string());
        self
    }

    /// Set as INSTEAD NOTHING
    pub fn do_nothing(mut self) -> Self {
        self.rule_type = RuleType::Instead;
        self.commands.clear();
        self
    }

    /// Get qualified relation name
    pub fn qualified_relation(&self) -> String {
        format!("{}.{}", self.schema, self.relation)
    }

    /// Check if rule applies to given context
    pub fn applies(&self, event: RuleEvent, relation: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if self.event != event {
            return false;
        }

        let qualified = self.qualified_relation();
        relation == qualified || relation == self.relation
    }

    /// Generate CREATE RULE SQL
    pub fn to_sql(&self) -> String {
        let mut sql = format!(
            "CREATE RULE {} AS ON {} TO {}.{}",
            self.name, self.event, self.schema, self.relation
        );

        if let Some(ref condition) = self.condition {
            sql.push_str(&format!("\n  WHERE {}", condition));
        }

        sql.push_str(&format!("\n  DO {}", self.rule_type));

        if self.commands.is_empty() {
            sql.push_str(" NOTHING");
        } else if self.commands.len() == 1 {
            sql.push_str(&format!(" {}", self.commands[0]));
        } else {
            sql.push_str(" (");
            for (i, cmd) in self.commands.iter().enumerate() {
                if i > 0 {
                    sql.push_str("; ");
                }
                sql.push_str(cmd);
            }
            sql.push(')');
        }

        sql
    }
}

// ============================================================================
// RULE MANAGER
// ============================================================================

/// Rules manager
pub struct RulesManager {
    /// Rules by (schema.relation, event) -> Vec<Rule>
    rules: RwLock<HashMap<(String, RuleEvent), Vec<Rule>>>,
    /// Rules by name
    rules_by_name: RwLock<HashMap<String, Rule>>,
}

impl RulesManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            rules_by_name: RwLock::new(HashMap::new()),
        }
    }

    /// Create a rule
    pub fn create_rule(&self, rule: Rule) -> Result<(), RuleError> {
        let key = (rule.qualified_relation(), rule.event);

        let mut rules = self.rules.write();
        let mut by_name = self.rules_by_name.write();

        // Check for duplicate name
        if by_name.contains_key(&rule.name) {
            return Err(RuleError::AlreadyExists(rule.name.clone()));
        }

        // Add to both maps
        by_name.insert(rule.name.clone(), rule.clone());
        rules.entry(key).or_default().push(rule);

        Ok(())
    }

    /// Drop a rule
    pub fn drop_rule(&self, name: &str) -> Result<(), RuleError> {
        let mut rules = self.rules.write();
        let mut by_name = self.rules_by_name.write();

        let rule = by_name
            .remove(name)
            .ok_or_else(|| RuleError::NotFound(name.to_string()))?;

        let key = (rule.qualified_relation(), rule.event);
        if let Some(rule_list) = rules.get_mut(&key) {
            rule_list.retain(|r| r.name != name);
        }

        Ok(())
    }

    /// Enable/disable a rule
    pub fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), RuleError> {
        let mut rules = self.rules.write();
        let mut by_name = self.rules_by_name.write();

        let rule = by_name
            .get_mut(name)
            .ok_or_else(|| RuleError::NotFound(name.to_string()))?;

        rule.enabled = enabled;

        // Update in the other map too
        let key = (rule.qualified_relation(), rule.event);
        if let Some(rule_list) = rules.get_mut(&key) {
            for r in rule_list.iter_mut() {
                if r.name == name {
                    r.enabled = enabled;
                }
            }
        }

        Ok(())
    }

    /// Get rules for a relation and event
    pub fn get_rules(&self, relation: &str, event: RuleEvent) -> Vec<Rule> {
        let rules = self.rules.read();
        let key = (relation.to_string(), event);

        rules
            .get(&key)
            .map(|v| v.iter().filter(|r| r.enabled).cloned().collect())
            .unwrap_or_default()
    }

    /// Get rule by name
    pub fn get_rule(&self, name: &str) -> Option<Rule> {
        let by_name = self.rules_by_name.read();
        by_name.get(name).cloned()
    }

    /// List all rules for a relation
    pub fn list_rules_for_relation(&self, relation: &str) -> Vec<Rule> {
        let by_name = self.rules_by_name.read();
        by_name
            .values()
            .filter(|r| r.qualified_relation() == relation || r.relation == relation)
            .cloned()
            .collect()
    }

    /// List all rules
    pub fn list_all_rules(&self) -> Vec<Rule> {
        let by_name = self.rules_by_name.read();
        by_name.values().cloned().collect()
    }

    /// Get INSTEAD rules (for view replacement)
    pub fn get_instead_rules(&self, relation: &str, event: RuleEvent) -> Vec<Rule> {
        self.get_rules(relation, event)
            .into_iter()
            .filter(|r| r.rule_type == RuleType::Instead)
            .collect()
    }

    /// Get ALSO rules
    pub fn get_also_rules(&self, relation: &str, event: RuleEvent) -> Vec<Rule> {
        self.get_rules(relation, event)
            .into_iter()
            .filter(|r| r.rule_type == RuleType::Also)
            .collect()
    }

    /// Check if relation has any rules
    pub fn has_rules(&self, relation: &str) -> bool {
        let by_name = self.rules_by_name.read();
        by_name
            .values()
            .any(|r| r.qualified_relation() == relation || r.relation == relation)
    }

    /// Check if relation has INSTEAD rule for event
    pub fn has_instead_rule(&self, relation: &str, event: RuleEvent) -> bool {
        !self.get_instead_rules(relation, event).is_empty()
    }
}

impl Default for RulesManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// QUERY REWRITER
// ============================================================================

/// Query rewrite result
#[derive(Debug, Clone)]
pub struct RewriteResult {
    /// Original query was replaced
    pub replaced: bool,
    /// Queries to execute (may be multiple for ALSO rules)
    pub queries: Vec<String>,
    /// Rules that were applied
    pub applied_rules: Vec<String>,
}

impl RewriteResult {
    /// No rewrite needed
    pub fn no_rewrite(query: &str) -> Self {
        Self {
            replaced: false,
            queries: vec![query.to_string()],
            applied_rules: Vec::new(),
        }
    }

    /// Query replaced by INSTEAD rule
    pub fn replaced(queries: Vec<String>, rules: Vec<String>) -> Self {
        Self {
            replaced: true,
            queries,
            applied_rules: rules,
        }
    }
}

/// Query rewriter
pub struct QueryRewriter {
    manager: Arc<RulesManager>,
}

impl QueryRewriter {
    /// Create a new rewriter
    pub fn new(manager: Arc<RulesManager>) -> Self {
        Self { manager }
    }

    /// Rewrite a query based on rules
    pub fn rewrite(
        &self,
        query: &str,
        relation: &str,
        event: RuleEvent,
        context: &RewriteContext,
    ) -> RewriteResult {
        // Get INSTEAD rules first
        let instead_rules = self.manager.get_instead_rules(relation, event);

        if !instead_rules.is_empty() {
            // Apply first matching INSTEAD rule
            for rule in instead_rules {
                if self.condition_matches(&rule, context) {
                    if rule.commands.is_empty() {
                        // INSTEAD NOTHING
                        return RewriteResult::replaced(Vec::new(), vec![rule.name.clone()]);
                    }

                    // Substitute variables in commands
                    let commands: Vec<String> = rule
                        .commands
                        .iter()
                        .map(|cmd| self.substitute_variables(cmd, context))
                        .collect();

                    return RewriteResult::replaced(commands, vec![rule.name.clone()]);
                }
            }
        }

        // Get ALSO rules
        let also_rules = self.manager.get_also_rules(relation, event);
        let mut queries = vec![query.to_string()];
        let mut applied = Vec::new();

        for rule in also_rules {
            if self.condition_matches(&rule, context) {
                for cmd in &rule.commands {
                    queries.push(self.substitute_variables(cmd, context));
                }
                applied.push(rule.name.clone());
            }
        }

        if applied.is_empty() {
            RewriteResult::no_rewrite(query)
        } else {
            RewriteResult {
                replaced: false,
                queries,
                applied_rules: applied,
            }
        }
    }

    fn condition_matches(&self, rule: &Rule, context: &RewriteContext) -> bool {
        // In real implementation, would evaluate the WHERE condition
        // For now, assume all conditions match
        if rule.condition.is_none() {
            return true;
        }

        // Simplified condition evaluation
        true
    }

    fn substitute_variables(&self, command: &str, context: &RewriteContext) -> String {
        let mut result = command.to_string();

        // Replace NEW.* references
        for (col, val) in &context.new_values {
            let pattern = format!("NEW.{}", col);
            if let Some(v) = val {
                result = result.replace(&pattern, v);
            }
        }

        // Replace OLD.* references
        for (col, val) in &context.old_values {
            let pattern = format!("OLD.{}", col);
            if let Some(v) = val {
                result = result.replace(&pattern, v);
            }
        }

        result
    }
}

/// Context for rule evaluation
#[derive(Debug, Clone, Default)]
pub struct RewriteContext {
    /// NEW row values (for INSERT/UPDATE)
    pub new_values: HashMap<String, Option<String>>,
    /// OLD row values (for UPDATE/DELETE)
    pub old_values: HashMap<String, Option<String>>,
    /// Current user
    pub current_user: Option<String>,
    /// Current schema
    pub current_schema: Option<String>,
}

impl RewriteContext {
    /// Create empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Set NEW value
    pub fn set_new(&mut self, column: &str, value: Option<String>) {
        self.new_values.insert(column.to_string(), value);
    }

    /// Set OLD value
    pub fn set_old(&mut self, column: &str, value: Option<String>) {
        self.old_values.insert(column.to_string(), value);
    }
}

// ============================================================================
// VIEW RULES
// ============================================================================

/// Helper for creating view update rules
pub struct ViewRuleHelper;

impl ViewRuleHelper {
    /// Create INSTEAD INSERT rule for updatable view
    pub fn insert_rule(
        schema: &str,
        view: &str,
        base_table: &str,
        column_mapping: &[(String, String)],
    ) -> Rule {
        let columns: Vec<_> = column_mapping.iter().map(|(_, b)| b.clone()).collect();
        let values: Vec<_> = column_mapping
            .iter()
            .map(|(v, _)| format!("NEW.{}", v))
            .collect();

        let command = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            base_table,
            columns.join(", "),
            values.join(", ")
        );

        Rule::new(&format!("_{}_insert", view), schema, view, RuleEvent::Insert)
            .instead()
            .do_command(&command)
    }

    /// Create INSTEAD UPDATE rule for updatable view
    pub fn update_rule(
        schema: &str,
        view: &str,
        base_table: &str,
        column_mapping: &[(String, String)],
        key_column: &str,
    ) -> Rule {
        let sets: Vec<_> = column_mapping
            .iter()
            .map(|(v, b)| format!("{} = NEW.{}", b, v))
            .collect();

        let command = format!(
            "UPDATE {} SET {} WHERE {} = OLD.{}",
            base_table,
            sets.join(", "),
            key_column,
            key_column
        );

        Rule::new(&format!("_{}_update", view), schema, view, RuleEvent::Update)
            .instead()
            .do_command(&command)
    }

    /// Create INSTEAD DELETE rule for updatable view
    pub fn delete_rule(
        schema: &str,
        view: &str,
        base_table: &str,
        key_column: &str,
    ) -> Rule {
        let command = format!(
            "DELETE FROM {} WHERE {} = OLD.{}",
            base_table, key_column, key_column
        );

        Rule::new(&format!("_{}_delete", view), schema, view, RuleEvent::Delete)
            .instead()
            .do_command(&command)
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Rule error
#[derive(Debug, Clone)]
pub enum RuleError {
    /// Rule not found
    NotFound(String),
    /// Rule already exists
    AlreadyExists(String),
    /// Invalid rule definition
    InvalidDefinition(String),
    /// Circular rule dependency
    CircularDependency(String),
    /// System rule cannot be modified
    SystemRule(String),
}

impl std::fmt::Display for RuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleError::NotFound(n) => write!(f, "rule \"{}\" does not exist", n),
            RuleError::AlreadyExists(n) => write!(f, "rule \"{}\" already exists", n),
            RuleError::InvalidDefinition(msg) => write!(f, "invalid rule: {}", msg),
            RuleError::CircularDependency(msg) => write!(f, "circular rule dependency: {}", msg),
            RuleError::SystemRule(n) => write!(f, "cannot modify system rule \"{}\"", n),
        }
    }
}

impl std::error::Error for RuleError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_event_display() {
        assert_eq!(RuleEvent::Select.to_string(), "SELECT");
        assert_eq!(RuleEvent::Insert.to_string(), "INSERT");
    }

    #[test]
    fn test_rule_event_parse() {
        assert_eq!("SELECT".parse::<RuleEvent>().unwrap(), RuleEvent::Select);
        assert_eq!("insert".parse::<RuleEvent>().unwrap(), RuleEvent::Insert);
    }

    #[test]
    fn test_rule_creation() {
        let rule = Rule::new("my_rule", "public", "users", RuleEvent::Insert)
            .instead()
            .do_command("INSERT INTO audit_log VALUES (NEW.*)");

        assert_eq!(rule.name, "my_rule");
        assert_eq!(rule.rule_type, RuleType::Instead);
        assert_eq!(rule.commands.len(), 1);
    }

    #[test]
    fn test_rule_instead_nothing() {
        let rule = Rule::new("deny_delete", "public", "protected", RuleEvent::Delete)
            .do_nothing();

        assert_eq!(rule.rule_type, RuleType::Instead);
        assert!(rule.commands.is_empty());
    }

    #[test]
    fn test_rule_with_condition() {
        let rule = Rule::new("audit_important", "public", "orders", RuleEvent::Insert)
            .also()
            .where_clause("NEW.amount > 1000")
            .do_command("INSERT INTO audit VALUES (NEW.id, NEW.amount)");

        assert_eq!(rule.condition, Some("NEW.amount > 1000".to_string()));
    }

    #[test]
    fn test_rules_manager_create() {
        let manager = RulesManager::new();

        let rule = Rule::new("test", "public", "users", RuleEvent::Insert);
        manager.create_rule(rule).unwrap();

        assert!(manager.get_rule("test").is_some());
    }

    #[test]
    fn test_rules_manager_duplicate() {
        let manager = RulesManager::new();

        let rule1 = Rule::new("test", "public", "users", RuleEvent::Insert);
        let rule2 = Rule::new("test", "public", "orders", RuleEvent::Delete);

        manager.create_rule(rule1).unwrap();
        assert!(manager.create_rule(rule2).is_err());
    }

    #[test]
    fn test_rules_manager_drop() {
        let manager = RulesManager::new();

        let rule = Rule::new("test", "public", "users", RuleEvent::Insert);
        manager.create_rule(rule).unwrap();

        manager.drop_rule("test").unwrap();
        assert!(manager.get_rule("test").is_none());
    }

    #[test]
    fn test_rules_manager_get_rules() {
        let manager = RulesManager::new();

        let rule1 = Rule::new("r1", "public", "users", RuleEvent::Insert).instead();
        let rule2 = Rule::new("r2", "public", "users", RuleEvent::Insert).also();
        let rule3 = Rule::new("r3", "public", "users", RuleEvent::Delete);

        manager.create_rule(rule1).unwrap();
        manager.create_rule(rule2).unwrap();
        manager.create_rule(rule3).unwrap();

        let insert_rules = manager.get_rules("public.users", RuleEvent::Insert);
        assert_eq!(insert_rules.len(), 2);

        let instead_rules = manager.get_instead_rules("public.users", RuleEvent::Insert);
        assert_eq!(instead_rules.len(), 1);
    }

    #[test]
    fn test_query_rewriter_instead() {
        let manager = Arc::new(RulesManager::new());

        let rule = Rule::new("redirect", "public", "view1", RuleEvent::Insert)
            .instead()
            .do_command("INSERT INTO base_table VALUES (NEW.id)");

        manager.create_rule(rule).unwrap();

        let rewriter = QueryRewriter::new(manager);
        let ctx = RewriteContext::new();

        let result = rewriter.rewrite(
            "INSERT INTO view1 VALUES (1)",
            "public.view1",
            RuleEvent::Insert,
            &ctx,
        );

        assert!(result.replaced);
        assert_eq!(result.queries.len(), 1);
        assert!(result.queries[0].contains("base_table"));
    }

    #[test]
    fn test_query_rewriter_also() {
        let manager = Arc::new(RulesManager::new());

        let rule = Rule::new("audit", "public", "users", RuleEvent::Insert)
            .also()
            .do_command("INSERT INTO audit_log VALUES (NEW.id)");

        manager.create_rule(rule).unwrap();

        let rewriter = QueryRewriter::new(manager);
        let ctx = RewriteContext::new();

        let result = rewriter.rewrite(
            "INSERT INTO users VALUES (1)",
            "public.users",
            RuleEvent::Insert,
            &ctx,
        );

        assert!(!result.replaced);
        assert_eq!(result.queries.len(), 2); // Original + audit
    }

    #[test]
    fn test_rule_to_sql() {
        let rule = Rule::new("my_rule", "public", "users", RuleEvent::Insert)
            .instead()
            .where_clause("NEW.active")
            .do_command("INSERT INTO base VALUES (NEW.*)");

        let sql = rule.to_sql();
        assert!(sql.contains("CREATE RULE my_rule"));
        assert!(sql.contains("ON INSERT TO public.users"));
        assert!(sql.contains("WHERE NEW.active"));
        assert!(sql.contains("DO INSTEAD"));
    }

    #[test]
    fn test_view_rule_helper() {
        let insert_rule = ViewRuleHelper::insert_rule(
            "public",
            "user_view",
            "users",
            &[
                ("name".to_string(), "user_name".to_string()),
                ("email".to_string(), "user_email".to_string()),
            ],
        );

        assert_eq!(insert_rule.event, RuleEvent::Insert);
        assert_eq!(insert_rule.rule_type, RuleType::Instead);
        assert!(insert_rule.commands[0].contains("INSERT INTO users"));
    }

    #[test]
    fn test_rewrite_context() {
        let mut ctx = RewriteContext::new();
        ctx.set_new("id", Some("123".to_string()));
        ctx.set_old("status", Some("active".to_string()));

        assert_eq!(ctx.new_values.get("id"), Some(&Some("123".to_string())));
        assert_eq!(ctx.old_values.get("status"), Some(&Some("active".to_string())));
    }
}
