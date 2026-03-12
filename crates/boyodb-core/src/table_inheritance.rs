//! Table Inheritance - PostgreSQL-style Table Inheritance
//!
//! This module provides:
//! - Single and multiple inheritance
//! - Partition inheritance
//! - Column inheritance with overrides
//! - Constraint inheritance

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

// ============================================================================
// INHERITANCE TYPES
// ============================================================================

/// Inheritance relationship type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InheritanceType {
    /// Regular inheritance (INHERITS)
    Standard,
    /// Partition inheritance
    Partition,
    /// View inheritance (for typed views)
    TypedView,
}

/// Column inheritance behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnInheritance {
    /// Column inherited from parent
    Inherited,
    /// Column defined locally (may override)
    Local,
    /// Column inherited and locally modified
    InheritedLocal,
}

// ============================================================================
// INHERITED COLUMN
// ============================================================================

/// Column definition with inheritance info
#[derive(Debug, Clone)]
pub struct InheritedColumn {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Default value expression
    pub default_expr: Option<String>,
    /// Inheritance behavior
    pub inheritance: ColumnInheritance,
    /// Parent tables this column is inherited from
    pub inherited_from: Vec<String>,
    /// Column number in table
    pub attnum: i16,
    /// Is this column part of a primary key
    pub is_primary_key: bool,
}

impl InheritedColumn {
    /// Create a new local column
    pub fn local(name: &str, data_type: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            default_expr: None,
            inheritance: ColumnInheritance::Local,
            inherited_from: Vec::new(),
            attnum: 0,
            is_primary_key: false,
        }
    }

    /// Create an inherited column
    pub fn inherited(name: &str, data_type: &str, parent: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            default_expr: None,
            inheritance: ColumnInheritance::Inherited,
            inherited_from: vec![parent.to_string()],
            attnum: 0,
            is_primary_key: false,
        }
    }

    /// Set nullable
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set default
    pub fn with_default(mut self, expr: &str) -> Self {
        self.default_expr = Some(expr.to_string());
        self
    }

    /// Check if column is inherited
    pub fn is_inherited(&self) -> bool {
        !self.inherited_from.is_empty()
    }

    /// Add inheritance from another parent
    pub fn add_parent(&mut self, parent: &str) {
        if !self.inherited_from.contains(&parent.to_string()) {
            self.inherited_from.push(parent.to_string());
        }
        if self.inheritance == ColumnInheritance::Local {
            self.inheritance = ColumnInheritance::InheritedLocal;
        }
    }
}

// ============================================================================
// INHERITED CONSTRAINT
// ============================================================================

/// Constraint with inheritance info
#[derive(Debug, Clone)]
pub struct InheritedConstraint {
    /// Constraint name
    pub name: String,
    /// Constraint type
    pub constraint_type: ConstraintKind,
    /// Constraint expression/definition
    pub definition: String,
    /// Is inherited from parent
    pub is_inherited: bool,
    /// Parent table (if inherited)
    pub inherited_from: Option<String>,
    /// Is locally defined (may add to inherited)
    pub is_local: bool,
    /// Columns involved
    pub columns: Vec<String>,
}

/// Constraint kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintKind {
    Check,
    PrimaryKey,
    Unique,
    ForeignKey,
    Exclusion,
    NotNull,
}

impl InheritedConstraint {
    /// Create a CHECK constraint
    pub fn check(name: &str, expression: &str) -> Self {
        Self {
            name: name.to_string(),
            constraint_type: ConstraintKind::Check,
            definition: expression.to_string(),
            is_inherited: false,
            inherited_from: None,
            is_local: true,
            columns: Vec::new(),
        }
    }

    /// Create from parent
    pub fn inherited_from(mut self, parent: &str) -> Self {
        self.is_inherited = true;
        self.inherited_from = Some(parent.to_string());
        self.is_local = false;
        self
    }
}

// ============================================================================
// INHERITED TABLE
// ============================================================================

/// Table with inheritance information
#[derive(Debug, Clone)]
pub struct InheritedTable {
    /// Schema name
    pub schema: String,
    /// Table name
    pub name: String,
    /// Parent tables
    pub parents: Vec<ParentRelation>,
    /// Child tables
    pub children: Vec<String>,
    /// Columns (including inherited)
    pub columns: Vec<InheritedColumn>,
    /// Constraints (including inherited)
    pub constraints: Vec<InheritedConstraint>,
    /// Is this a partition
    pub is_partition: bool,
    /// Partition bound (if partition)
    pub partition_bound: Option<String>,
    /// Inheritance type
    pub inheritance_type: InheritanceType,
}

/// Parent table relation
#[derive(Debug, Clone)]
pub struct ParentRelation {
    /// Parent table name (schema.table)
    pub parent: String,
    /// Inheritance sequence number
    pub sequence: i32,
    /// Is no inherit (for constraints)
    pub no_inherit: bool,
}

impl InheritedTable {
    /// Create a new table
    pub fn new(schema: &str, name: &str) -> Self {
        Self {
            schema: schema.to_string(),
            name: name.to_string(),
            parents: Vec::new(),
            children: Vec::new(),
            columns: Vec::new(),
            constraints: Vec::new(),
            is_partition: false,
            partition_bound: None,
            inheritance_type: InheritanceType::Standard,
        }
    }

    /// Get qualified name
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Add a parent
    pub fn inherit_from(mut self, parent: &str) -> Self {
        let seq = self.parents.len() as i32 + 1;
        self.parents.push(ParentRelation {
            parent: parent.to_string(),
            sequence: seq,
            no_inherit: false,
        });
        self
    }

    /// Add a column
    pub fn with_column(mut self, column: InheritedColumn) -> Self {
        self.columns.push(column);
        self
    }

    /// Add a constraint
    pub fn with_constraint(mut self, constraint: InheritedConstraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Mark as partition
    pub fn as_partition(mut self, bound: &str) -> Self {
        self.is_partition = true;
        self.partition_bound = Some(bound.to_string());
        self.inheritance_type = InheritanceType::Partition;
        self
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&InheritedColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get inherited columns only
    pub fn inherited_columns(&self) -> Vec<&InheritedColumn> {
        self.columns
            .iter()
            .filter(|c| c.is_inherited())
            .collect()
    }

    /// Get local columns only
    pub fn local_columns(&self) -> Vec<&InheritedColumn> {
        self.columns
            .iter()
            .filter(|c| !c.is_inherited())
            .collect()
    }

    /// Get inherited constraints
    pub fn inherited_constraints(&self) -> Vec<&InheritedConstraint> {
        self.constraints
            .iter()
            .filter(|c| c.is_inherited)
            .collect()
    }

    /// Check if table has children
    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    /// Check if table has parents
    pub fn has_parents(&self) -> bool {
        !self.parents.is_empty()
    }
}

// ============================================================================
// INHERITANCE MANAGER
// ============================================================================

/// Manages table inheritance relationships
pub struct InheritanceManager {
    /// Tables by qualified name
    tables: RwLock<HashMap<String, InheritedTable>>,
    /// Parent to children mapping
    children_map: RwLock<HashMap<String, Vec<String>>>,
}

impl InheritanceManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            children_map: RwLock::new(HashMap::new()),
        }
    }

    /// Register a table
    pub fn register_table(&self, table: InheritedTable) -> Result<(), InheritanceError> {
        let qualified_name = table.qualified_name();

        // Update parent's children list
        for parent in &table.parents {
            let mut children_map = self.children_map.write().unwrap();
            children_map
                .entry(parent.parent.clone())
                .or_default()
                .push(qualified_name.clone());
        }

        let mut tables = self.tables.write().unwrap();
        tables.insert(qualified_name, table);

        Ok(())
    }

    /// Get a table
    pub fn get_table(&self, qualified_name: &str) -> Option<InheritedTable> {
        let tables = self.tables.read().unwrap();
        tables.get(qualified_name).cloned()
    }

    /// Get children of a table
    pub fn get_children(&self, qualified_name: &str) -> Vec<String> {
        let children_map = self.children_map.read().unwrap();
        children_map
            .get(qualified_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all descendants (recursive)
    pub fn get_all_descendants(&self, qualified_name: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut to_visit = vec![qualified_name.to_string()];
        let mut visited = HashSet::new();

        while let Some(current) = to_visit.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            let children = self.get_children(&current);
            for child in children {
                result.push(child.clone());
                to_visit.push(child);
            }
        }

        result
    }

    /// Get all ancestors (recursive)
    pub fn get_all_ancestors(&self, qualified_name: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut to_visit = vec![qualified_name.to_string()];
        let mut visited = HashSet::new();

        while let Some(current) = to_visit.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if let Some(table) = self.get_table(&current) {
                for parent in table.parents {
                    result.push(parent.parent.clone());
                    to_visit.push(parent.parent);
                }
            }
        }

        result
    }

    /// Add inheritance relationship
    pub fn add_inheritance(
        &self,
        child: &str,
        parent: &str,
    ) -> Result<(), InheritanceError> {
        // Check for cycles
        if self.would_create_cycle(child, parent) {
            return Err(InheritanceError::CycleDetected {
                child: child.to_string(),
                parent: parent.to_string(),
            });
        }

        let mut tables = self.tables.write().unwrap();
        let mut children_map = self.children_map.write().unwrap();

        // Get parent table columns to inherit
        let parent_table = tables
            .get(parent)
            .ok_or_else(|| InheritanceError::ParentNotFound(parent.to_string()))?
            .clone();

        // Update child table
        let child_table = tables
            .get_mut(child)
            .ok_or_else(|| InheritanceError::ChildNotFound(child.to_string()))?;

        // Add parent relation
        let seq = child_table.parents.len() as i32 + 1;
        child_table.parents.push(ParentRelation {
            parent: parent.to_string(),
            sequence: seq,
            no_inherit: false,
        });

        // Inherit columns
        for parent_col in &parent_table.columns {
            if let Some(existing) = child_table.columns.iter_mut().find(|c| c.name == parent_col.name) {
                // Column exists - check compatibility and merge
                if existing.data_type != parent_col.data_type {
                    return Err(InheritanceError::TypeMismatch {
                        column: parent_col.name.clone(),
                        child_type: existing.data_type.clone(),
                        parent_type: parent_col.data_type.clone(),
                    });
                }
                existing.add_parent(parent);
            } else {
                // Add inherited column
                let mut col = InheritedColumn::inherited(&parent_col.name, &parent_col.data_type, parent);
                col.nullable = parent_col.nullable;
                col.default_expr = parent_col.default_expr.clone();
                child_table.columns.push(col);
            }
        }

        // Inherit CHECK constraints
        for parent_constraint in &parent_table.constraints {
            if parent_constraint.constraint_type == ConstraintKind::Check {
                let constraint = InheritedConstraint {
                    name: parent_constraint.name.clone(),
                    constraint_type: ConstraintKind::Check,
                    definition: parent_constraint.definition.clone(),
                    is_inherited: true,
                    inherited_from: Some(parent.to_string()),
                    is_local: false,
                    columns: parent_constraint.columns.clone(),
                };
                child_table.constraints.push(constraint);
            }
        }

        // Update children map
        children_map
            .entry(parent.to_string())
            .or_default()
            .push(child.to_string());

        Ok(())
    }

    /// Remove inheritance relationship
    pub fn remove_inheritance(
        &self,
        child: &str,
        parent: &str,
    ) -> Result<(), InheritanceError> {
        let mut tables = self.tables.write().unwrap();
        let mut children_map = self.children_map.write().unwrap();

        // Update child table
        let child_table = tables
            .get_mut(child)
            .ok_or_else(|| InheritanceError::ChildNotFound(child.to_string()))?;

        // Remove parent relation
        child_table.parents.retain(|p| p.parent != parent);

        // Mark inherited columns/constraints as local if no other parent
        for col in &mut child_table.columns {
            col.inherited_from.retain(|p| p != parent);
            if col.inherited_from.is_empty() {
                col.inheritance = ColumnInheritance::Local;
            }
        }

        // Remove purely inherited constraints
        child_table
            .constraints
            .retain(|c| c.inherited_from.as_ref() != Some(&parent.to_string()) || c.is_local);

        // Update children map
        if let Some(children) = children_map.get_mut(parent) {
            children.retain(|c| c != child);
        }

        Ok(())
    }

    /// Check if adding inheritance would create a cycle
    fn would_create_cycle(&self, child: &str, parent: &str) -> bool {
        if child == parent {
            return true;
        }

        let ancestors = self.get_all_ancestors(parent);
        ancestors.contains(&child.to_string())
    }

    /// Get tables to scan for a query (including children)
    pub fn get_scan_tables(&self, qualified_name: &str, include_children: bool) -> Vec<String> {
        let mut result = vec![qualified_name.to_string()];

        if include_children {
            let descendants = self.get_all_descendants(qualified_name);
            result.extend(descendants);
        }

        result
    }

    /// Validate inheritance compatibility
    pub fn validate_inheritance(
        &self,
        child: &InheritedTable,
        parent: &InheritedTable,
    ) -> Result<(), InheritanceError> {
        // Check column compatibility
        for parent_col in &parent.columns {
            if let Some(child_col) = child.columns.iter().find(|c| c.name == parent_col.name) {
                if child_col.data_type != parent_col.data_type {
                    return Err(InheritanceError::TypeMismatch {
                        column: parent_col.name.clone(),
                        child_type: child_col.data_type.clone(),
                        parent_type: parent_col.data_type.clone(),
                    });
                }

                // Child NOT NULL must be compatible with parent
                if !parent_col.nullable && child_col.nullable {
                    return Err(InheritanceError::NullabilityMismatch {
                        column: parent_col.name.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

impl Default for InheritanceManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Inheritance error
#[derive(Debug, Clone)]
pub enum InheritanceError {
    /// Parent table not found
    ParentNotFound(String),
    /// Child table not found
    ChildNotFound(String),
    /// Would create inheritance cycle
    CycleDetected { child: String, parent: String },
    /// Column type mismatch
    TypeMismatch {
        column: String,
        child_type: String,
        parent_type: String,
    },
    /// Column nullability mismatch
    NullabilityMismatch { column: String },
    /// Constraint conflict
    ConstraintConflict { name: String, message: String },
    /// Cannot drop parent with children
    HasChildren(String),
}

impl std::fmt::Display for InheritanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InheritanceError::ParentNotFound(t) => write!(f, "parent table \"{}\" not found", t),
            InheritanceError::ChildNotFound(t) => write!(f, "child table \"{}\" not found", t),
            InheritanceError::CycleDetected { child, parent } => {
                write!(f, "inheritance cycle detected: {} -> {}", child, parent)
            }
            InheritanceError::TypeMismatch {
                column,
                child_type,
                parent_type,
            } => {
                write!(
                    f,
                    "column \"{}\" type mismatch: child has {}, parent has {}",
                    column, child_type, parent_type
                )
            }
            InheritanceError::NullabilityMismatch { column } => {
                write!(
                    f,
                    "column \"{}\" nullability mismatch: child allows NULL but parent does not",
                    column
                )
            }
            InheritanceError::ConstraintConflict { name, message } => {
                write!(f, "constraint \"{}\" conflict: {}", name, message)
            }
            InheritanceError::HasChildren(t) => {
                write!(f, "table \"{}\" has child tables", t)
            }
        }
    }
}

impl std::error::Error for InheritanceError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inherited_column() {
        let col = InheritedColumn::local("id", "integer").not_null();
        assert_eq!(col.name, "id");
        assert!(!col.nullable);
        assert!(!col.is_inherited());

        let col = InheritedColumn::inherited("name", "text", "parent_table");
        assert!(col.is_inherited());
        assert_eq!(col.inherited_from, vec!["parent_table"]);
    }

    #[test]
    fn test_inherited_table() {
        let table = InheritedTable::new("public", "child")
            .inherit_from("public.parent")
            .with_column(InheritedColumn::local("id", "integer"));

        assert_eq!(table.qualified_name(), "public.child");
        assert!(table.has_parents());
        assert!(!table.has_children());
    }

    #[test]
    fn test_inheritance_manager_register() {
        let manager = InheritanceManager::new();

        let parent = InheritedTable::new("public", "parent")
            .with_column(InheritedColumn::local("id", "integer"));
        manager.register_table(parent).unwrap();

        let child = InheritedTable::new("public", "child")
            .inherit_from("public.parent");
        manager.register_table(child).unwrap();

        let children = manager.get_children("public.parent");
        assert_eq!(children, vec!["public.child"]);
    }

    #[test]
    fn test_inheritance_manager_add() {
        let manager = InheritanceManager::new();

        let parent = InheritedTable::new("public", "parent")
            .with_column(InheritedColumn::local("id", "integer").not_null())
            .with_column(InheritedColumn::local("name", "text"));
        manager.register_table(parent).unwrap();

        let child = InheritedTable::new("public", "child")
            .with_column(InheritedColumn::local("extra", "boolean"));
        manager.register_table(child).unwrap();

        manager.add_inheritance("public.child", "public.parent").unwrap();

        let child_table = manager.get_table("public.child").unwrap();
        assert_eq!(child_table.columns.len(), 3); // id, name, extra
        assert!(child_table.get_column("id").unwrap().is_inherited());
    }

    #[test]
    fn test_inheritance_cycle_detection() {
        let manager = InheritanceManager::new();

        let a = InheritedTable::new("public", "a");
        let b = InheritedTable::new("public", "b");
        let c = InheritedTable::new("public", "c");

        manager.register_table(a).unwrap();
        manager.register_table(b).unwrap();
        manager.register_table(c).unwrap();

        manager.add_inheritance("public.b", "public.a").unwrap();
        manager.add_inheritance("public.c", "public.b").unwrap();

        // This would create: a -> b -> c -> a
        let result = manager.add_inheritance("public.a", "public.c");
        assert!(matches!(result, Err(InheritanceError::CycleDetected { .. })));
    }

    #[test]
    fn test_type_mismatch() {
        let manager = InheritanceManager::new();

        let parent = InheritedTable::new("public", "parent")
            .with_column(InheritedColumn::local("id", "integer"));
        manager.register_table(parent).unwrap();

        let child = InheritedTable::new("public", "child")
            .with_column(InheritedColumn::local("id", "text")); // Type mismatch
        manager.register_table(child).unwrap();

        let result = manager.add_inheritance("public.child", "public.parent");
        assert!(matches!(result, Err(InheritanceError::TypeMismatch { .. })));
    }

    #[test]
    fn test_get_all_descendants() {
        let manager = InheritanceManager::new();

        let a = InheritedTable::new("public", "a");
        let b = InheritedTable::new("public", "b").inherit_from("public.a");
        let c = InheritedTable::new("public", "c").inherit_from("public.b");

        manager.register_table(a).unwrap();
        manager.register_table(b).unwrap();
        manager.register_table(c).unwrap();

        let descendants = manager.get_all_descendants("public.a");
        assert!(descendants.contains(&"public.b".to_string()));
        assert!(descendants.contains(&"public.c".to_string()));
    }

    #[test]
    fn test_get_scan_tables() {
        let manager = InheritanceManager::new();

        let parent = InheritedTable::new("public", "parent");
        let child1 = InheritedTable::new("public", "child1").inherit_from("public.parent");
        let child2 = InheritedTable::new("public", "child2").inherit_from("public.parent");

        manager.register_table(parent).unwrap();
        manager.register_table(child1).unwrap();
        manager.register_table(child2).unwrap();

        let tables = manager.get_scan_tables("public.parent", true);
        assert_eq!(tables.len(), 3);

        let tables = manager.get_scan_tables("public.parent", false);
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn test_remove_inheritance() {
        let manager = InheritanceManager::new();

        let parent = InheritedTable::new("public", "parent")
            .with_column(InheritedColumn::local("id", "integer"));
        manager.register_table(parent).unwrap();

        let child = InheritedTable::new("public", "child");
        manager.register_table(child).unwrap();

        manager.add_inheritance("public.child", "public.parent").unwrap();
        assert!(!manager.get_children("public.parent").is_empty());

        manager.remove_inheritance("public.child", "public.parent").unwrap();
        assert!(manager.get_children("public.parent").is_empty());
    }

    #[test]
    fn test_partition_table() {
        let table = InheritedTable::new("public", "orders_2024")
            .inherit_from("public.orders")
            .as_partition("FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");

        assert!(table.is_partition);
        assert_eq!(table.inheritance_type, InheritanceType::Partition);
        assert!(table.partition_bound.is_some());
    }

    #[test]
    fn test_inherited_constraint() {
        let constraint = InheritedConstraint::check("positive_amount", "amount > 0");
        assert_eq!(constraint.constraint_type, ConstraintKind::Check);
        assert!(constraint.is_local);
        assert!(!constraint.is_inherited);

        let inherited = constraint.inherited_from("public.parent");
        assert!(inherited.is_inherited);
        assert!(!inherited.is_local);
    }
}
