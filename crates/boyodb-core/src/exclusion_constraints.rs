//! Exclusion Constraints
//!
//! PostgreSQL-compatible exclusion constraints that prevent overlapping data.
//! Useful for scheduling, resource allocation, and temporal data.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::hash::{Hash, Hasher};

/// Exclusion constraint definition
#[derive(Debug, Clone)]
pub struct ExclusionConstraint {
    /// Constraint name
    pub name: String,
    /// Table name
    pub table: String,
    /// Constraint elements (column, operator pairs)
    pub elements: Vec<ExclusionElement>,
    /// Index method (GiST, GIN, etc.)
    pub index_method: IndexMethod,
    /// WHERE clause (partial constraint)
    pub where_clause: Option<String>,
    /// Deferrable
    pub deferrable: bool,
    /// Initially deferred
    pub initially_deferred: bool,
}

/// Exclusion element (column and operator)
#[derive(Debug, Clone)]
pub struct ExclusionElement {
    /// Column name
    pub column: String,
    /// Operator for comparison
    pub operator: ExclusionOperator,
    /// Column type
    pub column_type: ColumnType,
}

/// Exclusion operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionOperator {
    /// Equal (=)
    Equal,
    /// Overlaps (&&) for ranges
    Overlaps,
    /// Contains (@>) for ranges
    Contains,
    /// Contained by (<@) for ranges
    ContainedBy,
    /// Adjacent (-|-) for ranges
    Adjacent,
    /// Distance within (for points/geometry)
    DistanceWithin,
}

impl ExclusionOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::Overlaps => "&&",
            Self::Contains => "@>",
            Self::ContainedBy => "<@",
            Self::Adjacent => "-|-",
            Self::DistanceWithin => "<->",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "=" => Some(Self::Equal),
            "&&" => Some(Self::Overlaps),
            "@>" => Some(Self::Contains),
            "<@" => Some(Self::ContainedBy),
            "-|-" => Some(Self::Adjacent),
            "<->" => Some(Self::DistanceWithin),
            _ => None,
        }
    }
}

/// Column type for exclusion
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int4Range,
    Int8Range,
    TsRange,
    TstzRange,
    DateRange,
    Box,
    Circle,
    Point,
    Integer,
    Text,
    Custom(String),
}

/// Index method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    GiST,
    SPGiST,
    GIN,
    BTree,
}

impl IndexMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GiST => "gist",
            Self::SPGiST => "spgist",
            Self::GIN => "gin",
            Self::BTree => "btree",
        }
    }
}

/// Range value for exclusion checking
#[derive(Debug, Clone)]
pub struct RangeValue {
    pub lower: Option<i64>,
    pub upper: Option<i64>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}

impl RangeValue {
    pub fn new(lower: i64, upper: i64) -> Self {
        Self {
            lower: Some(lower),
            upper: Some(upper),
            lower_inclusive: true,
            upper_inclusive: false, // PostgreSQL default: [lower, upper)
        }
    }

    pub fn empty() -> Self {
        Self {
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        match (self.lower, self.upper) {
            (Some(l), Some(u)) => {
                if self.lower_inclusive && self.upper_inclusive {
                    l > u
                } else {
                    l >= u
                }
            }
            _ => true,
        }
    }

    /// Check if two ranges overlap
    pub fn overlaps(&self, other: &RangeValue) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }

        let (l1, u1) = match (self.lower, self.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        let (l2, u2) = match (other.lower, other.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        // Ranges overlap if they are not disjoint
        // Disjoint if: u1 <= l2 or u2 <= l1 (adjusting for inclusivity)
        let disjoint = if self.upper_inclusive && other.lower_inclusive {
            u1 < l2
        } else {
            u1 <= l2
        } || if other.upper_inclusive && self.lower_inclusive {
            u2 < l1
        } else {
            u2 <= l1
        };

        !disjoint
    }

    /// Check if this range contains another
    pub fn contains(&self, other: &RangeValue) -> bool {
        if other.is_empty() {
            return true;
        }
        if self.is_empty() {
            return false;
        }

        let (l1, u1) = match (self.lower, self.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        let (l2, u2) = match (other.lower, other.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        l1 <= l2 && u1 >= u2
    }

    /// Check if ranges are adjacent
    pub fn adjacent(&self, other: &RangeValue) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }

        let (_, u1) = match (self.lower, self.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        let (l2, _) = match (other.lower, other.upper) {
            (Some(l), Some(u)) => (l, u),
            _ => return false,
        };

        // Adjacent if one's upper equals other's lower
        u1 == l2 && !self.upper_inclusive && other.lower_inclusive
            || u1 == l2 && self.upper_inclusive && !other.lower_inclusive
    }
}

/// Constraint value (for checking)
#[derive(Debug, Clone)]
pub enum ConstraintValue {
    Range(RangeValue),
    Integer(i64),
    Text(String),
    Point(f64, f64),
    Null,
}

impl ConstraintValue {
    /// Check if values conflict using the given operator
    pub fn conflicts_with(&self, other: &ConstraintValue, op: ExclusionOperator) -> bool {
        match (self, other) {
            (ConstraintValue::Range(r1), ConstraintValue::Range(r2)) => {
                match op {
                    ExclusionOperator::Overlaps => r1.overlaps(r2),
                    ExclusionOperator::Contains => r1.contains(r2),
                    ExclusionOperator::ContainedBy => r2.contains(r1),
                    ExclusionOperator::Adjacent => r1.adjacent(r2) || r2.adjacent(r1),
                    ExclusionOperator::Equal => {
                        r1.lower == r2.lower && r1.upper == r2.upper
                    }
                    _ => false,
                }
            }
            (ConstraintValue::Integer(i1), ConstraintValue::Integer(i2)) => {
                match op {
                    ExclusionOperator::Equal => i1 == i2,
                    _ => false,
                }
            }
            (ConstraintValue::Text(t1), ConstraintValue::Text(t2)) => {
                match op {
                    ExclusionOperator::Equal => t1 == t2,
                    _ => false,
                }
            }
            (ConstraintValue::Point(x1, y1), ConstraintValue::Point(x2, y2)) => {
                match op {
                    ExclusionOperator::Equal => {
                        (x1 - x2).abs() < f64::EPSILON && (y1 - y2).abs() < f64::EPSILON
                    }
                    ExclusionOperator::DistanceWithin => {
                        // Would need a threshold parameter
                        let dist = ((x1 - x2).powi(2) + (y1 - y2).powi(2)).sqrt();
                        dist < 1.0 // Default threshold
                    }
                    _ => false,
                }
            }
            (ConstraintValue::Null, _) | (_, ConstraintValue::Null) => false,
            _ => false,
        }
    }
}

/// Row identifier for exclusion index
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page: u64,
    pub slot: u32,
}

/// Exclusion index entry
#[derive(Debug, Clone)]
struct IndexEntry {
    row_id: RowId,
    values: Vec<ConstraintValue>,
}

/// Exclusion constraint manager
pub struct ExclusionConstraintManager {
    /// Constraint definitions
    constraints: RwLock<HashMap<String, ExclusionConstraint>>,
    /// Index data per constraint
    indexes: RwLock<HashMap<String, Vec<IndexEntry>>>,
    /// Statistics
    stats: RwLock<ExclusionStats>,
}

/// Exclusion statistics
#[derive(Debug, Clone, Default)]
pub struct ExclusionStats {
    /// Total constraints
    pub total_constraints: u64,
    /// Checks performed
    pub checks_performed: u64,
    /// Violations found
    pub violations_found: u64,
    /// Index lookups
    pub index_lookups: u64,
}

impl ExclusionConstraintManager {
    pub fn new() -> Self {
        Self {
            constraints: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            stats: RwLock::new(ExclusionStats::default()),
        }
    }

    /// Add an exclusion constraint
    pub fn add_constraint(&self, constraint: ExclusionConstraint) -> Result<(), ExclusionError> {
        let name = constraint.name.clone();

        let mut constraints = self.constraints.write();
        if constraints.contains_key(&name) {
            return Err(ExclusionError::ConstraintExists(name));
        }

        constraints.insert(name.clone(), constraint);
        self.indexes.write().insert(name, Vec::new());

        let mut stats = self.stats.write();
        stats.total_constraints += 1;

        Ok(())
    }

    /// Drop an exclusion constraint
    pub fn drop_constraint(&self, name: &str) -> Result<(), ExclusionError> {
        let mut constraints = self.constraints.write();

        if constraints.remove(name).is_none() {
            return Err(ExclusionError::ConstraintNotFound(name.into()));
        }

        self.indexes.write().remove(name);

        let mut stats = self.stats.write();
        if stats.total_constraints > 0 {
            stats.total_constraints -= 1;
        }

        Ok(())
    }

    /// Get constraint
    pub fn get_constraint(&self, name: &str) -> Option<ExclusionConstraint> {
        self.constraints.read().get(name).cloned()
    }

    /// Get constraints for a table
    pub fn get_table_constraints(&self, table: &str) -> Vec<ExclusionConstraint> {
        self.constraints.read()
            .values()
            .filter(|c| c.table == table)
            .cloned()
            .collect()
    }

    /// Check if insert would violate exclusion constraint
    pub fn check_insert(
        &self,
        table: &str,
        row_id: &RowId,
        values: &HashMap<String, ConstraintValue>,
    ) -> Result<(), ExclusionViolation> {
        let constraints = self.get_table_constraints(table);

        let mut stats = self.stats.write();
        stats.checks_performed += 1;

        for constraint in constraints {
            // Extract values for this constraint
            let row_values: Vec<ConstraintValue> = constraint.elements.iter()
                .map(|e| values.get(&e.column).cloned().unwrap_or(ConstraintValue::Null))
                .collect();

            // Check against existing entries
            let indexes = self.indexes.read();
            if let Some(index_entries) = indexes.get(&constraint.name) {
                stats.index_lookups += 1;

                for entry in index_entries {
                    if self.entries_conflict(&constraint, &row_values, &entry.values) {
                        stats.violations_found += 1;
                        return Err(ExclusionViolation {
                            constraint_name: constraint.name.clone(),
                            conflicting_row: entry.row_id.clone(),
                            new_row: row_id.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Add row to exclusion indexes
    pub fn index_row(
        &self,
        table: &str,
        row_id: RowId,
        values: &HashMap<String, ConstraintValue>,
    ) {
        let constraints = self.get_table_constraints(table);
        let mut indexes = self.indexes.write();

        for constraint in constraints {
            let row_values: Vec<ConstraintValue> = constraint.elements.iter()
                .map(|e| values.get(&e.column).cloned().unwrap_or(ConstraintValue::Null))
                .collect();

            if let Some(index) = indexes.get_mut(&constraint.name) {
                index.push(IndexEntry {
                    row_id: row_id.clone(),
                    values: row_values,
                });
            }
        }
    }

    /// Remove row from exclusion indexes
    pub fn unindex_row(&self, table: &str, row_id: &RowId) {
        let constraints = self.get_table_constraints(table);
        let mut indexes = self.indexes.write();

        for constraint in constraints {
            if let Some(index) = indexes.get_mut(&constraint.name) {
                index.retain(|e| e.row_id != *row_id);
            }
        }
    }

    /// Check if two entries conflict
    fn entries_conflict(
        &self,
        constraint: &ExclusionConstraint,
        new_values: &[ConstraintValue],
        existing_values: &[ConstraintValue],
    ) -> bool {
        // All element comparisons must be true for a conflict
        constraint.elements.iter()
            .enumerate()
            .all(|(i, element)| {
                if i >= new_values.len() || i >= existing_values.len() {
                    return false;
                }
                new_values[i].conflicts_with(&existing_values[i], element.operator)
            })
    }

    /// Generate CREATE statement
    pub fn to_ddl(&self, name: &str) -> Option<String> {
        let constraint = self.get_constraint(name)?;

        let elements: Vec<String> = constraint.elements.iter()
            .map(|e| format!("{} WITH {}", e.column, e.operator.as_str()))
            .collect();

        let mut ddl = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} EXCLUDE USING {} ({})",
            constraint.table,
            constraint.name,
            constraint.index_method.as_str(),
            elements.join(", ")
        );

        if let Some(ref where_clause) = constraint.where_clause {
            ddl.push_str(&format!(" WHERE ({})", where_clause));
        }

        if constraint.deferrable {
            ddl.push_str(" DEFERRABLE");
            if constraint.initially_deferred {
                ddl.push_str(" INITIALLY DEFERRED");
            }
        }

        Some(ddl)
    }

    /// Get statistics
    pub fn stats(&self) -> ExclusionStats {
        self.stats.read().clone()
    }
}

impl Default for ExclusionConstraintManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Exclusion constraint violation
#[derive(Debug, Clone)]
pub struct ExclusionViolation {
    /// Constraint name
    pub constraint_name: String,
    /// Conflicting existing row
    pub conflicting_row: RowId,
    /// New row that caused violation
    pub new_row: RowId,
}

impl std::fmt::Display for ExclusionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Exclusion constraint '{}' violated: row {:?} conflicts with {:?}",
            self.constraint_name, self.new_row, self.conflicting_row
        )
    }
}

impl std::error::Error for ExclusionViolation {}

/// Exclusion error
#[derive(Debug, Clone)]
pub enum ExclusionError {
    /// Constraint already exists
    ConstraintExists(String),
    /// Constraint not found
    ConstraintNotFound(String),
    /// Invalid operator for type
    InvalidOperator(String),
    /// Unsupported index method
    UnsupportedIndexMethod(String),
}

impl std::fmt::Display for ExclusionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConstraintExists(n) => write!(f, "Constraint '{}' already exists", n),
            Self::ConstraintNotFound(n) => write!(f, "Constraint '{}' not found", n),
            Self::InvalidOperator(s) => write!(f, "Invalid operator: {}", s),
            Self::UnsupportedIndexMethod(m) => write!(f, "Unsupported index method: {}", m),
        }
    }
}

impl std::error::Error for ExclusionError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager_with_constraint() -> ExclusionConstraintManager {
        let manager = ExclusionConstraintManager::new();

        // Add constraint: no overlapping room bookings
        let constraint = ExclusionConstraint {
            name: "no_overlapping_bookings".into(),
            table: "room_bookings".into(),
            elements: vec![
                ExclusionElement {
                    column: "room_id".into(),
                    operator: ExclusionOperator::Equal,
                    column_type: ColumnType::Integer,
                },
                ExclusionElement {
                    column: "during".into(),
                    operator: ExclusionOperator::Overlaps,
                    column_type: ColumnType::TsRange,
                },
            ],
            index_method: IndexMethod::GiST,
            where_clause: None,
            deferrable: false,
            initially_deferred: false,
        };

        manager.add_constraint(constraint).unwrap();
        manager
    }

    #[test]
    fn test_range_overlaps() {
        let r1 = RangeValue::new(10, 20);
        let r2 = RangeValue::new(15, 25);
        let r3 = RangeValue::new(20, 30);
        let r4 = RangeValue::new(30, 40);

        assert!(r1.overlaps(&r2)); // 10-20 overlaps 15-25
        assert!(!r1.overlaps(&r3)); // 10-20 does not overlap 20-30 (half-open)
        assert!(!r1.overlaps(&r4)); // 10-20 does not overlap 30-40
    }

    #[test]
    fn test_add_constraint() {
        let manager = make_manager_with_constraint();

        let constraint = manager.get_constraint("no_overlapping_bookings");
        assert!(constraint.is_some());
        assert_eq!(constraint.unwrap().elements.len(), 2);
    }

    #[test]
    fn test_check_insert_no_conflict() {
        let manager = make_manager_with_constraint();

        // Insert first booking
        let row1 = RowId { page: 0, slot: 0 };
        let mut values1 = HashMap::new();
        values1.insert("room_id".into(), ConstraintValue::Integer(1));
        values1.insert("during".into(), ConstraintValue::Range(RangeValue::new(100, 200)));

        manager.check_insert("room_bookings", &row1, &values1).unwrap();
        manager.index_row("room_bookings", row1, &values1);

        // Insert non-overlapping booking
        let row2 = RowId { page: 0, slot: 1 };
        let mut values2 = HashMap::new();
        values2.insert("room_id".into(), ConstraintValue::Integer(1));
        values2.insert("during".into(), ConstraintValue::Range(RangeValue::new(200, 300)));

        // Should succeed (no overlap)
        manager.check_insert("room_bookings", &row2, &values2).unwrap();
    }

    #[test]
    fn test_check_insert_with_conflict() {
        let manager = make_manager_with_constraint();

        // Insert first booking
        let row1 = RowId { page: 0, slot: 0 };
        let mut values1 = HashMap::new();
        values1.insert("room_id".into(), ConstraintValue::Integer(1));
        values1.insert("during".into(), ConstraintValue::Range(RangeValue::new(100, 200)));

        manager.check_insert("room_bookings", &row1, &values1).unwrap();
        manager.index_row("room_bookings", row1, &values1);

        // Insert overlapping booking
        let row2 = RowId { page: 0, slot: 1 };
        let mut values2 = HashMap::new();
        values2.insert("room_id".into(), ConstraintValue::Integer(1));
        values2.insert("during".into(), ConstraintValue::Range(RangeValue::new(150, 250)));

        // Should fail (overlaps)
        let result = manager.check_insert("room_bookings", &row2, &values2);
        assert!(result.is_err());
    }

    #[test]
    fn test_different_room_no_conflict() {
        let manager = make_manager_with_constraint();

        // Insert first booking
        let row1 = RowId { page: 0, slot: 0 };
        let mut values1 = HashMap::new();
        values1.insert("room_id".into(), ConstraintValue::Integer(1));
        values1.insert("during".into(), ConstraintValue::Range(RangeValue::new(100, 200)));

        manager.check_insert("room_bookings", &row1, &values1).unwrap();
        manager.index_row("room_bookings", row1, &values1);

        // Insert overlapping time but different room
        let row2 = RowId { page: 0, slot: 1 };
        let mut values2 = HashMap::new();
        values2.insert("room_id".into(), ConstraintValue::Integer(2)); // Different room
        values2.insert("during".into(), ConstraintValue::Range(RangeValue::new(150, 250)));

        // Should succeed (different room)
        manager.check_insert("room_bookings", &row2, &values2).unwrap();
    }

    #[test]
    fn test_ddl_generation() {
        let manager = make_manager_with_constraint();
        let ddl = manager.to_ddl("no_overlapping_bookings").unwrap();

        assert!(ddl.contains("EXCLUDE USING gist"));
        assert!(ddl.contains("room_id WITH ="));
        assert!(ddl.contains("during WITH &&"));
    }

    #[test]
    fn test_unindex_row() {
        let manager = make_manager_with_constraint();

        let row1 = RowId { page: 0, slot: 0 };
        let mut values1 = HashMap::new();
        values1.insert("room_id".into(), ConstraintValue::Integer(1));
        values1.insert("during".into(), ConstraintValue::Range(RangeValue::new(100, 200)));

        manager.index_row("room_bookings", row1.clone(), &values1);

        // Remove row from index
        manager.unindex_row("room_bookings", &row1);

        // Now overlapping insert should succeed
        let row2 = RowId { page: 0, slot: 1 };
        let mut values2 = HashMap::new();
        values2.insert("room_id".into(), ConstraintValue::Integer(1));
        values2.insert("during".into(), ConstraintValue::Range(RangeValue::new(150, 250)));

        manager.check_insert("room_bookings", &row2, &values2).unwrap();
    }
}
