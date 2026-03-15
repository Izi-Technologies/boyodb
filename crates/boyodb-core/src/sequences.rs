//! Database Sequences
//!
//! Provides CREATE SEQUENCE, NEXTVAL(), CURRVAL(), SETVAL() functionality
//! for auto-increment ID generation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::SystemTime;

/// Sequence error types
#[derive(Debug, Clone)]
pub enum SequenceError {
    /// Sequence not found
    NotFound(String),
    /// Sequence already exists
    AlreadyExists(String),
    /// Sequence exhausted (reached max/min)
    Exhausted(String),
    /// Invalid parameter
    InvalidParameter(String),
    /// CURRVAL called before NEXTVAL
    CurrentValueNotSet(String),
    /// Permission denied
    PermissionDenied(String),
    /// Sequence is owned by a column
    OwnedByColumn(String),
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(s) => write!(f, "sequence not found: {}", s),
            Self::AlreadyExists(s) => write!(f, "sequence already exists: {}", s),
            Self::Exhausted(s) => write!(f, "sequence exhausted: {}", s),
            Self::InvalidParameter(s) => write!(f, "invalid parameter: {}", s),
            Self::CurrentValueNotSet(s) => write!(f, "currval not set for: {}", s),
            Self::PermissionDenied(s) => write!(f, "permission denied: {}", s),
            Self::OwnedByColumn(s) => write!(f, "sequence owned by column: {}", s),
        }
    }
}

impl std::error::Error for SequenceError {}

/// Sequence definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDefinition {
    /// Schema name
    pub schema: String,
    /// Sequence name
    pub name: String,
    /// Owner
    pub owner: String,
    /// Data type (smallint, integer, bigint)
    pub data_type: SequenceDataType,
    /// Start value
    pub start_value: i64,
    /// Minimum value
    pub min_value: i64,
    /// Maximum value
    pub max_value: i64,
    /// Increment by
    pub increment_by: i64,
    /// Cycle when exhausted
    pub cycle: bool,
    /// Cache size (pre-allocate values)
    pub cache_size: u32,
    /// Owned by column (schema.table.column)
    pub owned_by: Option<String>,
    /// Created timestamp
    pub created_at: SystemTime,
}

impl SequenceDefinition {
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// Sequence data type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceDataType {
    SmallInt, // 2 bytes, -32768 to 32767
    Integer,  // 4 bytes, -2147483648 to 2147483647
    BigInt,   // 8 bytes, full i64 range
}

impl SequenceDataType {
    pub fn min_value(&self) -> i64 {
        match self {
            Self::SmallInt => i16::MIN as i64,
            Self::Integer => i32::MIN as i64,
            Self::BigInt => i64::MIN,
        }
    }

    pub fn max_value(&self) -> i64 {
        match self {
            Self::SmallInt => i16::MAX as i64,
            Self::Integer => i32::MAX as i64,
            Self::BigInt => i64::MAX,
        }
    }

    pub fn default_start(&self, ascending: bool) -> i64 {
        if ascending {
            1
        } else {
            -1
        }
    }
}

impl Default for SequenceDataType {
    fn default() -> Self {
        Self::BigInt
    }
}

/// Sequence state (runtime)
pub struct SequenceState {
    /// Current value (last returned)
    current_value: AtomicI64,
    /// Is current value set (NEXTVAL called)
    is_set: RwLock<bool>,
    /// Cached values for this sequence
    cache: RwLock<SequenceCache>,
    /// Last log position (for WAL)
    last_log_pos: AtomicI64,
}

/// Sequence cache
struct SequenceCache {
    /// Cached values
    values: Vec<i64>,
    /// Current position in cache
    position: usize,
}

impl SequenceState {
    fn new(start_value: i64) -> Self {
        Self {
            current_value: AtomicI64::new(start_value),
            is_set: RwLock::new(false),
            cache: RwLock::new(SequenceCache {
                values: Vec::new(),
                position: 0,
            }),
            last_log_pos: AtomicI64::new(0),
        }
    }
}

/// Session-level sequence state
#[derive(Default)]
pub struct SessionSequenceState {
    /// Current values per sequence (for CURRVAL)
    current_values: HashMap<String, i64>,
}

impl SessionSequenceState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_current(&mut self, sequence: &str, value: i64) {
        self.current_values.insert(sequence.to_string(), value);
    }

    pub fn get_current(&self, sequence: &str) -> Option<i64> {
        self.current_values.get(sequence).copied()
    }

    pub fn clear(&mut self) {
        self.current_values.clear();
    }
}

/// Sequence manager
pub struct SequenceManager {
    /// Sequence definitions
    definitions: RwLock<HashMap<String, SequenceDefinition>>,
    /// Sequence states
    states: RwLock<HashMap<String, Arc<SequenceState>>>,
    /// Configuration
    config: SequenceConfig,
}

/// Sequence configuration
#[derive(Debug, Clone)]
pub struct SequenceConfig {
    /// Default cache size
    pub default_cache_size: u32,
    /// WAL logging enabled
    pub wal_logging: bool,
}

impl Default for SequenceConfig {
    fn default() -> Self {
        Self {
            default_cache_size: 1,
            wal_logging: true,
        }
    }
}

/// Options for creating a sequence
#[derive(Debug, Clone, Default)]
pub struct CreateSequenceOptions {
    pub data_type: Option<SequenceDataType>,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: Option<bool>,
    pub cache: Option<u32>,
    pub owned_by: Option<String>,
    pub if_not_exists: bool,
}

/// Options for altering a sequence
#[derive(Debug, Clone, Default)]
pub struct AlterSequenceOptions {
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start: Option<i64>,
    pub restart: Option<Option<i64>>, // Some(None) = RESTART, Some(Some(n)) = RESTART WITH n
    pub cycle: Option<bool>,
    pub cache: Option<u32>,
    pub owned_by: Option<Option<String>>, // Some(None) = OWNED BY NONE
}

impl SequenceManager {
    pub fn new(config: SequenceConfig) -> Self {
        Self {
            definitions: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Create a new sequence
    pub fn create_sequence(
        &self,
        schema: &str,
        name: &str,
        owner: &str,
        options: CreateSequenceOptions,
    ) -> Result<(), SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        // Check if exists
        {
            let definitions = self.definitions.read();
            if definitions.contains_key(&full_name) {
                if options.if_not_exists {
                    return Ok(());
                }
                return Err(SequenceError::AlreadyExists(full_name));
            }
        }

        let data_type = options.data_type.unwrap_or_default();
        let increment = options.increment.unwrap_or(1);
        let ascending = increment > 0;

        let min_value = options.min_value.unwrap_or_else(|| {
            if ascending {
                1
            } else {
                data_type.min_value()
            }
        });

        let max_value = options.max_value.unwrap_or_else(|| {
            if ascending {
                data_type.max_value()
            } else {
                -1
            }
        });

        let start_value = options
            .start
            .unwrap_or_else(|| data_type.default_start(ascending));

        // Validate parameters
        if min_value > max_value {
            return Err(SequenceError::InvalidParameter(
                "min_value must be <= max_value".into(),
            ));
        }

        if start_value < min_value || start_value > max_value {
            return Err(SequenceError::InvalidParameter(
                "start_value must be between min_value and max_value".into(),
            ));
        }

        if increment == 0 {
            return Err(SequenceError::InvalidParameter(
                "increment must not be zero".into(),
            ));
        }

        let definition = SequenceDefinition {
            schema: schema.to_string(),
            name: name.to_string(),
            owner: owner.to_string(),
            data_type,
            start_value,
            min_value,
            max_value,
            increment_by: increment,
            cycle: options.cycle.unwrap_or(false),
            cache_size: options.cache.unwrap_or(self.config.default_cache_size),
            owned_by: options.owned_by,
            created_at: SystemTime::now(),
        };

        // Create state (start at start_value - increment so first NEXTVAL returns start_value)
        let initial_value = if ascending {
            start_value - increment
        } else {
            start_value - increment
        };
        let state = Arc::new(SequenceState::new(initial_value));

        // Insert
        let mut definitions = self.definitions.write();
        let mut states = self.states.write();

        definitions.insert(full_name.clone(), definition);
        states.insert(full_name, state);

        Ok(())
    }

    /// Drop a sequence
    pub fn drop_sequence(
        &self,
        schema: &str,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<(), SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        let mut definitions = self.definitions.write();

        if let Some(def) = definitions.get(&full_name) {
            // Check if owned by column
            if def.owned_by.is_some() && !cascade {
                return Err(SequenceError::OwnedByColumn(full_name));
            }
        } else if !if_exists {
            return Err(SequenceError::NotFound(full_name));
        } else {
            return Ok(());
        }

        definitions.remove(&full_name);

        let mut states = self.states.write();
        states.remove(&full_name);

        Ok(())
    }

    /// Alter a sequence
    pub fn alter_sequence(
        &self,
        schema: &str,
        name: &str,
        options: AlterSequenceOptions,
    ) -> Result<(), SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        let mut definitions = self.definitions.write();
        let definition = definitions
            .get_mut(&full_name)
            .ok_or_else(|| SequenceError::NotFound(full_name.clone()))?;

        // Apply changes
        if let Some(increment) = options.increment {
            if increment == 0 {
                return Err(SequenceError::InvalidParameter(
                    "increment must not be zero".into(),
                ));
            }
            definition.increment_by = increment;
        }

        if let Some(min_value) = options.min_value {
            definition.min_value = min_value;
        }

        if let Some(max_value) = options.max_value {
            definition.max_value = max_value;
        }

        if let Some(cycle) = options.cycle {
            definition.cycle = cycle;
        }

        if let Some(cache) = options.cache {
            definition.cache_size = cache;
        }

        if let Some(owned_by) = options.owned_by {
            definition.owned_by = owned_by;
        }

        // Handle RESTART
        if let Some(restart_value) = options.restart {
            let restart_to = restart_value.unwrap_or(definition.start_value);

            // Validate
            if restart_to < definition.min_value || restart_to > definition.max_value {
                return Err(SequenceError::InvalidParameter(
                    "restart value out of range".into(),
                ));
            }

            // Update state
            let states = self.states.read();
            if let Some(state) = states.get(&full_name) {
                let initial = if definition.increment_by > 0 {
                    restart_to - definition.increment_by
                } else {
                    restart_to - definition.increment_by
                };
                state.current_value.store(initial, Ordering::SeqCst);
                *state.is_set.write() = false;
                let mut cache = state.cache.write();
                cache.values.clear();
                cache.position = 0;
            }
        }

        // Validate final state
        if definition.min_value > definition.max_value {
            return Err(SequenceError::InvalidParameter(
                "min_value must be <= max_value".into(),
            ));
        }

        Ok(())
    }

    /// Get next value (NEXTVAL)
    pub fn nextval(
        &self,
        schema: &str,
        name: &str,
        session: &mut SessionSequenceState,
    ) -> Result<i64, SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        let definitions = self.definitions.read();
        let definition = definitions
            .get(&full_name)
            .ok_or_else(|| SequenceError::NotFound(full_name.clone()))?;

        let states = self.states.read();
        let state = states
            .get(&full_name)
            .ok_or_else(|| SequenceError::NotFound(full_name.clone()))?;

        // Try to get from cache first
        {
            let mut cache = state.cache.write();
            if cache.position < cache.values.len() {
                let value = cache.values[cache.position];
                cache.position += 1;
                *state.is_set.write() = true;
                state.current_value.store(value, Ordering::SeqCst);
                session.set_current(&full_name, value);
                return Ok(value);
            }
        }

        // Need to allocate new values
        let cache_size = definition.cache_size as usize;
        let mut new_values = Vec::with_capacity(cache_size);

        let current = state.current_value.load(Ordering::SeqCst);

        for i in 0..cache_size {
            let next = current + definition.increment_by * (i as i64 + 1);

            // Check bounds
            if definition.increment_by > 0 {
                if next > definition.max_value {
                    if definition.cycle {
                        let next = definition.min_value + (i as i64 * definition.increment_by);
                        if next <= definition.max_value {
                            new_values.push(next);
                        } else {
                            break;
                        }
                    } else if new_values.is_empty() {
                        return Err(SequenceError::Exhausted(full_name));
                    } else {
                        break;
                    }
                } else {
                    new_values.push(next);
                }
            } else {
                if next < definition.min_value {
                    if definition.cycle {
                        let next = definition.max_value + (i as i64 * definition.increment_by);
                        if next >= definition.min_value {
                            new_values.push(next);
                        } else {
                            break;
                        }
                    } else if new_values.is_empty() {
                        return Err(SequenceError::Exhausted(full_name));
                    } else {
                        break;
                    }
                } else {
                    new_values.push(next);
                }
            }
        }

        if new_values.is_empty() {
            return Err(SequenceError::Exhausted(full_name));
        }

        let first_value = new_values[0];
        let last_value = *new_values.last().unwrap();

        // Update state
        state.current_value.store(last_value, Ordering::SeqCst);
        *state.is_set.write() = true;

        // Update cache
        {
            let mut cache = state.cache.write();
            cache.values = new_values;
            cache.position = 1; // We're returning the first one
        }

        session.set_current(&full_name, first_value);

        Ok(first_value)
    }

    /// Get current value (CURRVAL)
    pub fn currval(
        &self,
        schema: &str,
        name: &str,
        session: &SessionSequenceState,
    ) -> Result<i64, SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        // Check sequence exists
        {
            let definitions = self.definitions.read();
            if !definitions.contains_key(&full_name) {
                return Err(SequenceError::NotFound(full_name.clone()));
            }
        }

        // Get from session
        session
            .get_current(&full_name)
            .ok_or_else(|| SequenceError::CurrentValueNotSet(full_name))
    }

    /// Set value (SETVAL)
    pub fn setval(
        &self,
        schema: &str,
        name: &str,
        value: i64,
        is_called: bool,
        session: &mut SessionSequenceState,
    ) -> Result<i64, SequenceError> {
        let full_name = format!("{}.{}", schema, name);

        let definitions = self.definitions.read();
        let definition = definitions
            .get(&full_name)
            .ok_or_else(|| SequenceError::NotFound(full_name.clone()))?;

        // Validate value
        if value < definition.min_value || value > definition.max_value {
            return Err(SequenceError::InvalidParameter(format!(
                "value {} is out of range [{}, {}]",
                value, definition.min_value, definition.max_value
            )));
        }

        let states = self.states.read();
        let state = states
            .get(&full_name)
            .ok_or_else(|| SequenceError::NotFound(full_name.clone()))?;

        // Set value
        // If is_called is true, next NEXTVAL will return value + increment
        // If is_called is false, next NEXTVAL will return value
        if is_called {
            state.current_value.store(value, Ordering::SeqCst);
            *state.is_set.write() = true;
            session.set_current(&full_name, value);
        } else {
            let prev = value - definition.increment_by;
            state.current_value.store(prev, Ordering::SeqCst);
            *state.is_set.write() = false;
        }

        // Clear cache
        {
            let mut cache = state.cache.write();
            cache.values.clear();
            cache.position = 0;
        }

        Ok(value)
    }

    /// Get last value (for internal use)
    pub fn lastval(&self, session: &SessionSequenceState) -> Result<i64, SequenceError> {
        // Return the last value from any sequence in this session
        // This is a simplified version - real implementation would track order
        session
            .current_values
            .values()
            .last()
            .copied()
            .ok_or_else(|| SequenceError::CurrentValueNotSet("no sequence used".to_string()))
    }

    /// Get sequence definition
    pub fn get_sequence(&self, schema: &str, name: &str) -> Option<SequenceDefinition> {
        let full_name = format!("{}.{}", schema, name);
        self.definitions.read().get(&full_name).cloned()
    }

    /// List all sequences
    pub fn list_sequences(&self, schema: Option<&str>) -> Vec<SequenceDefinition> {
        self.definitions
            .read()
            .values()
            .filter(|s| schema.map_or(true, |sc| s.schema == sc))
            .cloned()
            .collect()
    }

    /// Check if sequence exists
    pub fn sequence_exists(&self, schema: &str, name: &str) -> bool {
        let full_name = format!("{}.{}", schema, name);
        self.definitions.read().contains_key(&full_name)
    }

    /// Get sequences owned by a table/column
    pub fn get_sequences_owned_by(&self, table: &str) -> Vec<SequenceDefinition> {
        self.definitions
            .read()
            .values()
            .filter(|s| {
                s.owned_by
                    .as_ref()
                    .map_or(false, |o| o.starts_with(table))
            })
            .cloned()
            .collect()
    }

    /// Get current state for monitoring
    pub fn get_sequence_state(&self, schema: &str, name: &str) -> Option<SequenceStateInfo> {
        let full_name = format!("{}.{}", schema, name);

        let definitions = self.definitions.read();
        let definition = definitions.get(&full_name)?;

        let states = self.states.read();
        let state = states.get(&full_name)?;

        let current = state.current_value.load(Ordering::SeqCst);
        let is_set = *state.is_set.read();
        let cache = state.cache.read();

        Some(SequenceStateInfo {
            full_name,
            current_value: current,
            is_set,
            cached_values: cache.values.len() - cache.position,
            min_value: definition.min_value,
            max_value: definition.max_value,
            increment_by: definition.increment_by,
            cycle: definition.cycle,
        })
    }
}

impl Default for SequenceManager {
    fn default() -> Self {
        Self::new(SequenceConfig::default())
    }
}

/// Sequence state info for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceStateInfo {
    pub full_name: String,
    pub current_value: i64,
    pub is_set: bool,
    pub cached_values: usize,
    pub min_value: i64,
    pub max_value: i64,
    pub increment_by: i64,
    pub cycle: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sequence() {
        let manager = SequenceManager::default();

        manager
            .create_sequence("public", "test_seq", "admin", CreateSequenceOptions::default())
            .unwrap();

        let seq = manager.get_sequence("public", "test_seq").unwrap();
        assert_eq!(seq.name, "test_seq");
        assert_eq!(seq.start_value, 1);
        assert_eq!(seq.increment_by, 1);
    }

    #[test]
    fn test_nextval() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence("public", "id_seq", "admin", CreateSequenceOptions::default())
            .unwrap();

        let v1 = manager.nextval("public", "id_seq", &mut session).unwrap();
        assert_eq!(v1, 1);

        let v2 = manager.nextval("public", "id_seq", &mut session).unwrap();
        assert_eq!(v2, 2);

        let v3 = manager.nextval("public", "id_seq", &mut session).unwrap();
        assert_eq!(v3, 3);
    }

    #[test]
    fn test_currval() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence("public", "test_seq", "admin", CreateSequenceOptions::default())
            .unwrap();

        // CURRVAL before NEXTVAL should fail
        let result = manager.currval("public", "test_seq", &session);
        assert!(result.is_err());

        // After NEXTVAL, CURRVAL should work
        manager.nextval("public", "test_seq", &mut session).unwrap();
        let curr = manager.currval("public", "test_seq", &session).unwrap();
        assert_eq!(curr, 1);

        // CURRVAL should return same value
        let curr2 = manager.currval("public", "test_seq", &session).unwrap();
        assert_eq!(curr2, 1);
    }

    #[test]
    fn test_setval() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence("public", "test_seq", "admin", CreateSequenceOptions::default())
            .unwrap();

        // Set to 100, is_called = true
        manager
            .setval("public", "test_seq", 100, true, &mut session)
            .unwrap();

        // Next value should be 101
        let v = manager.nextval("public", "test_seq", &mut session).unwrap();
        assert_eq!(v, 101);

        // Set to 200, is_called = false
        manager
            .setval("public", "test_seq", 200, false, &mut session)
            .unwrap();

        // Next value should be 200
        let v = manager.nextval("public", "test_seq", &mut session).unwrap();
        assert_eq!(v, 200);
    }

    #[test]
    fn test_descending_sequence() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence(
                "public",
                "desc_seq",
                "admin",
                CreateSequenceOptions {
                    start: Some(-1),
                    increment: Some(-1),
                    max_value: Some(-1),
                    min_value: Some(-100),
                    ..Default::default()
                },
            )
            .unwrap();

        let v1 = manager.nextval("public", "desc_seq", &mut session).unwrap();
        assert_eq!(v1, -1);

        let v2 = manager.nextval("public", "desc_seq", &mut session).unwrap();
        assert_eq!(v2, -2);
    }

    #[test]
    fn test_cycle() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence(
                "public",
                "cycle_seq",
                "admin",
                CreateSequenceOptions {
                    start: Some(1),
                    max_value: Some(3),
                    min_value: Some(1),
                    cycle: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(manager.nextval("public", "cycle_seq", &mut session).unwrap(), 1);
        assert_eq!(manager.nextval("public", "cycle_seq", &mut session).unwrap(), 2);
        assert_eq!(manager.nextval("public", "cycle_seq", &mut session).unwrap(), 3);
        // Should cycle back
        assert_eq!(manager.nextval("public", "cycle_seq", &mut session).unwrap(), 1);
    }

    #[test]
    fn test_exhausted() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence(
                "public",
                "small_seq",
                "admin",
                CreateSequenceOptions {
                    start: Some(1),
                    max_value: Some(2),
                    min_value: Some(1),
                    cycle: Some(false),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(manager.nextval("public", "small_seq", &mut session).unwrap(), 1);
        assert_eq!(manager.nextval("public", "small_seq", &mut session).unwrap(), 2);
        // Should fail
        let result = manager.nextval("public", "small_seq", &mut session);
        assert!(matches!(result, Err(SequenceError::Exhausted(_))));
    }

    #[test]
    fn test_alter_sequence() {
        let manager = SequenceManager::default();
        let mut session = SessionSequenceState::new();

        manager
            .create_sequence("public", "alter_seq", "admin", CreateSequenceOptions::default())
            .unwrap();

        manager.nextval("public", "alter_seq", &mut session).unwrap();

        // Alter increment
        manager
            .alter_sequence(
                "public",
                "alter_seq",
                AlterSequenceOptions {
                    increment: Some(10),
                    ..Default::default()
                },
            )
            .unwrap();

        let v = manager.nextval("public", "alter_seq", &mut session).unwrap();
        assert_eq!(v, 11); // 1 + 10

        // Restart
        manager
            .alter_sequence(
                "public",
                "alter_seq",
                AlterSequenceOptions {
                    restart: Some(Some(100)),
                    ..Default::default()
                },
            )
            .unwrap();

        let v = manager.nextval("public", "alter_seq", &mut session).unwrap();
        assert_eq!(v, 100);
    }
}
