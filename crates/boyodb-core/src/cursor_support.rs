//! Cursor Support - DECLARE/FETCH/CLOSE for Large Result Sets
//!
//! This module provides server-side cursors for:
//! - Streaming large result sets without loading into memory
//! - Bidirectional scrolling (SCROLL cursors)
//! - Hold cursors that survive transaction commit
//! - WITH HOLD and WITHOUT HOLD semantics

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ============================================================================
// CURSOR TYPES
// ============================================================================

/// Cursor scrollability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorScrollability {
    /// Forward-only cursor (default, most efficient)
    NoScroll,
    /// Bidirectional cursor (supports FETCH BACKWARD, FIRST, LAST, etc.)
    Scroll,
}

impl Default for CursorScrollability {
    fn default() -> Self {
        CursorScrollability::NoScroll
    }
}

/// Cursor hold behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorHold {
    /// Close cursor on transaction commit (default)
    WithoutHold,
    /// Keep cursor open after transaction commit
    WithHold,
}

impl Default for CursorHold {
    fn default() -> Self {
        CursorHold::WithoutHold
    }
}

/// Cursor sensitivity to concurrent updates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorSensitivity {
    /// Cursor sees snapshot at declaration time
    Insensitive,
    /// Cursor may see concurrent changes (implementation dependent)
    Sensitive,
    /// Use default behavior
    Asensitive,
}

impl Default for CursorSensitivity {
    fn default() -> Self {
        CursorSensitivity::Asensitive
    }
}

/// Fetch direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDirection {
    /// Fetch next row(s)
    Next,
    /// Fetch previous row(s)
    Prior,
    /// Fetch first row
    First,
    /// Fetch last row
    Last,
    /// Fetch row at absolute position
    Absolute(i64),
    /// Fetch row at relative position from current
    Relative(i64),
    /// Fetch forward N rows
    Forward(u64),
    /// Fetch backward N rows
    Backward(u64),
    /// Fetch all remaining rows forward
    ForwardAll,
    /// Fetch all remaining rows backward
    BackwardAll,
}

impl FetchDirection {
    /// Check if this direction requires a scrollable cursor
    pub fn requires_scroll(&self) -> bool {
        match self {
            FetchDirection::Prior
            | FetchDirection::First
            | FetchDirection::Last
            | FetchDirection::Absolute(_)
            | FetchDirection::Backward(_)
            | FetchDirection::BackwardAll => true,
            FetchDirection::Relative(n) if *n < 0 => true,
            _ => false,
        }
    }
}

// ============================================================================
// CURSOR DEFINITION
// ============================================================================

/// Cursor declaration options
#[derive(Debug, Clone)]
pub struct CursorOptions {
    /// Cursor name
    pub name: String,
    /// Query to execute
    pub query: String,
    /// Scrollability
    pub scrollability: CursorScrollability,
    /// Hold behavior
    pub hold: CursorHold,
    /// Sensitivity
    pub sensitivity: CursorSensitivity,
    /// Binary format for results
    pub binary: bool,
}

impl CursorOptions {
    /// Create a new cursor with default options
    pub fn new(name: &str, query: &str) -> Self {
        Self {
            name: name.to_string(),
            query: query.to_string(),
            scrollability: CursorScrollability::default(),
            hold: CursorHold::default(),
            sensitivity: CursorSensitivity::default(),
            binary: false,
        }
    }

    /// Make cursor scrollable
    pub fn scroll(mut self) -> Self {
        self.scrollability = CursorScrollability::Scroll;
        self
    }

    /// Make cursor holdable
    pub fn with_hold(mut self) -> Self {
        self.hold = CursorHold::WithHold;
        self
    }

    /// Set binary mode
    pub fn binary(mut self) -> Self {
        self.binary = true;
        self
    }
}

/// Row data type (simplified)
#[derive(Debug, Clone)]
pub struct CursorRow {
    /// Column values
    pub values: Vec<Option<String>>,
}

impl CursorRow {
    /// Create a new row
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    /// Get column count
    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    /// Get value at index
    pub fn get(&self, index: usize) -> Option<&Option<String>> {
        self.values.get(index)
    }
}

/// Column description for cursor results
#[derive(Debug, Clone)]
pub struct CursorColumn {
    /// Column name
    pub name: String,
    /// Type OID
    pub type_oid: i32,
    /// Type modifier
    pub type_mod: i32,
}

// ============================================================================
// CURSOR STATE
// ============================================================================

/// Cursor state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// Cursor is open and ready for fetching
    Open,
    /// Cursor has been exhausted (reached end)
    Exhausted,
    /// Cursor is closed
    Closed,
}

/// Active cursor instance
pub struct Cursor {
    /// Cursor ID
    pub id: u64,
    /// Options
    pub options: CursorOptions,
    /// Current state
    state: CursorState,
    /// Current position (0 = before first row)
    position: i64,
    /// Total row count (if known)
    row_count: Option<u64>,
    /// Cached rows for scroll cursors
    cached_rows: Vec<CursorRow>,
    /// Column descriptions
    columns: Vec<CursorColumn>,
    /// Transaction ID that owns this cursor
    transaction_id: Option<u64>,
    /// Creation time
    created_at: Instant,
    /// Last access time
    last_accessed: Instant,
    /// Rows fetched count
    rows_fetched: u64,
}

impl Cursor {
    /// Create a new cursor
    pub fn new(id: u64, options: CursorOptions, transaction_id: Option<u64>) -> Self {
        let now = Instant::now();
        Self {
            id,
            options,
            state: CursorState::Open,
            position: 0,
            row_count: None,
            cached_rows: Vec::new(),
            columns: Vec::new(),
            transaction_id,
            created_at: now,
            last_accessed: now,
            rows_fetched: 0,
        }
    }

    /// Get cursor name
    pub fn name(&self) -> &str {
        &self.options.name
    }

    /// Get current state
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Get current position
    pub fn position(&self) -> i64 {
        self.position
    }

    /// Check if cursor is scrollable
    pub fn is_scrollable(&self) -> bool {
        self.options.scrollability == CursorScrollability::Scroll
    }

    /// Check if cursor survives transaction commit
    pub fn is_holdable(&self) -> bool {
        self.options.hold == CursorHold::WithHold
    }

    /// Set column descriptions
    pub fn set_columns(&mut self, columns: Vec<CursorColumn>) {
        self.columns = columns;
    }

    /// Get column descriptions
    pub fn columns(&self) -> &[CursorColumn] {
        &self.columns
    }

    /// Load rows into cache (for scroll cursors)
    pub fn load_rows(&mut self, rows: Vec<CursorRow>) {
        self.row_count = Some(rows.len() as u64);
        self.cached_rows = rows;
    }

    /// Fetch rows in given direction
    pub fn fetch(&mut self, direction: FetchDirection, count: Option<u64>) -> Result<Vec<CursorRow>, CursorError> {
        if self.state == CursorState::Closed {
            return Err(CursorError::CursorClosed(self.options.name.clone()));
        }

        // Check if direction requires scroll capability
        if direction.requires_scroll() && !self.is_scrollable() {
            return Err(CursorError::NotScrollable(self.options.name.clone()));
        }

        self.last_accessed = Instant::now();

        let rows = match direction {
            FetchDirection::Next => self.fetch_next(count.unwrap_or(1))?,
            FetchDirection::Prior => self.fetch_prior(count.unwrap_or(1))?,
            FetchDirection::First => self.fetch_first()?,
            FetchDirection::Last => self.fetch_last()?,
            FetchDirection::Absolute(pos) => self.fetch_absolute(pos)?,
            FetchDirection::Relative(offset) => self.fetch_relative(offset)?,
            FetchDirection::Forward(n) => self.fetch_next(n)?,
            FetchDirection::Backward(n) => self.fetch_prior(n)?,
            FetchDirection::ForwardAll => self.fetch_forward_all()?,
            FetchDirection::BackwardAll => self.fetch_backward_all()?,
        };

        self.rows_fetched += rows.len() as u64;
        Ok(rows)
    }

    fn fetch_next(&mut self, count: u64) -> Result<Vec<CursorRow>, CursorError> {
        let total = self.row_count.unwrap_or(0) as i64;
        let mut rows = Vec::new();

        for _ in 0..count {
            self.position += 1;
            if self.position > total {
                self.state = CursorState::Exhausted;
                break;
            }
            if let Some(row) = self.cached_rows.get((self.position - 1) as usize) {
                rows.push(row.clone());
            }
        }

        Ok(rows)
    }

    fn fetch_prior(&mut self, count: u64) -> Result<Vec<CursorRow>, CursorError> {
        let mut rows = Vec::new();

        for _ in 0..count {
            self.position -= 1;
            if self.position < 1 {
                self.position = 0;
                break;
            }
            if let Some(row) = self.cached_rows.get((self.position - 1) as usize) {
                rows.push(row.clone());
            }
        }

        // Reverse to maintain fetch order
        rows.reverse();
        Ok(rows)
    }

    fn fetch_first(&mut self) -> Result<Vec<CursorRow>, CursorError> {
        self.position = 1;
        if let Some(row) = self.cached_rows.first() {
            Ok(vec![row.clone()])
        } else {
            Ok(vec![])
        }
    }

    fn fetch_last(&mut self) -> Result<Vec<CursorRow>, CursorError> {
        let total = self.row_count.unwrap_or(0) as i64;
        self.position = total;
        if let Some(row) = self.cached_rows.last() {
            Ok(vec![row.clone()])
        } else {
            Ok(vec![])
        }
    }

    fn fetch_absolute(&mut self, pos: i64) -> Result<Vec<CursorRow>, CursorError> {
        let total = self.row_count.unwrap_or(0) as i64;

        let actual_pos = if pos >= 0 {
            pos
        } else {
            // Negative position counts from end
            total + pos + 1
        };

        if actual_pos < 1 || actual_pos > total {
            self.position = if actual_pos < 1 { 0 } else { total + 1 };
            return Ok(vec![]);
        }

        self.position = actual_pos;
        if let Some(row) = self.cached_rows.get((actual_pos - 1) as usize) {
            Ok(vec![row.clone()])
        } else {
            Ok(vec![])
        }
    }

    fn fetch_relative(&mut self, offset: i64) -> Result<Vec<CursorRow>, CursorError> {
        let new_pos = self.position + offset;
        self.fetch_absolute(new_pos)
    }

    fn fetch_forward_all(&mut self) -> Result<Vec<CursorRow>, CursorError> {
        let total = self.row_count.unwrap_or(0);
        let remaining = total.saturating_sub(self.position as u64);
        self.fetch_next(remaining)
    }

    fn fetch_backward_all(&mut self) -> Result<Vec<CursorRow>, CursorError> {
        let remaining = self.position as u64;
        self.fetch_prior(remaining)
    }

    /// Move cursor without fetching
    pub fn move_cursor(&mut self, direction: FetchDirection, count: Option<u64>) -> Result<u64, CursorError> {
        if self.state == CursorState::Closed {
            return Err(CursorError::CursorClosed(self.options.name.clone()));
        }

        if direction.requires_scroll() && !self.is_scrollable() {
            return Err(CursorError::NotScrollable(self.options.name.clone()));
        }

        self.last_accessed = Instant::now();
        let total = self.row_count.unwrap_or(0) as i64;

        let (new_pos, moved) = match direction {
            FetchDirection::Next => {
                let n = count.unwrap_or(1) as i64;
                let new = (self.position + n).min(total + 1);
                (new, (new - self.position).unsigned_abs())
            }
            FetchDirection::Prior => {
                let n = count.unwrap_or(1) as i64;
                let new = (self.position - n).max(0);
                (new, (self.position - new).unsigned_abs())
            }
            FetchDirection::First => (1.min(total), 1),
            FetchDirection::Last => (total, 1),
            FetchDirection::Absolute(pos) => {
                let actual = if pos >= 0 { pos } else { total + pos + 1 };
                (actual.clamp(0, total + 1), 1)
            }
            FetchDirection::Relative(offset) => {
                let new = (self.position + offset).clamp(0, total + 1);
                (new, offset.unsigned_abs())
            }
            FetchDirection::Forward(n) => {
                let new = (self.position + n as i64).min(total + 1);
                (new, n)
            }
            FetchDirection::Backward(n) => {
                let new = (self.position - n as i64).max(0);
                (new, n)
            }
            FetchDirection::ForwardAll => (total + 1, (total + 1 - self.position) as u64),
            FetchDirection::BackwardAll => (0, self.position as u64),
        };

        self.position = new_pos;
        if self.position > total {
            self.state = CursorState::Exhausted;
        }

        Ok(moved)
    }

    /// Close the cursor
    pub fn close(&mut self) {
        self.state = CursorState::Closed;
        self.cached_rows.clear();
    }

    /// Get statistics
    pub fn stats(&self) -> CursorStats {
        CursorStats {
            name: self.options.name.clone(),
            state: self.state,
            position: self.position,
            row_count: self.row_count,
            rows_fetched: self.rows_fetched,
            created_at: self.created_at,
            last_accessed: self.last_accessed,
            is_scrollable: self.is_scrollable(),
            is_holdable: self.is_holdable(),
        }
    }
}

/// Cursor statistics
#[derive(Debug, Clone)]
pub struct CursorStats {
    pub name: String,
    pub state: CursorState,
    pub position: i64,
    pub row_count: Option<u64>,
    pub rows_fetched: u64,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub is_scrollable: bool,
    pub is_holdable: bool,
}

impl CursorStats {
    /// Time since creation
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Time since last access
    pub fn idle_time(&self) -> Duration {
        self.last_accessed.elapsed()
    }
}

// ============================================================================
// CURSOR MANAGER
// ============================================================================

/// Cursor manager for a session
pub struct CursorManager {
    /// Active cursors by name
    cursors: RwLock<HashMap<String, Cursor>>,
    /// Cursor ID counter
    next_id: AtomicU64,
    /// Maximum cursors per session
    max_cursors: usize,
    /// Cursor timeout
    timeout: Duration,
}

impl CursorManager {
    /// Create a new cursor manager
    pub fn new(max_cursors: usize, timeout: Duration) -> Self {
        Self {
            cursors: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            max_cursors,
            timeout,
        }
    }

    /// Declare a new cursor
    pub fn declare(
        &self,
        options: CursorOptions,
        transaction_id: Option<u64>,
    ) -> Result<u64, CursorError> {
        let mut cursors = self.cursors.write().unwrap();

        // Check if cursor name already exists
        if cursors.contains_key(&options.name) {
            return Err(CursorError::AlreadyExists(options.name.clone()));
        }

        // Check limit
        if cursors.len() >= self.max_cursors {
            return Err(CursorError::TooManyCursors(self.max_cursors));
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let name = options.name.clone();
        let cursor = Cursor::new(id, options, transaction_id);

        cursors.insert(name, cursor);
        Ok(id)
    }

    /// Get a cursor by name
    pub fn get(&self, name: &str) -> Option<CursorStats> {
        let cursors = self.cursors.read().unwrap();
        cursors.get(name).map(|c| c.stats())
    }

    /// Fetch from a cursor
    pub fn fetch(
        &self,
        name: &str,
        direction: FetchDirection,
        count: Option<u64>,
    ) -> Result<Vec<CursorRow>, CursorError> {
        let mut cursors = self.cursors.write().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| CursorError::NotFound(name.to_string()))?;

        cursor.fetch(direction, count)
    }

    /// Move a cursor
    pub fn move_cursor(
        &self,
        name: &str,
        direction: FetchDirection,
        count: Option<u64>,
    ) -> Result<u64, CursorError> {
        let mut cursors = self.cursors.write().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| CursorError::NotFound(name.to_string()))?;

        cursor.move_cursor(direction, count)
    }

    /// Close a cursor
    pub fn close(&self, name: &str) -> Result<(), CursorError> {
        let mut cursors = self.cursors.write().unwrap();
        if let Some(mut cursor) = cursors.remove(name) {
            cursor.close();
            Ok(())
        } else {
            Err(CursorError::NotFound(name.to_string()))
        }
    }

    /// Close all cursors
    pub fn close_all(&self) {
        let mut cursors = self.cursors.write().unwrap();
        for (_, mut cursor) in cursors.drain() {
            cursor.close();
        }
    }

    /// Close cursors for a transaction
    pub fn close_for_transaction(&self, transaction_id: u64, committed: bool) {
        let mut cursors = self.cursors.write().unwrap();
        let to_close: Vec<String> = cursors
            .iter()
            .filter(|(_, c)| {
                c.transaction_id == Some(transaction_id)
                    && (c.options.hold == CursorHold::WithoutHold || !committed)
            })
            .map(|(name, _)| name.clone())
            .collect();

        for name in to_close {
            if let Some(mut cursor) = cursors.remove(&name) {
                cursor.close();
            }
        }
    }

    /// Clean up timed-out cursors
    pub fn cleanup_expired(&self) -> usize {
        let mut cursors = self.cursors.write().unwrap();
        let expired: Vec<String> = cursors
            .iter()
            .filter(|(_, c)| c.last_accessed.elapsed() > self.timeout)
            .map(|(name, _)| name.clone())
            .collect();

        let count = expired.len();
        for name in expired {
            if let Some(mut cursor) = cursors.remove(&name) {
                cursor.close();
            }
        }
        count
    }

    /// List all cursors
    pub fn list(&self) -> Vec<CursorStats> {
        let cursors = self.cursors.read().unwrap();
        cursors.values().map(|c| c.stats()).collect()
    }

    /// Get cursor count
    pub fn count(&self) -> usize {
        let cursors = self.cursors.read().unwrap();
        cursors.len()
    }

    /// Load rows into a cursor
    pub fn load_rows(&self, name: &str, rows: Vec<CursorRow>) -> Result<(), CursorError> {
        let mut cursors = self.cursors.write().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| CursorError::NotFound(name.to_string()))?;

        cursor.load_rows(rows);
        Ok(())
    }

    /// Set column descriptions for a cursor
    pub fn set_columns(&self, name: &str, columns: Vec<CursorColumn>) -> Result<(), CursorError> {
        let mut cursors = self.cursors.write().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| CursorError::NotFound(name.to_string()))?;

        cursor.set_columns(columns);
        Ok(())
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new(100, Duration::from_secs(3600))
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Cursor error
#[derive(Debug, Clone, PartialEq)]
pub enum CursorError {
    /// Cursor not found
    NotFound(String),
    /// Cursor already exists
    AlreadyExists(String),
    /// Cursor is closed
    CursorClosed(String),
    /// Cursor is not scrollable
    NotScrollable(String),
    /// Too many cursors
    TooManyCursors(usize),
    /// Invalid fetch direction
    InvalidDirection(String),
    /// Query error
    QueryError(String),
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CursorError::NotFound(name) => write!(f, "cursor \"{}\" does not exist", name),
            CursorError::AlreadyExists(name) => {
                write!(f, "cursor \"{}\" already exists", name)
            }
            CursorError::CursorClosed(name) => write!(f, "cursor \"{}\" is closed", name),
            CursorError::NotScrollable(name) => {
                write!(f, "cursor \"{}\" is not scrollable", name)
            }
            CursorError::TooManyCursors(max) => {
                write!(f, "too many cursors (max {})", max)
            }
            CursorError::InvalidDirection(msg) => write!(f, "invalid fetch direction: {}", msg),
            CursorError::QueryError(msg) => write!(f, "query error: {}", msg),
        }
    }
}

impl std::error::Error for CursorError {}

// ============================================================================
// SQL PARSING HELPERS
// ============================================================================

/// Parse DECLARE cursor statement
pub fn parse_declare(sql: &str) -> Option<CursorOptions> {
    let sql_upper = sql.trim().to_uppercase();
    if !sql_upper.starts_with("DECLARE") {
        return None;
    }

    // Simple parser: DECLARE name [BINARY] [INSENSITIVE] [NO] SCROLL [CURSOR] [WITH[OUT] HOLD] FOR query
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 4 {
        return None;
    }

    let name = parts.get(1)?.to_string();
    let mut scrollability = CursorScrollability::NoScroll;
    let mut hold = CursorHold::WithoutHold;
    let mut binary = false;
    let mut query_start = 0;

    for (i, part) in parts.iter().enumerate() {
        let p = part.to_uppercase();
        match p.as_str() {
            "BINARY" => binary = true,
            "SCROLL" => scrollability = CursorScrollability::Scroll,
            "NO" if parts.get(i + 1).map(|s| s.to_uppercase()) == Some("SCROLL".to_string()) => {
                scrollability = CursorScrollability::NoScroll;
            }
            "WITH" if parts.get(i + 1).map(|s| s.to_uppercase()) == Some("HOLD".to_string()) => {
                hold = CursorHold::WithHold;
            }
            "WITHOUT" if parts.get(i + 1).map(|s| s.to_uppercase()) == Some("HOLD".to_string()) => {
                hold = CursorHold::WithoutHold;
            }
            "FOR" => {
                query_start = sql.to_uppercase().find("FOR").unwrap() + 3;
                break;
            }
            _ => {}
        }
    }

    if query_start == 0 {
        return None;
    }

    let query = sql[query_start..].trim().to_string();

    Some(CursorOptions {
        name,
        query,
        scrollability,
        hold,
        sensitivity: CursorSensitivity::Asensitive,
        binary,
    })
}

/// Parse FETCH statement
pub fn parse_fetch(sql: &str) -> Option<(FetchDirection, Option<u64>, String)> {
    let sql_upper = sql.trim().to_uppercase();
    if !sql_upper.starts_with("FETCH") {
        return None;
    }

    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    let mut idx = 1;
    let mut direction = FetchDirection::Next;
    let mut count = None;

    // Parse direction and count
    while idx < parts.len() {
        let p = parts[idx].to_uppercase();
        match p.as_str() {
            "NEXT" => direction = FetchDirection::Next,
            "PRIOR" | "PREVIOUS" => direction = FetchDirection::Prior,
            "FIRST" => direction = FetchDirection::First,
            "LAST" => direction = FetchDirection::Last,
            "ABSOLUTE" => {
                idx += 1;
                if let Some(n) = parts.get(idx).and_then(|s| s.parse::<i64>().ok()) {
                    direction = FetchDirection::Absolute(n);
                }
            }
            "RELATIVE" => {
                idx += 1;
                if let Some(n) = parts.get(idx).and_then(|s| s.parse::<i64>().ok()) {
                    direction = FetchDirection::Relative(n);
                }
            }
            "FORWARD" => {
                idx += 1;
                if let Some(next) = parts.get(idx) {
                    if next.to_uppercase() == "ALL" {
                        direction = FetchDirection::ForwardAll;
                    } else if let Ok(n) = next.parse::<u64>() {
                        direction = FetchDirection::Forward(n);
                    }
                }
            }
            "BACKWARD" => {
                idx += 1;
                if let Some(next) = parts.get(idx) {
                    if next.to_uppercase() == "ALL" {
                        direction = FetchDirection::BackwardAll;
                    } else if let Ok(n) = next.parse::<u64>() {
                        direction = FetchDirection::Backward(n);
                    }
                }
            }
            "ALL" => direction = FetchDirection::ForwardAll,
            "FROM" | "IN" => {
                idx += 1;
                break;
            }
            _ => {
                // Could be a count or cursor name
                if let Ok(n) = p.parse::<u64>() {
                    count = Some(n);
                } else {
                    // Assume cursor name
                    break;
                }
            }
        }
        idx += 1;
    }

    // Get cursor name
    let cursor_name = parts.get(idx)?.to_string();

    Some((direction, count, cursor_name))
}

/// Parse CLOSE statement
pub fn parse_close(sql: &str) -> Option<String> {
    let sql_upper = sql.trim().to_uppercase();
    if !sql_upper.starts_with("CLOSE") {
        return None;
    }

    let parts: Vec<&str> = sql.split_whitespace().collect();
    parts.get(1).map(|s| s.to_string())
}

/// Parse MOVE statement
pub fn parse_move(sql: &str) -> Option<(FetchDirection, Option<u64>, String)> {
    let sql_upper = sql.trim().to_uppercase();
    if !sql_upper.starts_with("MOVE") {
        return None;
    }

    // MOVE has same syntax as FETCH
    let fetch_sql = format!("FETCH {}", &sql[4..]);
    parse_fetch(&fetch_sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_options() {
        let opts = CursorOptions::new("my_cursor", "SELECT * FROM users")
            .scroll()
            .with_hold();

        assert_eq!(opts.name, "my_cursor");
        assert_eq!(opts.scrollability, CursorScrollability::Scroll);
        assert_eq!(opts.hold, CursorHold::WithHold);
    }

    #[test]
    fn test_cursor_creation() {
        let opts = CursorOptions::new("test", "SELECT 1");
        let cursor = Cursor::new(1, opts, Some(100));

        assert_eq!(cursor.name(), "test");
        assert_eq!(cursor.state(), CursorState::Open);
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn test_cursor_fetch_next() {
        let opts = CursorOptions::new("test", "SELECT * FROM t");
        let mut cursor = Cursor::new(1, opts, None);

        let rows = vec![
            CursorRow::new(vec![Some("a".to_string())]),
            CursorRow::new(vec![Some("b".to_string())]),
            CursorRow::new(vec![Some("c".to_string())]),
        ];
        cursor.load_rows(rows);

        let fetched = cursor.fetch(FetchDirection::Next, None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(cursor.position(), 1);

        let fetched = cursor.fetch(FetchDirection::Next, Some(2)).unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(cursor.position(), 3);
    }

    #[test]
    fn test_cursor_scroll() {
        let opts = CursorOptions::new("test", "SELECT * FROM t").scroll();
        let mut cursor = Cursor::new(1, opts, None);

        let rows = vec![
            CursorRow::new(vec![Some("1".to_string())]),
            CursorRow::new(vec![Some("2".to_string())]),
            CursorRow::new(vec![Some("3".to_string())]),
        ];
        cursor.load_rows(rows);

        // Fetch last
        let fetched = cursor.fetch(FetchDirection::Last, None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(cursor.position(), 3);

        // Fetch first
        let fetched = cursor.fetch(FetchDirection::First, None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(cursor.position(), 1);

        // Fetch prior
        let fetched = cursor.fetch(FetchDirection::Prior, None).unwrap();
        assert_eq!(fetched.len(), 0);
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn test_cursor_absolute() {
        let opts = CursorOptions::new("test", "SELECT * FROM t").scroll();
        let mut cursor = Cursor::new(1, opts, None);

        let rows = vec![
            CursorRow::new(vec![Some("a".to_string())]),
            CursorRow::new(vec![Some("b".to_string())]),
            CursorRow::new(vec![Some("c".to_string())]),
        ];
        cursor.load_rows(rows);

        // Absolute 2
        let fetched = cursor.fetch(FetchDirection::Absolute(2), None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(cursor.position(), 2);

        // Absolute -1 (last row)
        let fetched = cursor.fetch(FetchDirection::Absolute(-1), None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(cursor.position(), 3);
    }

    #[test]
    fn test_cursor_not_scrollable() {
        let opts = CursorOptions::new("test", "SELECT * FROM t");
        let mut cursor = Cursor::new(1, opts, None);

        cursor.load_rows(vec![CursorRow::new(vec![Some("a".to_string())])]);

        let result = cursor.fetch(FetchDirection::Prior, None);
        assert!(matches!(result, Err(CursorError::NotScrollable(_))));
    }

    #[test]
    fn test_cursor_manager() {
        let manager = CursorManager::default();

        let opts = CursorOptions::new("c1", "SELECT 1");
        let id = manager.declare(opts, Some(1)).unwrap();
        assert!(id > 0);

        assert!(manager.get("c1").is_some());
        assert!(manager.get("c2").is_none());
    }

    #[test]
    fn test_cursor_manager_duplicate() {
        let manager = CursorManager::default();

        let opts1 = CursorOptions::new("c1", "SELECT 1");
        manager.declare(opts1, None).unwrap();

        let opts2 = CursorOptions::new("c1", "SELECT 2");
        let result = manager.declare(opts2, None);
        assert!(matches!(result, Err(CursorError::AlreadyExists(_))));
    }

    #[test]
    fn test_cursor_manager_close() {
        let manager = CursorManager::default();

        let opts = CursorOptions::new("c1", "SELECT 1");
        manager.declare(opts, None).unwrap();

        assert_eq!(manager.count(), 1);
        manager.close("c1").unwrap();
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_cursor_manager_transaction_close() {
        let manager = CursorManager::default();

        let opts1 = CursorOptions::new("c1", "SELECT 1");
        let opts2 = CursorOptions::new("c2", "SELECT 2").with_hold();

        manager.declare(opts1, Some(100)).unwrap();
        manager.declare(opts2, Some(100)).unwrap();

        assert_eq!(manager.count(), 2);

        // Commit transaction - holdable cursor should survive
        manager.close_for_transaction(100, true);
        assert_eq!(manager.count(), 1);
        assert!(manager.get("c2").is_some());
    }

    #[test]
    fn test_parse_declare() {
        let sql = "DECLARE my_cursor SCROLL CURSOR WITH HOLD FOR SELECT * FROM users";
        let opts = parse_declare(sql).unwrap();

        assert_eq!(opts.name, "my_cursor");
        assert_eq!(opts.scrollability, CursorScrollability::Scroll);
        assert_eq!(opts.hold, CursorHold::WithHold);
        assert!(opts.query.contains("SELECT"));
    }

    #[test]
    fn test_parse_fetch() {
        let (dir, count, name) = parse_fetch("FETCH NEXT FROM my_cursor").unwrap();
        assert!(matches!(dir, FetchDirection::Next));
        assert_eq!(count, None);
        assert_eq!(name, "my_cursor");

        let (dir, count, name) = parse_fetch("FETCH 10 FROM my_cursor").unwrap();
        assert!(matches!(dir, FetchDirection::Next));
        assert_eq!(count, Some(10));
        assert_eq!(name, "my_cursor");

        let (dir, _, _) = parse_fetch("FETCH ABSOLUTE 5 FROM my_cursor").unwrap();
        assert!(matches!(dir, FetchDirection::Absolute(5)));

        let (dir, _, _) = parse_fetch("FETCH FORWARD ALL FROM my_cursor").unwrap();
        assert!(matches!(dir, FetchDirection::ForwardAll));
    }

    #[test]
    fn test_parse_close() {
        let name = parse_close("CLOSE my_cursor").unwrap();
        assert_eq!(name, "my_cursor");
    }

    #[test]
    fn test_cursor_stats() {
        let opts = CursorOptions::new("test", "SELECT 1").scroll().with_hold();
        let cursor = Cursor::new(1, opts, None);

        let stats = cursor.stats();
        assert_eq!(stats.name, "test");
        assert!(stats.is_scrollable);
        assert!(stats.is_holdable);
    }

    #[test]
    fn test_fetch_direction_requires_scroll() {
        assert!(!FetchDirection::Next.requires_scroll());
        assert!(FetchDirection::Prior.requires_scroll());
        assert!(FetchDirection::First.requires_scroll());
        assert!(FetchDirection::Last.requires_scroll());
        assert!(FetchDirection::Absolute(1).requires_scroll());
        assert!(FetchDirection::Backward(1).requires_scroll());
    }
}
