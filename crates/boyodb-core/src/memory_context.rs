//! Per-Query Memory Accounting and Context Management
//!
//! This module implements PostgreSQL-style memory contexts for managing
//! memory allocation within queries. Each query gets its own memory context
//! that tracks allocations and can be reset/freed when the query completes.

// Allow Arc wrapping non-Send/Sync types - this is intentional for memory contexts
// that manage memory blocks with raw pointers
#![allow(clippy::arc_with_non_send_sync)]

use parking_lot::RwLock;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum MemoryContextError {
    /// Memory limit exceeded
    LimitExceeded {
        requested: usize,
        limit: usize,
        used: usize,
    },
    /// Allocation failed
    AllocationFailed(usize),
    /// Context not found
    ContextNotFound(String),
    /// Context is read-only
    ContextReadOnly,
    /// Invalid operation
    InvalidOperation(String),
}

impl fmt::Display for MemoryContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryContextError::LimitExceeded {
                requested,
                limit,
                used,
            } => {
                write!(
                    f,
                    "Memory limit exceeded: requested {}, limit {}, used {}",
                    requested, limit, used
                )
            }
            MemoryContextError::AllocationFailed(size) => {
                write!(f, "Memory allocation failed: {} bytes", size)
            }
            MemoryContextError::ContextNotFound(name) => {
                write!(f, "Memory context not found: {}", name)
            }
            MemoryContextError::ContextReadOnly => {
                write!(f, "Memory context is read-only")
            }
            MemoryContextError::InvalidOperation(msg) => {
                write!(f, "Invalid operation: {}", msg)
            }
        }
    }
}

impl std::error::Error for MemoryContextError {}

// ============================================================================
// Memory Block
// ============================================================================

/// A block of allocated memory within a context
#[derive(Debug)]
struct MemoryBlock {
    /// Pointer to the allocated memory
    ptr: NonNull<u8>,
    /// Size of the allocation
    size: usize,
    /// Layout used for allocation
    layout: Layout,
    /// Allocation ID
    id: u64,
}

impl MemoryBlock {
    fn new(size: usize) -> Result<Self, MemoryContextError> {
        let layout = Layout::from_size_align(size, 8)
            .map_err(|_| MemoryContextError::AllocationFailed(size))?;

        let ptr = unsafe { System.alloc(layout) };
        let ptr = NonNull::new(ptr).ok_or(MemoryContextError::AllocationFailed(size))?;

        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        Ok(Self {
            ptr,
            size,
            layout,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        unsafe {
            System.dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

// ============================================================================
// Memory Context Stats
// ============================================================================

/// Statistics for a memory context
#[derive(Debug, Default)]
pub struct MemoryContextStats {
    /// Total bytes allocated
    pub total_allocated: AtomicU64,
    /// Current bytes in use
    pub current_used: AtomicU64,
    /// Peak usage
    pub peak_used: AtomicU64,
    /// Number of allocations
    pub allocation_count: AtomicU64,
    /// Number of deallocations
    pub deallocation_count: AtomicU64,
    /// Number of resets
    pub reset_count: AtomicU64,
}

impl MemoryContextStats {
    pub fn snapshot(&self) -> MemoryContextStatsSnapshot {
        MemoryContextStatsSnapshot {
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            current_used: self.current_used.load(Ordering::Relaxed),
            peak_used: self.peak_used.load(Ordering::Relaxed),
            allocation_count: self.allocation_count.load(Ordering::Relaxed),
            deallocation_count: self.deallocation_count.load(Ordering::Relaxed),
            reset_count: self.reset_count.load(Ordering::Relaxed),
        }
    }

    fn record_allocation(&self, size: usize) {
        self.total_allocated
            .fetch_add(size as u64, Ordering::Relaxed);
        let new_used = self.current_used.fetch_add(size as u64, Ordering::Relaxed) + size as u64;

        // Update peak if necessary
        let mut peak = self.peak_used.load(Ordering::Relaxed);
        while new_used > peak {
            match self.peak_used.compare_exchange_weak(
                peak,
                new_used,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }

        self.allocation_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_deallocation(&self, size: usize) {
        self.current_used.fetch_sub(size as u64, Ordering::Relaxed);
        self.deallocation_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_reset(&self) {
        self.current_used.store(0, Ordering::Relaxed);
        self.reset_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot of memory context statistics
#[derive(Debug, Clone)]
pub struct MemoryContextStatsSnapshot {
    pub total_allocated: u64,
    pub current_used: u64,
    pub peak_used: u64,
    pub allocation_count: u64,
    pub deallocation_count: u64,
    pub reset_count: u64,
}

impl MemoryContextStatsSnapshot {
    /// Average allocation size
    pub fn average_allocation_size(&self) -> f64 {
        if self.allocation_count == 0 {
            0.0
        } else {
            self.total_allocated as f64 / self.allocation_count as f64
        }
    }
}

// ============================================================================
// Memory Context
// ============================================================================

/// Type of memory context
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryContextType {
    /// Top-level context (global)
    TopLevel,
    /// Query-level context
    Query,
    /// Transaction-level context
    Transaction,
    /// Expression evaluation context
    Expression,
    /// Sort/hash operation context
    Operation,
    /// Cache context
    Cache,
    /// Temporary context
    Temporary,
}

/// A memory context for tracking allocations
pub struct MemoryContext {
    /// Context name
    name: String,
    /// Context type
    context_type: MemoryContextType,
    /// Parent context
    parent: Option<Weak<MemoryContext>>,
    /// Child contexts
    children: RwLock<Vec<Arc<MemoryContext>>>,
    /// Allocated blocks (Some = occupied, None = freed)
    blocks: RwLock<Vec<Option<MemoryBlock>>>,
    /// Block lookup by pointer address -> index
    block_map: RwLock<HashMap<usize, usize>>,
    /// Indices of freed slots for reuse
    free_slots: RwLock<Vec<usize>>,
    /// Memory limit (0 = unlimited)
    limit: AtomicUsize,
    /// Statistics
    stats: MemoryContextStats,
    /// Whether context is active
    active: AtomicBool,
    /// Whether context allows allocations
    allow_allocations: AtomicBool,
}

impl MemoryContext {
    /// Create a new top-level memory context
    pub fn new(name: &str, context_type: MemoryContextType) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            context_type,
            parent: None,
            children: RwLock::new(Vec::new()),
            blocks: RwLock::new(Vec::new()),
            block_map: RwLock::new(HashMap::new()),
            free_slots: RwLock::new(Vec::new()),
            limit: AtomicUsize::new(0),
            stats: MemoryContextStats::default(),
            active: AtomicBool::new(true),
            allow_allocations: AtomicBool::new(true),
        })
    }

    /// Create a child context
    pub fn create_child(
        parent: &Arc<MemoryContext>,
        name: &str,
        context_type: MemoryContextType,
    ) -> Arc<Self> {
        let child = Arc::new(Self {
            name: name.to_string(),
            context_type,
            parent: Some(Arc::downgrade(parent)),
            children: RwLock::new(Vec::new()),
            blocks: RwLock::new(Vec::new()),
            block_map: RwLock::new(HashMap::new()),
            free_slots: RwLock::new(Vec::new()),
            limit: AtomicUsize::new(0),
            stats: MemoryContextStats::default(),
            active: AtomicBool::new(true),
            allow_allocations: AtomicBool::new(true),
        });

        let mut children = parent.children.write();
        children.push(child.clone());

        child
    }

    /// Get context name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get context type
    pub fn context_type(&self) -> MemoryContextType {
        self.context_type
    }

    /// Set memory limit
    pub fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    /// Get memory limit
    pub fn get_limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> u64 {
        self.stats.current_used.load(Ordering::Relaxed)
    }

    /// Get total usage including children
    pub fn total_usage(&self) -> u64 {
        let mut total = self.current_usage();
        let children = self.children.read();
        for child in children.iter() {
            total += child.total_usage();
        }
        total
    }

    /// Atomically reserve memory, checking limits. Returns Err if limit exceeded.
    fn reserve_memory(&self, size: usize) -> Result<(), MemoryContextError> {
        let limit = self.limit.load(Ordering::Relaxed);

        if limit > 0 {
            // Use compare-and-swap loop to atomically check and reserve
            loop {
                let current = self.stats.current_used.load(Ordering::Acquire) as usize;
                if current + size > limit {
                    return Err(MemoryContextError::LimitExceeded {
                        requested: size,
                        limit,
                        used: current,
                    });
                }

                // Try to reserve the memory atomically
                match self.stats.current_used.compare_exchange_weak(
                    current as u64,
                    (current + size) as u64,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => continue, // Retry if another thread modified concurrently
                }
            }
        } else {
            // No limit, just increment
            self.stats
                .current_used
                .fetch_add(size as u64, Ordering::Relaxed);
        }

        // Note: We don't update parent's current_used here because total_usage()
        // already recursively aggregates children's usage. Updating parent would
        // cause double-counting.

        Ok(())
    }

    /// Release reserved memory
    fn release_memory(&self, size: usize) {
        self.stats
            .current_used
            .fetch_sub(size as u64, Ordering::Relaxed);
        // Note: Don't update parent - total_usage() handles aggregation
    }

    /// Allocate memory in this context
    pub fn alloc(&self, size: usize) -> Result<*mut u8, MemoryContextError> {
        if !self.allow_allocations.load(Ordering::Relaxed) {
            return Err(MemoryContextError::ContextReadOnly);
        }

        // Atomically reserve memory (checks limits)
        self.reserve_memory(size)?;

        // Try to allocate the block
        let block = match MemoryBlock::new(size) {
            Ok(b) => b,
            Err(e) => {
                // Rollback the reservation on allocation failure
                self.release_memory(size);
                return Err(e);
            }
        };

        let ptr = block.as_ptr();
        let ptr_addr = ptr as usize;

        {
            let mut blocks = self.blocks.write();
            let mut free_slots = self.free_slots.write();
            let mut block_map = self.block_map.write();

            // Try to reuse a freed slot
            let idx = if let Some(free_idx) = free_slots.pop() {
                // Reuse the freed slot
                blocks[free_idx] = Some(block);
                free_idx
            } else {
                // No free slots, append to the end
                let idx = blocks.len();
                blocks.push(Some(block));
                idx
            };

            block_map.insert(ptr_addr, idx);
        }

        // Update other stats (current_used already updated in reserve_memory)
        self.stats
            .total_allocated
            .fetch_add(size as u64, Ordering::Relaxed);
        self.stats.allocation_count.fetch_add(1, Ordering::Relaxed);

        // Update peak if necessary
        let current = self.stats.current_used.load(Ordering::Relaxed);
        let mut peak = self.stats.peak_used.load(Ordering::Relaxed);
        while current > peak {
            match self.stats.peak_used.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }

        Ok(ptr)
    }

    /// Allocate zeroed memory
    pub fn alloc_zeroed(&self, size: usize) -> Result<*mut u8, MemoryContextError> {
        let ptr = self.alloc(size)?;
        unsafe {
            std::ptr::write_bytes(ptr, 0, size);
        }
        Ok(ptr)
    }

    /// Deallocate memory (if tracked in this context)
    pub fn dealloc(&self, ptr: *mut u8) -> bool {
        let ptr_addr = ptr as usize;

        let mut block_map = self.block_map.write();
        let mut blocks = self.blocks.write();
        let mut free_slots = self.free_slots.write();

        if let Some(idx) = block_map.remove(&ptr_addr) {
            if idx < blocks.len() {
                // Get the block and its size
                if let Some(block) = blocks[idx].take() {
                    let size = block.size;

                    // Release memory tracking
                    self.release_memory(size);
                    self.stats
                        .deallocation_count
                        .fetch_add(1, Ordering::Relaxed);

                    // Block is dropped here when `take()` moves it out and it goes out of scope
                    // Mark this slot as free for reuse
                    free_slots.push(idx);

                    return true;
                }
                // Block was already freed (None) - double-free
                return false;
            }
            true
        } else {
            false
        }
    }

    /// Reset the context, freeing all allocations
    pub fn reset(&self) {
        // Reset all children first
        let children = self.children.read();
        for child in children.iter() {
            child.reset();
        }
        drop(children);

        // Clear our allocations
        let mut blocks = self.blocks.write();
        let mut block_map = self.block_map.write();
        let mut free_slots = self.free_slots.write();

        blocks.clear();
        block_map.clear();
        free_slots.clear();

        self.stats.record_reset();
    }

    /// Delete this context (and all children)
    pub fn delete(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.reset();
    }

    /// Check if context is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Freeze context (prevent further allocations)
    pub fn freeze(&self) {
        self.allow_allocations.store(false, Ordering::Relaxed);
    }

    /// Unfreeze context
    pub fn unfreeze(&self) {
        self.allow_allocations.store(true, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> MemoryContextStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get number of children
    pub fn child_count(&self) -> usize {
        self.children.read().len()
    }

    /// Get all child contexts
    pub fn children(&self) -> Vec<Arc<MemoryContext>> {
        self.children.read().clone()
    }

    /// Get number of active blocks
    pub fn block_count(&self) -> usize {
        self.blocks.read().len()
    }

    /// Print context tree
    pub fn print_tree(&self, indent: usize) -> String {
        let mut result = format!(
            "{}{} ({:?}): {} bytes, {} blocks\n",
            " ".repeat(indent),
            self.name,
            self.context_type,
            self.current_usage(),
            self.block_count()
        );

        let children = self.children.read();
        for child in children.iter() {
            result.push_str(&child.print_tree(indent + 2));
        }

        result
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("name", &self.name)
            .field("context_type", &self.context_type)
            .field("current_usage", &self.current_usage())
            .field("block_count", &self.block_count())
            .finish()
    }
}

// ============================================================================
// Query Memory Context
// ============================================================================

/// Memory context for a single query
pub struct QueryMemoryContext {
    /// Query ID
    query_id: u64,
    /// Main context
    context: Arc<MemoryContext>,
    /// Sort context
    sort_context: Option<Arc<MemoryContext>>,
    /// Hash context
    hash_context: Option<Arc<MemoryContext>>,
    /// Aggregate context
    agg_context: Option<Arc<MemoryContext>>,
    /// Work memory limit (for sorts/hashes)
    work_mem: usize,
}

impl QueryMemoryContext {
    /// Create a new query memory context
    pub fn new(query_id: u64, parent: &Arc<MemoryContext>, work_mem: usize) -> Self {
        let context = MemoryContext::create_child(
            parent,
            &format!("Query_{}", query_id),
            MemoryContextType::Query,
        );

        Self {
            query_id,
            context,
            sort_context: None,
            hash_context: None,
            agg_context: None,
            work_mem,
        }
    }

    /// Get the main context
    pub fn context(&self) -> &Arc<MemoryContext> {
        &self.context
    }

    /// Get or create sort context
    pub fn sort_context(&mut self) -> &Arc<MemoryContext> {
        if self.sort_context.is_none() {
            let ctx = MemoryContext::create_child(
                &self.context,
                &format!("Sort_{}", self.query_id),
                MemoryContextType::Operation,
            );
            ctx.set_limit(self.work_mem);
            self.sort_context = Some(ctx);
        }
        self.sort_context.as_ref().unwrap()
    }

    /// Get or create hash context
    pub fn hash_context(&mut self) -> &Arc<MemoryContext> {
        if self.hash_context.is_none() {
            let ctx = MemoryContext::create_child(
                &self.context,
                &format!("Hash_{}", self.query_id),
                MemoryContextType::Operation,
            );
            ctx.set_limit(self.work_mem);
            self.hash_context = Some(ctx);
        }
        self.hash_context.as_ref().unwrap()
    }

    /// Get or create aggregate context
    pub fn agg_context(&mut self) -> &Arc<MemoryContext> {
        if self.agg_context.is_none() {
            let ctx = MemoryContext::create_child(
                &self.context,
                &format!("Agg_{}", self.query_id),
                MemoryContextType::Operation,
            );
            self.agg_context = Some(ctx);
        }
        self.agg_context.as_ref().unwrap()
    }

    /// Get total memory usage
    pub fn total_usage(&self) -> u64 {
        self.context.total_usage()
    }

    /// Reset all contexts
    pub fn reset(&self) {
        self.context.reset();
    }

    /// Get query ID
    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    /// Check if within work_mem limit
    pub fn within_work_mem(&self) -> bool {
        let operation_usage = self
            .sort_context
            .as_ref()
            .map(|c| c.current_usage())
            .unwrap_or(0)
            + self
                .hash_context
                .as_ref()
                .map(|c| c.current_usage())
                .unwrap_or(0);
        operation_usage as usize <= self.work_mem
    }
}

// ============================================================================
// Memory Context Manager
// ============================================================================

/// Global statistics for memory context manager
#[derive(Debug, Default)]
pub struct MemoryManagerStats {
    /// Total contexts created
    pub contexts_created: AtomicU64,
    /// Total contexts destroyed
    pub contexts_destroyed: AtomicU64,
    /// Current active contexts
    pub active_contexts: AtomicU64,
    /// Total allocations
    pub total_allocations: AtomicU64,
    /// Total bytes allocated
    pub total_bytes_allocated: AtomicU64,
    /// Allocation limit violations
    pub limit_violations: AtomicU64,
}

impl MemoryManagerStats {
    pub fn snapshot(&self) -> MemoryManagerStatsSnapshot {
        MemoryManagerStatsSnapshot {
            contexts_created: self.contexts_created.load(Ordering::Relaxed),
            contexts_destroyed: self.contexts_destroyed.load(Ordering::Relaxed),
            active_contexts: self.active_contexts.load(Ordering::Relaxed),
            total_allocations: self.total_allocations.load(Ordering::Relaxed),
            total_bytes_allocated: self.total_bytes_allocated.load(Ordering::Relaxed),
            limit_violations: self.limit_violations.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of memory manager statistics
#[derive(Debug, Clone)]
pub struct MemoryManagerStatsSnapshot {
    pub contexts_created: u64,
    pub contexts_destroyed: u64,
    pub active_contexts: u64,
    pub total_allocations: u64,
    pub total_bytes_allocated: u64,
    pub limit_violations: u64,
}

/// Manages memory contexts for all queries
pub struct MemoryContextManager {
    /// Top-level context
    top_context: Arc<MemoryContext>,
    /// Query contexts
    query_contexts: RwLock<HashMap<u64, Arc<QueryMemoryContext>>>,
    /// Default work_mem
    work_mem: AtomicUsize,
    /// Global memory limit
    global_limit: AtomicUsize,
    /// Statistics
    stats: MemoryManagerStats,
    /// Next query ID
    next_query_id: AtomicU64,
}

impl MemoryContextManager {
    /// Create a new memory context manager
    pub fn new(global_limit: usize, work_mem: usize) -> Self {
        let top_context = MemoryContext::new("TopMemoryContext", MemoryContextType::TopLevel);
        top_context.set_limit(global_limit);

        Self {
            top_context,
            query_contexts: RwLock::new(HashMap::new()),
            work_mem: AtomicUsize::new(work_mem),
            global_limit: AtomicUsize::new(global_limit),
            stats: MemoryManagerStats::default(),
            next_query_id: AtomicU64::new(0),
        }
    }

    /// Create a new query context
    pub fn create_query_context(&self) -> Arc<QueryMemoryContext> {
        let query_id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let work_mem = self.work_mem.load(Ordering::Relaxed);

        let ctx = Arc::new(QueryMemoryContext::new(
            query_id,
            &self.top_context,
            work_mem,
        ));

        let mut contexts = self.query_contexts.write();
        contexts.insert(query_id, ctx.clone());

        self.stats.contexts_created.fetch_add(1, Ordering::Relaxed);
        self.stats.active_contexts.fetch_add(1, Ordering::Relaxed);

        ctx
    }

    /// Get a query context
    pub fn get_query_context(&self, query_id: u64) -> Option<Arc<QueryMemoryContext>> {
        let contexts = self.query_contexts.read();
        contexts.get(&query_id).cloned()
    }

    /// Release a query context
    pub fn release_query_context(&self, query_id: u64) {
        let mut contexts = self.query_contexts.write();
        if let Some(ctx) = contexts.remove(&query_id) {
            ctx.reset();
            self.stats
                .contexts_destroyed
                .fetch_add(1, Ordering::Relaxed);
            self.stats.active_contexts.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get top-level context
    pub fn top_context(&self) -> &Arc<MemoryContext> {
        &self.top_context
    }

    /// Get total memory usage
    pub fn total_usage(&self) -> u64 {
        self.top_context.total_usage()
    }

    /// Set global memory limit
    pub fn set_global_limit(&self, limit: usize) {
        self.global_limit.store(limit, Ordering::Relaxed);
        self.top_context.set_limit(limit);
    }

    /// Set default work_mem
    pub fn set_work_mem(&self, work_mem: usize) {
        self.work_mem.store(work_mem, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> MemoryManagerStatsSnapshot {
        self.stats.snapshot()
    }

    /// List all query contexts
    pub fn list_query_contexts(&self) -> Vec<(u64, u64)> {
        let contexts = self.query_contexts.read();
        contexts
            .iter()
            .map(|(id, ctx)| (*id, ctx.total_usage()))
            .collect()
    }

    /// Get memory report
    pub fn memory_report(&self) -> String {
        self.top_context.print_tree(0)
    }

    /// Reset all contexts
    pub fn reset_all(&self) {
        let mut contexts = self.query_contexts.write();
        let count = contexts.len();
        for (_id, ctx) in contexts.drain() {
            ctx.reset();
        }
        self.stats
            .contexts_destroyed
            .fetch_add(count as u64, Ordering::Relaxed);
        self.stats.active_contexts.store(0, Ordering::Relaxed);

        self.top_context.reset();
    }
}

impl Default for MemoryContextManager {
    fn default() -> Self {
        Self::new(1024 * 1024 * 1024, 64 * 1024 * 1024) // 1GB global, 64MB work_mem
    }
}

// ============================================================================
// Allocation Tracker (for debugging)
// ============================================================================

/// Tracks allocations for debugging purposes
#[derive(Debug)]
pub struct AllocationTracker {
    /// Allocations by size
    by_size: RwLock<HashMap<usize, u64>>,
    /// Allocations by context
    by_context: RwLock<HashMap<String, u64>>,
    /// Large allocations (> 1MB)
    large_allocations: RwLock<Vec<(String, usize, String)>>,
    /// Tracking enabled
    enabled: AtomicBool,
}

impl AllocationTracker {
    pub fn new() -> Self {
        Self {
            by_size: RwLock::new(HashMap::new()),
            by_context: RwLock::new(HashMap::new()),
            large_allocations: RwLock::new(Vec::new()),
            enabled: AtomicBool::new(false),
        }
    }

    /// Enable tracking
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Disable tracking
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// Record an allocation
    pub fn record(&self, context_name: &str, size: usize, description: &str) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        // Track by size bucket
        let bucket = if size < 64 {
            64
        } else if size < 256 {
            256
        } else if size < 1024 {
            1024
        } else if size < 4096 {
            4096
        } else if size < 65536 {
            65536
        } else {
            1024 * 1024
        };

        {
            let mut by_size = self.by_size.write();
            *by_size.entry(bucket).or_insert(0) += 1;
        }

        // Track by context
        {
            let mut by_context = self.by_context.write();
            *by_context.entry(context_name.to_string()).or_insert(0) += 1;
        }

        // Track large allocations
        if size > 1024 * 1024 {
            let mut large = self.large_allocations.write();
            large.push((context_name.to_string(), size, description.to_string()));
        }
    }

    /// Get allocation summary
    pub fn summary(&self) -> AllocationSummary {
        AllocationSummary {
            by_size: self.by_size.read().clone(),
            by_context: self.by_context.read().clone(),
            large_allocations: self.large_allocations.read().clone(),
        }
    }

    /// Reset tracking data
    pub fn reset(&self) {
        self.by_size.write().clear();
        self.by_context.write().clear();
        self.large_allocations.write().clear();
    }
}

impl Default for AllocationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of tracked allocations
#[derive(Debug, Clone)]
pub struct AllocationSummary {
    pub by_size: HashMap<usize, u64>,
    pub by_context: HashMap<String, u64>,
    pub large_allocations: Vec<(String, usize, String)>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_context_creation() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);
        assert_eq!(ctx.name(), "test");
        assert_eq!(ctx.context_type(), MemoryContextType::Query);
        assert!(ctx.is_active());
    }

    #[test]
    fn test_allocation() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        let ptr = ctx.alloc(1024).unwrap();
        assert!(!ptr.is_null());

        let stats = ctx.stats();
        assert_eq!(stats.current_used, 1024);
        assert_eq!(stats.allocation_count, 1);
    }

    #[test]
    fn test_allocation_zeroed() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        let ptr = ctx.alloc_zeroed(100).unwrap();

        // Check that memory is zeroed
        unsafe {
            for i in 0..100 {
                assert_eq!(*ptr.add(i), 0);
            }
        }
    }

    #[test]
    fn test_memory_limit() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);
        ctx.set_limit(1000);

        // First allocation should succeed
        ctx.alloc(500).unwrap();

        // Second allocation should fail (exceeds limit)
        let result = ctx.alloc(600);
        assert!(matches!(
            result,
            Err(MemoryContextError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn test_child_context() {
        let parent = MemoryContext::new("parent", MemoryContextType::Query);
        let child = MemoryContext::create_child(&parent, "child", MemoryContextType::Operation);

        assert_eq!(child.name(), "child");
        assert_eq!(parent.child_count(), 1);

        // Allocate in child
        child.alloc(100).unwrap();

        // Total should include child
        assert_eq!(parent.total_usage(), 100);
    }

    #[test]
    fn test_reset() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        ctx.alloc(100).unwrap();
        ctx.alloc(200).unwrap();

        assert!(ctx.current_usage() > 0);

        ctx.reset();

        let stats = ctx.stats();
        assert_eq!(stats.current_used, 0);
        assert_eq!(stats.reset_count, 1);
    }

    #[test]
    fn test_freeze() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        ctx.alloc(100).unwrap();

        ctx.freeze();

        let result = ctx.alloc(100);
        assert!(matches!(result, Err(MemoryContextError::ContextReadOnly)));

        ctx.unfreeze();
        ctx.alloc(100).unwrap();
    }

    #[test]
    fn test_query_memory_context() {
        let parent = MemoryContext::new("top", MemoryContextType::TopLevel);
        let mut query_ctx = QueryMemoryContext::new(1, &parent, 64 * 1024);

        query_ctx.context().alloc(1000).unwrap();

        // Get sort context
        let sort_ctx = query_ctx.sort_context();
        sort_ctx.alloc(500).unwrap();

        // Get hash context
        let hash_ctx = query_ctx.hash_context();
        hash_ctx.alloc(500).unwrap();

        assert_eq!(query_ctx.total_usage(), 2000);
    }

    #[test]
    fn test_memory_context_manager() {
        let manager = MemoryContextManager::new(1024 * 1024, 64 * 1024);

        let ctx = manager.create_query_context();
        ctx.context().alloc(1000).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.active_contexts, 1);

        let query_id = ctx.query_id();
        manager.release_query_context(query_id);

        let stats = manager.stats();
        assert_eq!(stats.active_contexts, 0);
        assert_eq!(stats.contexts_destroyed, 1);
    }

    #[test]
    fn test_peak_usage() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        ctx.alloc(1000).unwrap();
        let stats1 = ctx.stats();

        ctx.reset();

        ctx.alloc(500).unwrap();
        let stats2 = ctx.stats();

        // Peak should remember the 1000 byte allocation
        assert_eq!(stats2.peak_used, 1000);
    }

    #[test]
    fn test_print_tree() {
        let parent = MemoryContext::new("parent", MemoryContextType::Query);
        let child1 = MemoryContext::create_child(&parent, "child1", MemoryContextType::Operation);
        let child2 = MemoryContext::create_child(&parent, "child2", MemoryContextType::Operation);

        child1.alloc(100).unwrap();
        child2.alloc(200).unwrap();

        let tree = parent.print_tree(0);
        assert!(tree.contains("parent"));
        assert!(tree.contains("child1"));
        assert!(tree.contains("child2"));
    }

    #[test]
    fn test_allocation_tracker() {
        let tracker = AllocationTracker::new();
        tracker.enable();

        tracker.record("context1", 100, "small alloc");
        tracker.record("context1", 2 * 1024 * 1024, "large alloc");
        tracker.record("context2", 500, "medium alloc");

        let summary = tracker.summary();
        assert!(summary.by_context.contains_key("context1"));
        assert!(summary.by_context.contains_key("context2"));
        assert_eq!(summary.large_allocations.len(), 1);
    }

    #[test]
    fn test_average_allocation_size() {
        let ctx = MemoryContext::new("test", MemoryContextType::Query);

        ctx.alloc(100).unwrap();
        ctx.alloc(200).unwrap();
        ctx.alloc(300).unwrap();

        let stats = ctx.stats();
        let avg = stats.average_allocation_size();
        assert!((avg - 200.0).abs() < 0.01);
    }

    #[test]
    fn test_work_mem_check() {
        let parent = MemoryContext::new("top", MemoryContextType::TopLevel);
        let mut query_ctx = QueryMemoryContext::new(1, &parent, 1000);

        // Within limit
        query_ctx.sort_context().alloc(500).unwrap();
        assert!(query_ctx.within_work_mem());

        // Note: The work_mem check is advisory, doesn't prevent allocation
    }

    #[test]
    fn test_error_display() {
        let err = MemoryContextError::LimitExceeded {
            requested: 1000,
            limit: 500,
            used: 400,
        };
        assert!(format!("{}", err).contains("1000"));

        let err = MemoryContextError::ContextNotFound("test".to_string());
        assert!(format!("{}", err).contains("test"));
    }

    #[test]
    fn test_list_query_contexts() {
        let manager = MemoryContextManager::new(1024 * 1024, 64 * 1024);

        let ctx1 = manager.create_query_context();
        let ctx2 = manager.create_query_context();

        ctx1.context().alloc(100).unwrap();
        ctx2.context().alloc(200).unwrap();

        let contexts = manager.list_query_contexts();
        assert_eq!(contexts.len(), 2);
    }

    #[test]
    fn test_reset_all() {
        let manager = MemoryContextManager::new(1024 * 1024, 64 * 1024);

        manager.create_query_context();
        manager.create_query_context();

        let stats = manager.stats();
        assert_eq!(stats.active_contexts, 2);

        manager.reset_all();

        let stats = manager.stats();
        assert_eq!(stats.active_contexts, 0);
    }
}
