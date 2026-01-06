// Streaming/Lazy Execution Module
// Pipeline operators for memory-efficient query processing

use arrow_array::{RecordBatch, ArrayRef, Array};
use arrow_schema::SchemaRef;
use std::collections::VecDeque;
use std::sync::Arc;

// ============================================================================
// Core Streaming Abstractions
// ============================================================================

/// Result of polling an operator for data
#[derive(Debug)]
pub enum PollResult {
    /// Batch is ready
    Ready(RecordBatch),
    /// Operator needs more input
    NeedsInput,
    /// Operator has finished producing output
    Exhausted,
    /// Temporary pause, try again later
    Pending,
}

/// A streaming operator that processes data in a pipeline fashion
pub trait StreamOperator: Send {
    /// Get the output schema
    fn schema(&self) -> SchemaRef;

    /// Poll for the next batch of data
    fn poll_next(&mut self) -> PollResult;

    /// Push input data to this operator
    fn push_input(&mut self, batch: RecordBatch) -> Result<(), StreamError>;

    /// Signal that no more input will arrive
    fn finish_input(&mut self);

    /// Check if operator can accept more input
    fn can_accept_input(&self) -> bool;

    /// Get operator name for debugging
    fn name(&self) -> &'static str;
}

/// Error type for streaming operations
#[derive(Debug, Clone)]
pub enum StreamError {
    /// Schema mismatch
    SchemaMismatch(String),
    /// Operator is not accepting input
    NotAcceptingInput,
    /// Internal error
    Internal(String),
    /// Memory limit exceeded
    MemoryExceeded(u64),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::SchemaMismatch(s) => write!(f, "Schema mismatch: {}", s),
            StreamError::NotAcceptingInput => write!(f, "Operator not accepting input"),
            StreamError::Internal(s) => write!(f, "Internal error: {}", s),
            StreamError::MemoryExceeded(bytes) => write!(f, "Memory limit exceeded: {} bytes", bytes),
        }
    }
}

impl std::error::Error for StreamError {}

// ============================================================================
// Streaming Filter Operator
// ============================================================================

/// Filter operator that applies predicates in a streaming fashion
pub struct StreamingFilter {
    schema: SchemaRef,
    predicate: Box<dyn Fn(&RecordBatch) -> Result<ArrayRef, StreamError> + Send>,
    input_queue: VecDeque<RecordBatch>,
    input_finished: bool,
}

impl StreamingFilter {
    pub fn new<F>(schema: SchemaRef, predicate: F) -> Self
    where
        F: Fn(&RecordBatch) -> Result<ArrayRef, StreamError> + Send + 'static,
    {
        Self {
            schema,
            predicate: Box::new(predicate),
            input_queue: VecDeque::new(),
            input_finished: false,
        }
    }
}

impl StreamOperator for StreamingFilter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn poll_next(&mut self) -> PollResult {
        while let Some(batch) = self.input_queue.pop_front() {
            match (self.predicate)(&batch) {
                Ok(mask) => {
                    if let Some(bool_arr) = mask.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                        match arrow_select::filter::filter_record_batch(&batch, bool_arr) {
                            Ok(filtered) if filtered.num_rows() > 0 => {
                                return PollResult::Ready(filtered);
                            }
                            Ok(_) => continue, // Empty result, try next batch
                            Err(_) => continue,
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        if self.input_finished {
            PollResult::Exhausted
        } else {
            PollResult::NeedsInput
        }
    }

    fn push_input(&mut self, batch: RecordBatch) -> Result<(), StreamError> {
        if self.input_finished {
            return Err(StreamError::NotAcceptingInput);
        }
        self.input_queue.push_back(batch);
        Ok(())
    }

    fn finish_input(&mut self) {
        self.input_finished = true;
    }

    fn can_accept_input(&self) -> bool {
        !self.input_finished && self.input_queue.len() < 100
    }

    fn name(&self) -> &'static str {
        "StreamingFilter"
    }
}

// ============================================================================
// Streaming Projection Operator
// ============================================================================

/// Project specific columns in a streaming fashion
pub struct StreamingProject {
    schema: SchemaRef,
    column_indices: Vec<usize>,
    input_queue: VecDeque<RecordBatch>,
    input_finished: bool,
}

impl StreamingProject {
    pub fn new(input_schema: &SchemaRef, columns: &[String]) -> Self {
        let column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|name| input_schema.index_of(name).ok())
            .collect();

        let output_fields: Vec<_> = column_indices
            .iter()
            .map(|&i| input_schema.field(i).clone())
            .collect();

        let schema = Arc::new(arrow_schema::Schema::new(output_fields));

        Self {
            schema,
            column_indices,
            input_queue: VecDeque::new(),
            input_finished: false,
        }
    }
}

impl StreamOperator for StreamingProject {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn poll_next(&mut self) -> PollResult {
        if let Some(batch) = self.input_queue.pop_front() {
            let columns: Vec<ArrayRef> = self
                .column_indices
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect();

            match RecordBatch::try_new(self.schema.clone(), columns) {
                Ok(projected) => PollResult::Ready(projected),
                Err(_) => {
                    if self.input_finished && self.input_queue.is_empty() {
                        PollResult::Exhausted
                    } else {
                        PollResult::NeedsInput
                    }
                }
            }
        } else if self.input_finished {
            PollResult::Exhausted
        } else {
            PollResult::NeedsInput
        }
    }

    fn push_input(&mut self, batch: RecordBatch) -> Result<(), StreamError> {
        if self.input_finished {
            return Err(StreamError::NotAcceptingInput);
        }
        self.input_queue.push_back(batch);
        Ok(())
    }

    fn finish_input(&mut self) {
        self.input_finished = true;
    }

    fn can_accept_input(&self) -> bool {
        !self.input_finished && self.input_queue.len() < 100
    }

    fn name(&self) -> &'static str {
        "StreamingProject"
    }
}

// ============================================================================
// Streaming Limit Operator
// ============================================================================

/// Limit operator for LIMIT/OFFSET in streaming fashion
pub struct StreamingLimit {
    schema: SchemaRef,
    limit: usize,
    offset: usize,
    rows_seen: usize,
    rows_emitted: usize,
    input_queue: VecDeque<RecordBatch>,
    input_finished: bool,
}

impl StreamingLimit {
    pub fn new(schema: SchemaRef, limit: usize, offset: usize) -> Self {
        Self {
            schema,
            limit,
            offset,
            rows_seen: 0,
            rows_emitted: 0,
            input_queue: VecDeque::new(),
            input_finished: false,
        }
    }
}

impl StreamOperator for StreamingLimit {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn poll_next(&mut self) -> PollResult {
        // Already emitted enough rows
        if self.rows_emitted >= self.limit {
            return PollResult::Exhausted;
        }

        while let Some(batch) = self.input_queue.pop_front() {
            let batch_rows = batch.num_rows();
            let start_row = self.rows_seen;
            self.rows_seen += batch_rows;

            // Skip rows for offset
            if self.rows_seen <= self.offset {
                continue; // Entire batch is before offset
            }

            // Calculate which rows to take from this batch
            let skip_in_batch = if start_row < self.offset {
                self.offset - start_row
            } else {
                0
            };

            let remaining_limit = self.limit - self.rows_emitted;
            let take_count = (batch_rows - skip_in_batch).min(remaining_limit);

            if take_count > 0 {
                let sliced = batch.slice(skip_in_batch, take_count);
                self.rows_emitted += sliced.num_rows();
                return PollResult::Ready(sliced);
            }
        }

        if self.input_finished || self.rows_emitted >= self.limit {
            PollResult::Exhausted
        } else {
            PollResult::NeedsInput
        }
    }

    fn push_input(&mut self, batch: RecordBatch) -> Result<(), StreamError> {
        if self.input_finished || self.rows_emitted >= self.limit {
            return Err(StreamError::NotAcceptingInput);
        }
        self.input_queue.push_back(batch);
        Ok(())
    }

    fn finish_input(&mut self) {
        self.input_finished = true;
    }

    fn can_accept_input(&self) -> bool {
        !self.input_finished && self.rows_emitted < self.limit
    }

    fn name(&self) -> &'static str {
        "StreamingLimit"
    }
}

// ============================================================================
// Streaming Hash Aggregate (Partial)
// ============================================================================

use std::collections::HashMap;
use arrow_array::{Int64Array, Float64Array, UInt64Array};
use arrow_array::builder::{Int64Builder, Float64Builder, UInt64Builder, StringBuilder};
use arrow_schema::{DataType, Field, Schema};

/// Accumulator for aggregation
#[derive(Debug, Clone, Default)]
pub struct AggAccumulator {
    pub count: i64,
    pub sum_i64: i64,
    pub sum_f64: f64,
    pub min_i64: Option<i64>,
    pub max_i64: Option<i64>,
    pub min_f64: Option<f64>,
    pub max_f64: Option<f64>,
}

impl AggAccumulator {
    pub fn update_i64(&mut self, value: i64) {
        self.count += 1;
        self.sum_i64 += value;
        self.min_i64 = Some(self.min_i64.map_or(value, |m| m.min(value)));
        self.max_i64 = Some(self.max_i64.map_or(value, |m| m.max(value)));
    }

    pub fn update_f64(&mut self, value: f64) {
        self.count += 1;
        self.sum_f64 += value;
        self.min_f64 = Some(self.min_f64.map_or(value, |m| m.min(value)));
        self.max_f64 = Some(self.max_f64.map_or(value, |m| m.max(value)));
    }

    pub fn merge(&mut self, other: &AggAccumulator) {
        self.count += other.count;
        self.sum_i64 += other.sum_i64;
        self.sum_f64 += other.sum_f64;
        self.min_i64 = match (self.min_i64, other.min_i64) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) => a,
            (None, b) => b,
        };
        self.max_i64 = match (self.max_i64, other.max_i64) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, None) => a,
            (None, b) => b,
        };
        self.min_f64 = match (self.min_f64, other.min_f64) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) => a,
            (None, b) => b,
        };
        self.max_f64 = match (self.max_f64, other.max_f64) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, None) => a,
            (None, b) => b,
        };
    }
}

/// Streaming hash aggregation operator
pub struct StreamingHashAggregate {
    output_schema: SchemaRef,
    group_by_cols: Vec<String>,
    agg_col: Option<String>,
    agg_type: AggType,
    groups: HashMap<String, AggAccumulator>,
    input_finished: bool,
    output_emitted: bool,
    memory_limit: u64,
    current_memory: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl StreamingHashAggregate {
    pub fn new(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        agg_col: Option<String>,
        agg_type: AggType,
        memory_limit: u64,
    ) -> Self {
        // Build output schema
        let mut fields: Vec<Field> = group_by
            .iter()
            .filter_map(|name| input_schema.field_with_name(name).ok().cloned())
            .collect();

        // Add aggregation result field
        let agg_field = match agg_type {
            AggType::Count => Field::new("count", DataType::Int64, false),
            AggType::Sum => Field::new("sum", DataType::Float64, true),
            AggType::Avg => Field::new("avg", DataType::Float64, true),
            AggType::Min => Field::new("min", DataType::Float64, true),
            AggType::Max => Field::new("max", DataType::Float64, true),
        };
        fields.push(agg_field);

        Self {
            output_schema: Arc::new(Schema::new(fields)),
            group_by_cols: group_by,
            agg_col,
            agg_type,
            groups: HashMap::new(),
            input_finished: false,
            output_emitted: false,
            memory_limit,
            current_memory: 0,
        }
    }

    fn extract_group_key(&self, batch: &RecordBatch, row: usize) -> String {
        let mut key = String::new();
        for col_name in &self.group_by_cols {
            if let Ok(col_idx) = batch.schema().index_of(col_name) {
                let col = batch.column(col_idx);
                // Simple string representation for grouping
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                    if arr.is_valid(row) {
                        key.push_str(arr.value(row));
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    if arr.is_valid(row) {
                        key.push_str(&arr.value(row).to_string());
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    if arr.is_valid(row) {
                        key.push_str(&arr.value(row).to_string());
                    }
                }
                key.push('\0'); // Separator
            }
        }
        key
    }

    fn build_output(&self) -> Option<RecordBatch> {
        if self.groups.is_empty() {
            return None;
        }

        let num_groups = self.groups.len();

        // For simplicity, assume single string group by column
        let mut group_builder = StringBuilder::new();
        let mut result_builder = Float64Builder::new();

        for (key, acc) in &self.groups {
            // Remove trailing separator
            let clean_key = key.trim_end_matches('\0');
            group_builder.append_value(clean_key);

            let value = match self.agg_type {
                AggType::Count => acc.count as f64,
                AggType::Sum => acc.sum_f64 + acc.sum_i64 as f64,
                AggType::Avg => {
                    if acc.count > 0 {
                        (acc.sum_f64 + acc.sum_i64 as f64) / acc.count as f64
                    } else {
                        0.0
                    }
                }
                AggType::Min => acc.min_f64.or(acc.min_i64.map(|v| v as f64)).unwrap_or(0.0),
                AggType::Max => acc.max_f64.or(acc.max_i64.map(|v| v as f64)).unwrap_or(0.0),
            };
            result_builder.append_value(value);
        }

        // Build simple schema for output
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_key", DataType::Utf8, false),
            Field::new("agg_result", DataType::Float64, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(group_builder.finish()),
            Arc::new(result_builder.finish()),
        ];

        RecordBatch::try_new(schema, columns).ok()
    }
}

impl StreamOperator for StreamingHashAggregate {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn poll_next(&mut self) -> PollResult {
        if self.output_emitted {
            return PollResult::Exhausted;
        }

        if !self.input_finished {
            return PollResult::NeedsInput;
        }

        // Build and emit final result
        self.output_emitted = true;
        if let Some(batch) = self.build_output() {
            PollResult::Ready(batch)
        } else {
            PollResult::Exhausted
        }
    }

    fn push_input(&mut self, batch: RecordBatch) -> Result<(), StreamError> {
        if self.input_finished {
            return Err(StreamError::NotAcceptingInput);
        }

        // Find aggregation column
        let agg_col_idx = self.agg_col.as_ref().and_then(|name| {
            batch.schema().index_of(name).ok()
        });

        for row in 0..batch.num_rows() {
            let key = self.extract_group_key(&batch, row);

            let acc = self.groups.entry(key).or_insert_with(|| {
                self.current_memory += 100; // Rough estimate
                AggAccumulator::default()
            });

            // Update accumulator
            if let Some(col_idx) = agg_col_idx {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    if arr.is_valid(row) {
                        acc.update_i64(arr.value(row));
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    if arr.is_valid(row) {
                        acc.update_f64(arr.value(row));
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    if arr.is_valid(row) {
                        acc.update_i64(arr.value(row) as i64);
                    }
                } else {
                    // For count, just increment
                    acc.count += 1;
                }
            } else {
                // COUNT(*) - just increment
                acc.count += 1;
            }
        }

        // Check memory limit
        if self.current_memory > self.memory_limit {
            return Err(StreamError::MemoryExceeded(self.current_memory));
        }

        Ok(())
    }

    fn finish_input(&mut self) {
        self.input_finished = true;
    }

    fn can_accept_input(&self) -> bool {
        !self.input_finished && self.current_memory < self.memory_limit
    }

    fn name(&self) -> &'static str {
        "StreamingHashAggregate"
    }
}

// ============================================================================
// Pipeline Executor
// ============================================================================

/// Executes a pipeline of streaming operators
pub struct PipelineExecutor {
    operators: Vec<Box<dyn StreamOperator>>,
}

impl PipelineExecutor {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    pub fn add_operator(&mut self, op: Box<dyn StreamOperator>) {
        self.operators.push(op);
    }

    /// Execute the pipeline with a source of input batches
    pub fn execute<I>(&mut self, source: I) -> Vec<RecordBatch>
    where
        I: Iterator<Item = RecordBatch>,
    {
        let mut results = Vec::new();

        if self.operators.is_empty() {
            // No operators, just collect source
            return source.collect();
        }

        // Push all source data through the pipeline
        for batch in source {
            self.push_batch(batch);
            self.collect_outputs(&mut results);
        }

        // Finish all operators and collect remaining output
        for op in &mut self.operators {
            op.finish_input();
        }

        // Keep polling until all exhausted
        loop {
            let collected_any = self.collect_outputs(&mut results);
            if !collected_any {
                break;
            }
        }

        results
    }

    fn push_batch(&mut self, batch: RecordBatch) {
        if self.operators.is_empty() {
            return;
        }

        // Push to first operator
        let _ = self.operators[0].push_input(batch);

        // Propagate through pipeline
        for i in 0..self.operators.len() - 1 {
            while let PollResult::Ready(output) = self.operators[i].poll_next() {
                if self.operators[i + 1].can_accept_input() {
                    let _ = self.operators[i + 1].push_input(output);
                }
            }
        }
    }

    fn collect_outputs(&mut self, results: &mut Vec<RecordBatch>) -> bool {
        let mut collected_any = false;

        // Propagate through pipeline
        for i in 0..self.operators.len().saturating_sub(1) {
            while let PollResult::Ready(output) = self.operators[i].poll_next() {
                if self.operators[i + 1].can_accept_input() {
                    let _ = self.operators[i + 1].push_input(output);
                    collected_any = true;
                }
            }
        }

        // Collect from last operator
        if let Some(last) = self.operators.last_mut() {
            while let PollResult::Ready(batch) = last.poll_next() {
                results.push(batch);
                collected_any = true;
            }
        }

        collected_any
    }
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Lazy Batch Iterator
// ============================================================================

/// A lazy iterator that only materializes batches on demand
pub struct LazyBatchIterator<F>
where
    F: FnMut() -> Option<RecordBatch>,
{
    producer: F,
    buffer: VecDeque<RecordBatch>,
    buffer_limit: usize,
    exhausted: bool,
}

impl<F> LazyBatchIterator<F>
where
    F: FnMut() -> Option<RecordBatch>,
{
    pub fn new(producer: F, buffer_limit: usize) -> Self {
        Self {
            producer,
            buffer: VecDeque::new(),
            buffer_limit,
            exhausted: false,
        }
    }

    /// Pre-fetch batches into buffer
    pub fn prefetch(&mut self, count: usize) {
        for _ in 0..count {
            if self.exhausted || self.buffer.len() >= self.buffer_limit {
                break;
            }
            match (self.producer)() {
                Some(batch) => self.buffer.push_back(batch),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }
    }
}

impl<F> Iterator for LazyBatchIterator<F>
where
    F: FnMut() -> Option<RecordBatch>,
{
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Try buffer first
        if let Some(batch) = self.buffer.pop_front() {
            return Some(batch);
        }

        // If exhausted, no more
        if self.exhausted {
            return None;
        }

        // Produce on demand
        match (self.producer)() {
            Some(batch) => Some(batch),
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{Field, Schema};

    fn create_test_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_streaming_project() {
        let batch = create_test_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let schema = batch.schema();

        let mut project = StreamingProject::new(&schema, &["name".to_string()]);
        project.push_input(batch).unwrap();
        project.finish_input();

        match project.poll_next() {
            PollResult::Ready(result) => {
                assert_eq!(result.num_columns(), 1);
                assert_eq!(result.num_rows(), 3);
            }
            _ => panic!("Expected ready result"),
        }
    }

    #[test]
    fn test_streaming_limit() {
        let batch = create_test_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
        let schema = batch.schema();

        let mut limit = StreamingLimit::new(schema, 2, 1);
        limit.push_input(batch).unwrap();
        limit.finish_input();

        match limit.poll_next() {
            PollResult::Ready(result) => {
                assert_eq!(result.num_rows(), 2);
                let id_col = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(id_col.value(0), 2); // Offset 1
                assert_eq!(id_col.value(1), 3);
            }
            _ => panic!("Expected ready result"),
        }
    }

    #[test]
    fn test_streaming_limit_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let mut limit = StreamingLimit::new(schema.clone(), 3, 2);

        // Push multiple batches
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        ).unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3, 4, 5]))],
        ).unwrap();

        limit.push_input(batch1).unwrap();
        limit.push_input(batch2).unwrap();
        limit.finish_input();

        let mut total_rows = 0;
        loop {
            match limit.poll_next() {
                PollResult::Ready(batch) => {
                    total_rows += batch.num_rows();
                }
                PollResult::Exhausted => break,
                _ => panic!("Unexpected poll result"),
            }
        }

        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_streaming_aggregate_count() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        ).unwrap();

        let mut agg = StreamingHashAggregate::new(
            &schema,
            vec!["group".to_string()],
            None,
            AggType::Count,
            1024 * 1024,
        );

        agg.push_input(batch).unwrap();
        agg.finish_input();

        match agg.poll_next() {
            PollResult::Ready(result) => {
                assert_eq!(result.num_rows(), 2); // Two groups: a, b
            }
            _ => panic!("Expected ready result"),
        }
    }

    #[test]
    fn test_streaming_aggregate_sum() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        ).unwrap();

        let mut agg = StreamingHashAggregate::new(
            &schema,
            vec!["group".to_string()],
            Some("value".to_string()),
            AggType::Sum,
            1024 * 1024,
        );

        agg.push_input(batch).unwrap();
        agg.finish_input();

        match agg.poll_next() {
            PollResult::Ready(result) => {
                assert_eq!(result.num_rows(), 1);
                let sum_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(sum_col.value(0), 6.0); // 1 + 2 + 3
            }
            _ => panic!("Expected ready result"),
        }
    }

    #[test]
    fn test_pipeline_executor() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        ).unwrap();

        let mut executor = PipelineExecutor::new();
        executor.add_operator(Box::new(StreamingProject::new(&schema, &["id".to_string()])));
        executor.add_operator(Box::new(StreamingLimit::new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            3,
            0,
        )));

        let results = executor.execute(std::iter::once(batch));

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_lazy_batch_iterator() {
        let mut counter = 0;
        let mut iter = LazyBatchIterator::new(
            move || {
                if counter < 3 {
                    counter += 1;
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                    ]));
                    Some(RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![counter]))],
                    ).unwrap())
                } else {
                    None
                }
            },
            10,
        );

        let batches: Vec<_> = iter.collect();
        assert_eq!(batches.len(), 3);
    }

    #[test]
    fn test_lazy_iterator_prefetch() {
        let mut counter = 0;
        let mut iter = LazyBatchIterator::new(
            move || {
                if counter < 5 {
                    counter += 1;
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                    ]));
                    Some(RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![counter]))],
                    ).unwrap())
                } else {
                    None
                }
            },
            10,
        );

        // Prefetch 3 batches
        iter.prefetch(3);
        assert_eq!(iter.buffer.len(), 3);

        // Take all
        let batches: Vec<_> = iter.collect();
        assert_eq!(batches.len(), 5);
    }

    #[test]
    fn test_accumulator_merge() {
        let mut acc1 = AggAccumulator::default();
        acc1.update_i64(10);
        acc1.update_i64(20);

        let mut acc2 = AggAccumulator::default();
        acc2.update_i64(5);
        acc2.update_i64(30);

        acc1.merge(&acc2);

        assert_eq!(acc1.count, 4);
        assert_eq!(acc1.sum_i64, 65);
        assert_eq!(acc1.min_i64, Some(5));
        assert_eq!(acc1.max_i64, Some(30));
    }
}
