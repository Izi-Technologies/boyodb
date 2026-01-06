//! Projections - Pre-aggregated Views
//!
//! Projections are pre-computed aggregations stored alongside the main table data.
//! Unlike materialized views, projections are automatically maintained during inserts
//! and transparently used by the query optimizer.
//!
//! Example:
//! ```sql
//! ALTER TABLE events ADD PROJECTION hourly_stats (
//!     SELECT
//!         toStartOfHour(event_time) AS hour,
//!         tenant_id,
//!         COUNT(*) AS event_count,
//!         SUM(value) AS total_value
//!     GROUP BY hour, tenant_id
//! )
//! ```

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    builder::{Float64Builder, Int64Builder, StringBuilder, UInt64Builder},
};
use arrow_schema::{DataType, Field, Schema};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::EngineError;

/// Definition of a projection (pre-aggregated view)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionDef {
    /// Projection name
    pub name: String,
    /// Source table (database.table)
    pub source_table: String,
    /// GROUP BY columns
    pub group_by: Vec<String>,
    /// Aggregation expressions: (output_name, agg_type, input_column)
    pub aggregations: Vec<AggregationDef>,
    /// Optional time bucket column and granularity
    pub time_bucket: Option<TimeBucket>,
    /// Whether this projection is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool { true }

/// Time bucketing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBucket {
    /// Source timestamp column
    pub source_column: String,
    /// Output column name
    pub output_column: String,
    /// Bucket interval in seconds
    pub interval_seconds: u64,
}

/// Aggregation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationDef {
    /// Output column name
    pub output_name: String,
    /// Aggregation type
    pub agg_type: AggType,
    /// Input column (None for COUNT(*))
    pub input_column: Option<String>,
}

/// Supported aggregation types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggType {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggType::Count => "COUNT",
            AggType::Sum => "SUM",
            AggType::Min => "MIN",
            AggType::Max => "MAX",
            AggType::Avg => "AVG",
        }
    }
}

/// Accumulated aggregation state for a group
#[derive(Debug, Clone, Default)]
struct GroupState {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl GroupState {
    fn merge(&mut self, other: &GroupState) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
    }

    fn add_value(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    fn get(&self, agg_type: AggType) -> f64 {
        match agg_type {
            AggType::Count => self.count as f64,
            AggType::Sum => self.sum,
            AggType::Min => self.min.unwrap_or(0.0),
            AggType::Max => self.max.unwrap_or(0.0),
            AggType::Avg => if self.count > 0 { self.sum / self.count as f64 } else { 0.0 },
        }
    }
}

/// Group key for aggregation (supports multiple columns)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    values: Vec<GroupKeyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyValue {
    Int64(i64),
    UInt64(u64),
    String(String),
    Null,
}

impl GroupKey {
    fn from_row(batch: &RecordBatch, row: usize, columns: &[String]) -> Self {
        let values: Vec<GroupKeyValue> = columns
            .iter()
            .map(|col_name| {
                if let Ok(idx) = batch.schema().index_of(col_name) {
                    let col = batch.column(idx);
                    if col.is_null(row) {
                        GroupKeyValue::Null
                    } else {
                        match col.data_type() {
                            DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                GroupKeyValue::Int64(arr.value(row))
                            }
                            DataType::UInt64 => {
                                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                                GroupKeyValue::UInt64(arr.value(row))
                            }
                            DataType::Utf8 => {
                                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                GroupKeyValue::String(arr.value(row).to_string())
                            }
                            _ => GroupKeyValue::Null,
                        }
                    }
                } else {
                    GroupKeyValue::Null
                }
            })
            .collect();
        GroupKey { values }
    }
}

/// Materialized projection data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionData {
    /// The projection definition
    pub def: ProjectionDef,
    /// Segment ID where the projection data is stored
    pub segment_id: String,
    /// Number of groups in the projection
    pub num_groups: u64,
    /// Last update timestamp (micros)
    pub last_updated_micros: u64,
}

/// Build projection data from source batches
pub fn build_projection(
    def: &ProjectionDef,
    batches: &[RecordBatch],
) -> Result<RecordBatch, EngineError> {
    if batches.is_empty() {
        return Err(EngineError::InvalidArgument("no batches to project".into()));
    }

    // Track aggregation state per group
    let mut groups: FxHashMap<GroupKey, HashMap<String, GroupState>> = FxHashMap::default();

    for batch in batches {
        let num_rows = batch.num_rows();

        for row in 0..num_rows {
            // Build group key
            let group_key = GroupKey::from_row(batch, row, &def.group_by);

            // Get or create group state
            let group_states = groups.entry(group_key).or_insert_with(HashMap::new);

            // Update aggregation states
            for agg_def in &def.aggregations {
                let state = group_states.entry(agg_def.output_name.clone()).or_default();

                if agg_def.agg_type == AggType::Count {
                    state.count += 1;
                } else if let Some(ref col_name) = agg_def.input_column {
                    if let Ok(idx) = batch.schema().index_of(col_name) {
                        let col = batch.column(idx);
                        if !col.is_null(row) {
                            let value = extract_numeric_value(col.as_ref(), row);
                            state.add_value(value);
                        }
                    }
                }
            }
        }
    }

    // Build output schema
    let mut fields: Vec<Field> = def
        .group_by
        .iter()
        .map(|col| {
            // Try to infer type from first batch
            if let Ok(idx) = batches[0].schema().index_of(col) {
                Field::new(col, batches[0].schema().field(idx).data_type().clone(), true)
            } else {
                Field::new(col, DataType::Utf8, true)
            }
        })
        .collect();

    for agg_def in &def.aggregations {
        let dtype = match agg_def.agg_type {
            AggType::Count => DataType::Int64,
            _ => DataType::Float64,
        };
        fields.push(Field::new(&agg_def.output_name, dtype, true));
    }

    let schema = Arc::new(Schema::new(fields));

    // Build output arrays
    let num_groups = groups.len();
    let mut group_builders: Vec<Box<dyn ArrayBuilder>> = def
        .group_by
        .iter()
        .map(|col| {
            if let Ok(idx) = batches[0].schema().index_of(col) {
                match batches[0].schema().field(idx).data_type() {
                    DataType::Int64 => Box::new(Int64Builder::with_capacity(num_groups)) as Box<dyn ArrayBuilder>,
                    DataType::UInt64 => Box::new(UInt64Builder::with_capacity(num_groups)) as Box<dyn ArrayBuilder>,
                    _ => Box::new(StringBuilder::with_capacity(num_groups, num_groups * 32)) as Box<dyn ArrayBuilder>,
                }
            } else {
                Box::new(StringBuilder::with_capacity(num_groups, num_groups * 32)) as Box<dyn ArrayBuilder>
            }
        })
        .collect();

    let mut agg_builders: Vec<(AggType, Box<dyn ArrayBuilder>)> = def
        .aggregations
        .iter()
        .map(|agg_def| {
            let builder: Box<dyn ArrayBuilder> = match agg_def.agg_type {
                AggType::Count => Box::new(Int64Builder::with_capacity(num_groups)),
                _ => Box::new(Float64Builder::with_capacity(num_groups)),
            };
            (agg_def.agg_type, builder)
        })
        .collect();

    // Populate arrays
    for (key, states) in groups {
        // Add group key values
        for (i, value) in key.values.iter().enumerate() {
            match value {
                GroupKeyValue::Int64(v) => {
                    if let Some(builder) = group_builders[i].as_any_mut().downcast_mut::<Int64Builder>() {
                        builder.append_value(*v);
                    }
                }
                GroupKeyValue::UInt64(v) => {
                    if let Some(builder) = group_builders[i].as_any_mut().downcast_mut::<UInt64Builder>() {
                        builder.append_value(*v);
                    }
                }
                GroupKeyValue::String(v) => {
                    if let Some(builder) = group_builders[i].as_any_mut().downcast_mut::<StringBuilder>() {
                        builder.append_value(v);
                    }
                }
                GroupKeyValue::Null => {
                    if let Some(builder) = group_builders[i].as_any_mut().downcast_mut::<StringBuilder>() {
                        builder.append_null();
                    }
                }
            }
        }

        // Add aggregation values
        let default_state = GroupState::default();
        for (j, agg_def) in def.aggregations.iter().enumerate() {
            let state = states.get(&agg_def.output_name).unwrap_or(&default_state);
            let (agg_type, builder) = &mut agg_builders[j];
            match agg_type {
                AggType::Count => {
                    if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                        b.append_value(state.count as i64);
                    }
                }
                _ => {
                    if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                        b.append_value(state.get(*agg_type));
                    }
                }
            }
        }
    }

    // Build final arrays
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for builder in &mut group_builders {
        arrays.push(finish_builder(builder)?);
    }
    for (_, builder) in &mut agg_builders {
        arrays.push(finish_builder(builder)?);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| EngineError::Internal(format!("failed to build projection batch: {}", e)))
}

/// Trait for type-erased array builder
trait ArrayBuilder: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

impl ArrayBuilder for Int64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
}

impl ArrayBuilder for UInt64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
}

impl ArrayBuilder for Float64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
}

impl ArrayBuilder for StringBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
}

fn finish_builder(builder: &mut Box<dyn ArrayBuilder>) -> Result<ArrayRef, EngineError> {
    if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        return Ok(Arc::new(b.finish()));
    }
    if let Some(b) = builder.as_any_mut().downcast_mut::<UInt64Builder>() {
        return Ok(Arc::new(b.finish()));
    }
    if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        return Ok(Arc::new(b.finish()));
    }
    if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        return Ok(Arc::new(b.finish()));
    }
    Err(EngineError::Internal("unknown builder type".into()))
}

fn extract_numeric_value(arr: &dyn Array, row: usize) -> f64 {
    match arr.data_type() {
        DataType::Int64 => arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row) as f64,
        DataType::UInt64 => arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(row) as f64,
        DataType::Float64 => arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        _ => 0.0,
    }
}

/// Check if a query can use a projection
pub fn can_use_projection(
    projection: &ProjectionDef,
    query_group_by: &[String],
    query_aggregations: &[(String, AggType)],
) -> bool {
    // Query GROUP BY must be a subset of projection GROUP BY
    for col in query_group_by {
        if !projection.group_by.contains(col) {
            return false;
        }
    }

    // Query aggregations must be available in projection
    for (col, agg_type) in query_aggregations {
        let found = projection.aggregations.iter().any(|agg| {
            agg.agg_type == *agg_type && agg.input_column.as_ref().map_or(false, |c| c == col)
        });
        if !found && *agg_type != AggType::Count {
            return false;
        }
    }

    true
}

/// Merge projection results (for combining partial aggregations)
pub fn merge_projection_results(
    batches: Vec<RecordBatch>,
    group_by: &[String],
    aggregations: &[AggregationDef],
) -> Result<RecordBatch, EngineError> {
    if batches.is_empty() {
        return Err(EngineError::InvalidArgument("no batches to merge".into()));
    }

    // Re-aggregate the projection results
    let def = ProjectionDef {
        name: "merge".to_string(),
        source_table: "".to_string(),
        group_by: group_by.to_vec(),
        aggregations: aggregations.to_vec(),
        time_bucket: None,
        enabled: true,
    };

    build_projection(&def, &batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1, 1, 1, 2, 2, 2])),
                Arc::new(StringArray::from(vec!["click", "view", "click", "click", "view", "click"])),
                Arc::new(Float64Array::from(vec![10.0, 5.0, 15.0, 20.0, 8.0, 25.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_build_projection_count() {
        let def = ProjectionDef {
            name: "count_by_tenant".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["tenant_id".to_string()],
            aggregations: vec![AggregationDef {
                output_name: "event_count".to_string(),
                agg_type: AggType::Count,
                input_column: None,
            }],
            time_bucket: None,
            enabled: true,
        };

        let batch = make_test_batch();
        let result = build_projection(&def, &[batch]).unwrap();

        assert_eq!(result.num_rows(), 2); // Two unique tenants
        assert_eq!(result.num_columns(), 2); // tenant_id + event_count
    }

    #[test]
    fn test_build_projection_sum() {
        let def = ProjectionDef {
            name: "sum_by_tenant".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["tenant_id".to_string()],
            aggregations: vec![
                AggregationDef {
                    output_name: "event_count".to_string(),
                    agg_type: AggType::Count,
                    input_column: None,
                },
                AggregationDef {
                    output_name: "total_value".to_string(),
                    agg_type: AggType::Sum,
                    input_column: Some("value".to_string()),
                },
            ],
            time_bucket: None,
            enabled: true,
        };

        let batch = make_test_batch();
        let result = build_projection(&def, &[batch]).unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 3); // tenant_id + event_count + total_value

        // Check schema
        assert_eq!(result.schema().field(0).name(), "tenant_id");
        assert_eq!(result.schema().field(1).name(), "event_count");
        assert_eq!(result.schema().field(2).name(), "total_value");
    }

    #[test]
    fn test_build_projection_multiple_group_by() {
        let def = ProjectionDef {
            name: "count_by_tenant_type".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["tenant_id".to_string(), "event_type".to_string()],
            aggregations: vec![AggregationDef {
                output_name: "event_count".to_string(),
                agg_type: AggType::Count,
                input_column: None,
            }],
            time_bucket: None,
            enabled: true,
        };

        let batch = make_test_batch();
        let result = build_projection(&def, &[batch]).unwrap();

        // 4 unique combinations: (1, click), (1, view), (2, click), (2, view)
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_can_use_projection() {
        let projection = ProjectionDef {
            name: "hourly_stats".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["tenant_id".to_string(), "hour".to_string()],
            aggregations: vec![
                AggregationDef {
                    output_name: "count".to_string(),
                    agg_type: AggType::Count,
                    input_column: None,
                },
                AggregationDef {
                    output_name: "sum_value".to_string(),
                    agg_type: AggType::Sum,
                    input_column: Some("value".to_string()),
                },
            ],
            time_bucket: None,
            enabled: true,
        };

        // Can use: subset of group by columns
        assert!(can_use_projection(
            &projection,
            &["tenant_id".to_string()],
            &[("value".to_string(), AggType::Sum)],
        ));

        // Can use: exact match
        assert!(can_use_projection(
            &projection,
            &["tenant_id".to_string(), "hour".to_string()],
            &[],
        ));

        // Cannot use: group by column not in projection
        assert!(!can_use_projection(
            &projection,
            &["user_id".to_string()],
            &[],
        ));
    }

    #[test]
    fn test_projection_min_max() {
        let def = ProjectionDef {
            name: "minmax_by_tenant".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["tenant_id".to_string()],
            aggregations: vec![
                AggregationDef {
                    output_name: "min_value".to_string(),
                    agg_type: AggType::Min,
                    input_column: Some("value".to_string()),
                },
                AggregationDef {
                    output_name: "max_value".to_string(),
                    agg_type: AggType::Max,
                    input_column: Some("value".to_string()),
                },
            ],
            time_bucket: None,
            enabled: true,
        };

        let batch = make_test_batch();
        let result = build_projection(&def, &[batch]).unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 3);
    }
}
