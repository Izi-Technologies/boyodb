//! Optimizer Integration Module
//!
//! Provides integration between the SQL parser, cost-based optimizer, and query executor.
//! This module bridges:
//! - ParsedQuery (from sql.rs) -> LogicalPlan (optimizer)
//! - LogicalPlan -> PhysicalPlan (via CostBasedOptimizer)
//! - PhysicalPlan -> Execution (via PhysicalPlanExecutor)

use crate::engine::{Db, EngineError, QueryResponse};
use crate::optimizer::{
    AggFunction, AggregateExpr, CompareOp, CostBasedOptimizer, IndexInfo, JoinCondition,
    JoinType as OptimizerJoinType, LogicalPlan, PhysicalPlan, Predicate, ProjectExpr, SortExpr,
    StatValue, TableStats,
};
use crate::sql::{
    AggPlan, JoinClause, JoinType, NumericValue, OrderByClause, ParsedQuery, QueryFilter,
    SelectColumn, SelectExpr,
};
use arrow::compute::kernels::boolean as arrow_boolean;
use arrow::compute::is_null as arrow_is_null;
use arrow_array::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// ParsedQuery -> LogicalPlan Conversion
// ============================================================================

/// Convert a ParsedQuery to a LogicalPlan for optimization
pub fn parsed_query_to_logical_plan(query: &ParsedQuery) -> Result<LogicalPlan, EngineError> {
    let database = query
        .database
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let table = query
        .table
        .clone()
        .ok_or_else(|| EngineError::InvalidArgument("Query must specify a table".into()))?;

    // Start with a table scan
    let mut plan = LogicalPlan::Scan {
        table: table.clone(),
        database: database.clone(),
        columns: vec![], // Will be filled by projection
        filter: None,
    };

    // Add JOINs if present
    for join in &query.joins {
        let right_plan = LogicalPlan::Scan {
            table: join.table.clone(),
            database: join.database.clone(),
            columns: vec![],
            filter: None,
        };

        let join_type = match join.join_type {
            JoinType::Inner => OptimizerJoinType::Inner,
            JoinType::Left => OptimizerJoinType::Left,
            JoinType::Right => OptimizerJoinType::Right,
            JoinType::FullOuter => OptimizerJoinType::Full,
            JoinType::Cross => OptimizerJoinType::Cross,
        };

        let condition = JoinCondition::None; // Simplified - would parse ON clause

        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            join_type,
            condition,
        };
    }

    // Add filter if present
    if has_filters(&query.filter) {
        let predicate = query_filter_to_predicate(&query.filter);
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // Add aggregation if present
    if let Some(ref agg) = query.aggregation {
        let aggregates = agg_plan_to_aggregate_exprs(agg);
        let group_by = match &agg.group_by {
            crate::sql::GroupBy::None => vec![],
            crate::sql::GroupBy::Tenant => vec!["tenant_id".to_string()],
            crate::sql::GroupBy::Route => vec!["route_id".to_string()],
            crate::sql::GroupBy::Columns(cols) => cols
                .iter()
                .map(|c| match c {
                    crate::sql::GroupByColumn::TenantId => "tenant_id".to_string(),
                    crate::sql::GroupByColumn::RouteId => "route_id".to_string(),
                })
                .collect(),
        };

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by,
            aggregates,
        };
    }

    // Add projection
    if let Some(ref cols) = query.projection {
        let columns: Vec<ProjectExpr> = cols.iter().map(|c| ProjectExpr::Column(c.clone())).collect();
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns,
        };
    } else if !query.computed_columns.is_empty() {
        let columns: Vec<ProjectExpr> = query
            .computed_columns
            .iter()
            .map(select_column_to_project_expr)
            .collect();
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns,
        };
    }

    // Add ORDER BY if present
    if let Some(ref order_by) = query.order_by {
        let sort_exprs: Vec<SortExpr> = order_by
            .iter()
            .map(|ob| SortExpr {
                column: ob.column.clone(),
                descending: !ob.ascending,
                nulls_first: ob.nulls_first.unwrap_or(false),
            })
            .collect();

        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: sort_exprs,
        };
    }

    // Add LIMIT/OFFSET if present
    if query.filter.limit.is_some() || query.filter.offset.is_some() {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: query.filter.limit.unwrap_or(usize::MAX),
            offset: query.filter.offset.unwrap_or(0),
        };
    }

    Ok(plan)
}

fn has_filters(filter: &QueryFilter) -> bool {
    filter.watermark_ge.is_some()
        || filter.watermark_le.is_some()
        || filter.event_time_ge.is_some()
        || filter.event_time_le.is_some()
        || filter.tenant_id_eq.is_some()
        || filter.route_id_eq.is_some()
        || !filter.numeric_eq_filters.is_empty()
        || !filter.numeric_range_filters.is_empty()
        || !filter.string_eq_filters.is_empty()
        || !filter.like_filters.is_empty()
        || !filter.null_filters.is_empty()
}

fn query_filter_to_predicate(filter: &QueryFilter) -> Predicate {
    let mut predicates = Vec::new();

    if let Some(wm) = filter.watermark_ge {
        predicates.push(Predicate::Comparison {
            column: "watermark_micros".to_string(),
            op: CompareOp::Ge,
            value: StatValue::Int64(wm as i64),
        });
    }

    if let Some(wm) = filter.watermark_le {
        predicates.push(Predicate::Comparison {
            column: "watermark_micros".to_string(),
            op: CompareOp::Le,
            value: StatValue::Int64(wm as i64),
        });
    }

    if let Some(et) = filter.event_time_ge {
        predicates.push(Predicate::Comparison {
            column: "event_time".to_string(),
            op: CompareOp::Ge,
            value: StatValue::Int64(et as i64),
        });
    }

    if let Some(et) = filter.event_time_le {
        predicates.push(Predicate::Comparison {
            column: "event_time".to_string(),
            op: CompareOp::Le,
            value: StatValue::Int64(et as i64),
        });
    }

    if let Some(tid) = filter.tenant_id_eq {
        predicates.push(Predicate::Comparison {
            column: "tenant_id".to_string(),
            op: CompareOp::Eq,
            value: StatValue::Int64(tid as i64),
        });
    }

    if let Some(rid) = filter.route_id_eq {
        predicates.push(Predicate::Comparison {
            column: "route_id".to_string(),
            op: CompareOp::Eq,
            value: StatValue::Int64(rid as i64),
        });
    }

    // Convert numeric equality filters
    for (col, val) in &filter.numeric_eq_filters {
        let stat_val = match val {
            NumericValue::Int64(i) => StatValue::Int64(*i),
            NumericValue::UInt64(u) => StatValue::Int64(*u as i64),
            NumericValue::Float64(f) => StatValue::Float64(*f),
        };
        predicates.push(Predicate::Comparison {
            column: col.clone(),
            op: CompareOp::Eq,
            value: stat_val,
        });
    }

    // Convert numeric range filters
    for nf in &filter.numeric_range_filters {
        let stat_val = match &nf.value {
            NumericValue::Int64(i) => StatValue::Int64(*i),
            NumericValue::UInt64(u) => StatValue::Int64(*u as i64),
            NumericValue::Float64(f) => StatValue::Float64(*f),
        };
        let op = match nf.op {
            crate::sql::NumericOp::Lt => CompareOp::Lt,
            crate::sql::NumericOp::Le => CompareOp::Le,
            crate::sql::NumericOp::Gt => CompareOp::Gt,
            crate::sql::NumericOp::Ge => CompareOp::Ge,
            crate::sql::NumericOp::Eq => CompareOp::Eq,
            crate::sql::NumericOp::Ne => CompareOp::Ne,
        };
        predicates.push(Predicate::Comparison {
            column: nf.column.clone(),
            op,
            value: stat_val,
        });
    }

    // Convert string equality filters
    for (col, val) in &filter.string_eq_filters {
        predicates.push(Predicate::Comparison {
            column: col.clone(),
            op: CompareOp::Eq,
            value: StatValue::String(val.clone()),
        });
    }

    // Convert LIKE filters
    for (col, pattern, negated) in &filter.like_filters {
        predicates.push(Predicate::Like {
            column: col.clone(),
            pattern: pattern.clone(),
            negated: *negated,
        });
    }

    // Convert NULL filters
    for (col, is_null) in &filter.null_filters {
        predicates.push(Predicate::IsNull {
            column: col.clone(),
            negated: !*is_null,
        });
    }

    match predicates.len() {
        0 => Predicate::True,
        1 => predicates.remove(0),
        _ => Predicate::And(predicates),
    }
}

fn agg_plan_to_aggregate_exprs(agg: &AggPlan) -> Vec<AggregateExpr> {
    use crate::sql::AggKind;

    agg.aggs
        .iter()
        .map(|agg_expr| {
            // Use the alias from the SQL expression if provided, otherwise use default
            let get_alias = |default: String| -> Option<String> {
                agg_expr.alias.clone().or(Some(default))
            };

            match &agg_expr.kind {
                AggKind::CountStar => AggregateExpr {
                    function: AggFunction::Count,
                    column: None,
                    distinct: false,
                    alias: get_alias("count".to_string()),
                },
                AggKind::CountDistinct { column } => AggregateExpr {
                    function: AggFunction::CountDistinct,
                    column: Some(column.clone()),
                    distinct: true,
                    alias: get_alias(format!("count_distinct_{}", column)),
                },
                AggKind::Sum { column } => AggregateExpr {
                    function: AggFunction::Sum,
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("sum_{}", column)),
                },
                AggKind::Avg { column } => AggregateExpr {
                    function: AggFunction::Avg,
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("avg_{}", column)),
                },
                AggKind::Min { column } => AggregateExpr {
                    function: AggFunction::Min,
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("min_{}", column)),
                },
                AggKind::Max { column } => AggregateExpr {
                    function: AggFunction::Max,
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("max_{}", column)),
                },
                AggKind::StddevSamp { column } => AggregateExpr {
                    function: AggFunction::Sum, // Use Sum as placeholder for unsupported
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("stddev_samp_{}", column)),
                },
                AggKind::StddevPop { column } => AggregateExpr {
                    function: AggFunction::Sum, // Use Sum as placeholder for unsupported
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("stddev_pop_{}", column)),
                },
                AggKind::VarianceSamp { column } => AggregateExpr {
                    function: AggFunction::Sum, // Use Sum as placeholder for unsupported
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("var_samp_{}", column)),
                },
                AggKind::VariancePop { column } => AggregateExpr {
                    function: AggFunction::Sum, // Use Sum as placeholder for unsupported
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("var_pop_{}", column)),
                },
                AggKind::ApproxCountDistinct { column } => AggregateExpr {
                    function: AggFunction::CountDistinct, // Approximate as distinct count
                    column: Some(column.clone()),
                    distinct: true,
                    alias: get_alias(format!("approx_count_distinct_{}", column)),
                },
                AggKind::Median { column } => AggregateExpr {
                    function: AggFunction::Avg, // Use Avg as placeholder - actual impl in engine
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("median_{}", column)),
                },
                AggKind::PercentileCont { column, percentile } => AggregateExpr {
                    function: AggFunction::Avg, // Placeholder
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("percentile_cont_{}_{}", (*percentile * 100.0) as i32, column)),
                },
                AggKind::PercentileDisc { column, percentile } => AggregateExpr {
                    function: AggFunction::Avg, // Placeholder
                    column: Some(column.clone()),
                    distinct: false,
                    alias: get_alias(format!("percentile_disc_{}_{}", (*percentile * 100.0) as i32, column)),
                },
                AggKind::ArrayAgg { column, distinct } => AggregateExpr {
                    function: AggFunction::Sum, // Placeholder
                    column: Some(column.clone()),
                    distinct: *distinct,
                    alias: get_alias(format!("array_agg_{}", column)),
                },
                AggKind::StringAgg { column, distinct, .. } => AggregateExpr {
                    function: AggFunction::Sum, // Placeholder
                    column: Some(column.clone()),
                    distinct: *distinct,
                    alias: get_alias(format!("string_agg_{}", column)),
                },
            }
        })
        .collect()
}

fn select_column_to_project_expr(col: &SelectColumn) -> ProjectExpr {
    let expr = select_expr_to_project_expr(&col.expr);
    if let Some(ref alias) = col.alias {
        ProjectExpr::Alias {
            expr: Box::new(expr),
            alias: alias.clone(),
        }
    } else {
        expr
    }
}

fn select_expr_to_project_expr(expr: &SelectExpr) -> ProjectExpr {
    match expr {
        SelectExpr::Column(name) => ProjectExpr::Column(name.clone()),
        SelectExpr::QualifiedColumn { table, column } => {
            ProjectExpr::Column(format!("{}.{}", table, column))
        }
        SelectExpr::Literal(lit) => {
            let value = match lit {
                crate::sql::LiteralValue::Integer(i) => StatValue::Int64(*i),
                crate::sql::LiteralValue::Float(f) => StatValue::Float64(*f),
                crate::sql::LiteralValue::String(s) => StatValue::String(s.clone()),
                crate::sql::LiteralValue::Boolean(b) => StatValue::Bool(*b),
                crate::sql::LiteralValue::Null => StatValue::Null,
            };
            ProjectExpr::Literal(value)
        }
        SelectExpr::Function(func) => scalar_function_to_project_expr(func),
        SelectExpr::Aggregate(agg_kind) => {
            // Map aggregate functions to placeholder expressions
            let name = match agg_kind {
                crate::sql::AggKind::CountStar => "COUNT(*)".to_string(),
                crate::sql::AggKind::CountDistinct { column } => {
                    format!("COUNT(DISTINCT {})", column)
                }
                crate::sql::AggKind::Sum { column } => format!("SUM({})", column),
                crate::sql::AggKind::Avg { column } => format!("AVG({})", column),
                crate::sql::AggKind::Min { column } => format!("MIN({})", column),
                crate::sql::AggKind::Max { column } => format!("MAX({})", column),
                crate::sql::AggKind::StddevSamp { column } => format!("STDDEV({})", column),
                crate::sql::AggKind::StddevPop { column } => format!("STDDEV_POP({})", column),
                crate::sql::AggKind::VarianceSamp { column } => format!("VARIANCE({})", column),
                crate::sql::AggKind::VariancePop { column } => format!("VAR_POP({})", column),
                crate::sql::AggKind::ApproxCountDistinct { column } => {
                    format!("APPROX_COUNT_DISTINCT({})", column)
                }
                crate::sql::AggKind::Median { column } => format!("MEDIAN({})", column),
                crate::sql::AggKind::PercentileCont { column, percentile } => {
                    format!("PERCENTILE_CONT({}, {})", column, percentile)
                }
                crate::sql::AggKind::PercentileDisc { column, percentile } => {
                    format!("PERCENTILE_DISC({}, {})", column, percentile)
                }
                crate::sql::AggKind::ArrayAgg { column, distinct } => {
                    if *distinct {
                        format!("ARRAY_AGG(DISTINCT {})", column)
                    } else {
                        format!("ARRAY_AGG({})", column)
                    }
                }
                crate::sql::AggKind::StringAgg { column, delimiter, distinct } => {
                    if *distinct {
                        format!("STRING_AGG(DISTINCT {}, '{}')", column, delimiter)
                    } else {
                        format!("STRING_AGG({}, '{}')", column, delimiter)
                    }
                }
            };
            ProjectExpr::Column(name)
        }
        SelectExpr::Window { function, .. } => {
            // Map window functions to placeholder expressions
            let name = format!("{:?}", function);
            ProjectExpr::Column(name)
        }
        SelectExpr::BinaryOp { left, op, right } => ProjectExpr::Function {
            name: op.clone(),
            args: vec![
                select_expr_to_project_expr(left),
                select_expr_to_project_expr(right),
            ],
        },
        SelectExpr::UnaryOp { op, expr } => ProjectExpr::Function {
            name: op.clone(),
            args: vec![select_expr_to_project_expr(expr)],
        },
        SelectExpr::Case { .. } => ProjectExpr::Column("case_expr".to_string()),
        SelectExpr::Null => ProjectExpr::Literal(StatValue::Null),
        SelectExpr::Subquery(_) => ProjectExpr::Column("subquery".to_string()),
    }
}

fn scalar_function_to_project_expr(func: &crate::sql::ScalarFunction) -> ProjectExpr {
    use crate::sql::ScalarFunction;

    match func {
        ScalarFunction::Upper(e) => ProjectExpr::Function {
            name: "UPPER".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Lower(e) => ProjectExpr::Function {
            name: "LOWER".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Length(e) => ProjectExpr::Function {
            name: "LENGTH".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Trim(e) => ProjectExpr::Function {
            name: "TRIM".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::LTrim(e) => ProjectExpr::Function {
            name: "LTRIM".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::RTrim(e) => ProjectExpr::Function {
            name: "RTRIM".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Concat(exprs) => ProjectExpr::Function {
            name: "CONCAT".to_string(),
            args: exprs.iter().map(select_expr_to_project_expr).collect(),
        },
        ScalarFunction::Coalesce(exprs) => ProjectExpr::Function {
            name: "COALESCE".to_string(),
            args: exprs.iter().map(select_expr_to_project_expr).collect(),
        },
        ScalarFunction::Reverse(e) => ProjectExpr::Function {
            name: "REVERSE".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Abs(e) => ProjectExpr::Function {
            name: "ABS".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Round { expr, precision } => ProjectExpr::Function {
            name: "ROUND".to_string(),
            args: vec![
                select_expr_to_project_expr(expr),
                ProjectExpr::Literal(StatValue::Int64(precision.unwrap_or(0) as i64)),
            ],
        },
        ScalarFunction::Ceil(e) => ProjectExpr::Function {
            name: "CEIL".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Floor(e) => ProjectExpr::Function {
            name: "FLOOR".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Sqrt(e) => ProjectExpr::Function {
            name: "SQRT".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Power { base, exponent } => ProjectExpr::Function {
            name: "POWER".to_string(),
            args: vec![
                select_expr_to_project_expr(base),
                select_expr_to_project_expr(exponent),
            ],
        },
        ScalarFunction::Log { expr, base } => {
            let mut args = vec![select_expr_to_project_expr(expr)];
            if let Some(b) = base {
                args.push(ProjectExpr::Literal(StatValue::Float64(*b)));
            }
            ProjectExpr::Function {
                name: "LOG".to_string(),
                args,
            }
        }
        ScalarFunction::Ln(e) => ProjectExpr::Function {
            name: "LN".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Exp(e) => ProjectExpr::Function {
            name: "EXP".to_string(),
            args: vec![select_expr_to_project_expr(e)],
        },
        ScalarFunction::Cast { expr, target_type } => ProjectExpr::Function {
            name: format!("CAST_TO_{}", target_type),
            args: vec![select_expr_to_project_expr(expr)],
        },
        _ => {
            // For any other scalar functions, return a generic placeholder
            ProjectExpr::Column(format!("{:?}", func))
        }
    }
}

// ============================================================================
// Physical Plan Executor
// ============================================================================

/// Executor for physical query plans
pub struct PhysicalPlanExecutor<'a> {
    db: &'a Db,
    stats: ExecutionStats,
}

#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_scanned: u64,
    pub rows_filtered: u64,
    pub rows_output: u64,
    pub segments_scanned: u64,
    pub bytes_scanned: u64,
}

impl<'a> PhysicalPlanExecutor<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self {
            db,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute a physical plan and return record batches
    pub fn execute(&mut self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>, EngineError> {
        match plan {
            PhysicalPlan::SeqScan {
                table,
                database,
                columns,
                filter,
                parallel_degree: _,
            } => self.execute_seq_scan(database, table, columns, filter.as_ref()),

            PhysicalPlan::IndexScan {
                table,
                database,
                columns,
                filter,
                ..
            } => self.execute_seq_scan(database, table, columns, filter.as_ref()),

            PhysicalPlan::Filter { input, predicate } => {
                let batches = self.execute(input)?;
                self.apply_filter(batches, predicate)
            }

            PhysicalPlan::Project { input, columns } => {
                let batches = self.execute(input)?;
                self.apply_projection(batches, columns)
            }

            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::SortedAggregate { input, .. } => {
                // Delegate to existing aggregation - simplified
                self.execute(input)
            }

            PhysicalPlan::HashJoin { build, probe, .. }
            | PhysicalPlan::MergeJoin {
                left: build,
                right: probe,
                ..
            } => {
                let build_batches = self.execute(build)?;
                let probe_batches = self.execute(probe)?;
                // Simplified: concatenate batches
                let mut result = build_batches;
                result.extend(probe_batches);
                Ok(result)
            }

            PhysicalPlan::NestedLoopJoin { outer, inner, .. } => {
                let outer_batches = self.execute(outer)?;
                let inner_batches = self.execute(inner)?;
                let mut result = outer_batches;
                result.extend(inner_batches);
                Ok(result)
            }

            PhysicalPlan::Sort {
                input,
                order_by,
                limit,
            } => {
                let batches = self.execute(input)?;
                self.apply_sort(batches, order_by, *limit)
            }

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
            } => {
                let batches = self.execute(input)?;
                self.apply_sort(batches, order_by, Some(*limit))
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let batches = self.execute(input)?;
                self.apply_limit(batches, *limit, *offset)
            }

            PhysicalPlan::Exchange { input, .. } => self.execute(input),

            PhysicalPlan::Gather { inputs } | PhysicalPlan::UnionAll { inputs } => {
                let mut all_batches = Vec::new();
                for input in inputs {
                    let batches = self.execute(input)?;
                    all_batches.extend(batches);
                }
                Ok(all_batches)
            }
        }
    }

    fn execute_seq_scan(
        &mut self,
        database: &str,
        table: &str,
        _columns: &[String],
        filter: Option<&Predicate>,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let batches = self.db.load_table_batches(database, table)?;
        self.stats.segments_scanned += batches.len() as u64;

        for batch in &batches {
            self.stats.rows_scanned += batch.num_rows() as u64;
        }

        if let Some(pred) = filter {
            self.apply_filter(batches, pred)
        } else {
            Ok(batches)
        }
    }

    fn apply_filter(
        &mut self,
        batches: Vec<RecordBatch>,
        predicate: &Predicate,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        use arrow_array::BooleanArray;

        let mut result = Vec::new();

        for batch in batches {
            let mask = self.evaluate_predicate(&batch, predicate)?;
            let filtered = arrow_select::filter::filter_record_batch(&batch, &mask)
                .map_err(|e| EngineError::Internal(format!("Filter error: {}", e)))?;

            self.stats.rows_filtered += (batch.num_rows() - filtered.num_rows()) as u64;

            if filtered.num_rows() > 0 {
                result.push(filtered);
            }
        }

        Ok(result)
    }

    fn evaluate_predicate(
        &self,
        batch: &RecordBatch,
        predicate: &Predicate,
    ) -> Result<arrow_array::BooleanArray, EngineError> {
        use arrow_array::BooleanArray;

        let num_rows = batch.num_rows();

        match predicate {
            Predicate::True => Ok(BooleanArray::from(vec![true; num_rows])),
            Predicate::False => Ok(BooleanArray::from(vec![false; num_rows])),
            Predicate::Comparison { column, op, value } => {
                self.evaluate_comparison(batch, column, op, value)
            }
            Predicate::And(preds) => {
                let mut result = BooleanArray::from(vec![true; num_rows]);
                for pred in preds {
                    let mask = self.evaluate_predicate(batch, pred)?;
                    result = arrow_boolean::and(&result, &mask)
                        .map_err(|e| EngineError::Internal(format!("AND error: {}", e)))?;
                }
                Ok(result)
            }
            Predicate::Or(preds) => {
                let mut result = BooleanArray::from(vec![false; num_rows]);
                for pred in preds {
                    let mask = self.evaluate_predicate(batch, pred)?;
                    result = arrow_boolean::or(&result, &mask)
                        .map_err(|e| EngineError::Internal(format!("OR error: {}", e)))?;
                }
                Ok(result)
            }
            Predicate::Not(pred) => {
                let mask = self.evaluate_predicate(batch, pred)?;
                arrow_boolean::not(&mask)
                    .map_err(|e| EngineError::Internal(format!("NOT error: {}", e)))
            }
            Predicate::IsNull { column, negated } => {
                if let Some(col) = batch.column_by_name(column) {
                    let is_null_mask = arrow_is_null(col.as_ref())
                        .map_err(|e| EngineError::Internal(format!("IS NULL error: {}", e)))?;
                    if *negated {
                        arrow_boolean::not(&is_null_mask)
                            .map_err(|e| EngineError::Internal(format!("IS NOT NULL error: {}", e)))
                    } else {
                        Ok(is_null_mask)
                    }
                } else {
                    Ok(BooleanArray::from(vec![false; num_rows]))
                }
            }
            Predicate::In { .. } | Predicate::Like { .. } | Predicate::Between { .. } => {
                // Simplified: return all true for complex predicates
                Ok(BooleanArray::from(vec![true; num_rows]))
            }
            Predicate::ColumnCompare { .. } => Ok(BooleanArray::from(vec![true; num_rows])),
        }
    }

    fn evaluate_comparison(
        &self,
        batch: &RecordBatch,
        column: &str,
        op: &CompareOp,
        value: &StatValue,
    ) -> Result<arrow_array::BooleanArray, EngineError> {
        use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow_ord::cmp;

        let num_rows = batch.num_rows();

        let col = match batch.column_by_name(column) {
            Some(c) => c,
            None => return Ok(BooleanArray::from(vec![false; num_rows])),
        };

        match value {
            StatValue::Int64(v) => {
                let scalar = Int64Array::new_scalar(*v);
                if let Some(col_i64) = col.as_any().downcast_ref::<Int64Array>() {
                    match op {
                        CompareOp::Eq => cmp::eq(col_i64, &scalar),
                        CompareOp::Ne => cmp::neq(col_i64, &scalar),
                        CompareOp::Lt => cmp::lt(col_i64, &scalar),
                        CompareOp::Le => cmp::lt_eq(col_i64, &scalar),
                        CompareOp::Gt => cmp::gt(col_i64, &scalar),
                        CompareOp::Ge => cmp::gt_eq(col_i64, &scalar),
                    }
                    .map_err(|e| EngineError::Internal(format!("Comparison error: {}", e)))
                } else {
                    Ok(BooleanArray::from(vec![true; num_rows]))
                }
            }
            StatValue::Float64(v) => {
                let scalar = Float64Array::new_scalar(*v);
                if let Some(col_f64) = col.as_any().downcast_ref::<Float64Array>() {
                    match op {
                        CompareOp::Eq => cmp::eq(col_f64, &scalar),
                        CompareOp::Ne => cmp::neq(col_f64, &scalar),
                        CompareOp::Lt => cmp::lt(col_f64, &scalar),
                        CompareOp::Le => cmp::lt_eq(col_f64, &scalar),
                        CompareOp::Gt => cmp::gt(col_f64, &scalar),
                        CompareOp::Ge => cmp::gt_eq(col_f64, &scalar),
                    }
                    .map_err(|e| EngineError::Internal(format!("Comparison error: {}", e)))
                } else {
                    Ok(BooleanArray::from(vec![true; num_rows]))
                }
            }
            StatValue::String(v) => {
                let scalar = StringArray::new_scalar(v);
                if let Some(col_str) = col.as_any().downcast_ref::<StringArray>() {
                    match op {
                        CompareOp::Eq => cmp::eq(col_str, &scalar),
                        CompareOp::Ne => cmp::neq(col_str, &scalar),
                        CompareOp::Lt => cmp::lt(col_str, &scalar),
                        CompareOp::Le => cmp::lt_eq(col_str, &scalar),
                        CompareOp::Gt => cmp::gt(col_str, &scalar),
                        CompareOp::Ge => cmp::gt_eq(col_str, &scalar),
                    }
                    .map_err(|e| EngineError::Internal(format!("Comparison error: {}", e)))
                } else {
                    Ok(BooleanArray::from(vec![true; num_rows]))
                }
            }
            _ => Ok(BooleanArray::from(vec![true; num_rows])),
        }
    }

    fn apply_projection(
        &mut self,
        batches: Vec<RecordBatch>,
        columns: &[ProjectExpr],
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let col_names: Vec<String> = columns
            .iter()
            .filter_map(|expr| match expr {
                ProjectExpr::Column(name) => Some(name.clone()),
                ProjectExpr::Alias { alias, .. } => Some(alias.clone()),
                _ => None,
            })
            .collect();

        if col_names.is_empty() || col_names.contains(&"*".to_string()) {
            return Ok(batches);
        }

        let mut result = Vec::new();
        for batch in batches {
            let projected = self.project_batch(&batch, &col_names)?;
            result.push(projected);
        }
        Ok(result)
    }

    fn project_batch(
        &self,
        batch: &RecordBatch,
        columns: &[String],
    ) -> Result<RecordBatch, EngineError> {
        let schema = batch.schema();
        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        for col_name in columns {
            if let Ok(idx) = schema.index_of(col_name) {
                arrays.push(batch.column(idx).clone());
                fields.push(schema.field(idx).clone());
            }
        }

        if arrays.is_empty() {
            return Ok(batch.clone());
        }

        let new_schema = Arc::new(arrow_schema::Schema::new(fields));
        RecordBatch::try_new(new_schema, arrays)
            .map_err(|e| EngineError::Internal(format!("Projection error: {}", e)))
    }

    fn apply_sort(
        &mut self,
        batches: Vec<RecordBatch>,
        order_by: &[SortExpr],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        if batches.is_empty() || order_by.is_empty() {
            return Ok(batches);
        }

        let schema = batches[0].schema();
        let combined = arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| EngineError::Internal(format!("Concat error: {}", e)))?;

        let mut sort_columns = Vec::new();
        for expr in order_by {
            if let Some(col) = combined.column_by_name(&expr.column) {
                sort_columns.push(arrow_ord::sort::SortColumn {
                    values: col.clone(),
                    options: Some(arrow_ord::sort::SortOptions {
                        descending: expr.descending,
                        nulls_first: expr.nulls_first,
                    }),
                });
            }
        }

        if sort_columns.is_empty() {
            return Ok(vec![combined]);
        }

        let indices = arrow_ord::sort::lexsort_to_indices(&sort_columns, limit)
            .map_err(|e| EngineError::Internal(format!("Sort error: {}", e)))?;

        let sorted = arrow_select::take::take_record_batch(&combined, &indices)
            .map_err(|e| EngineError::Internal(format!("Take error: {}", e)))?;

        Ok(vec![sorted])
    }

    fn apply_limit(
        &mut self,
        batches: Vec<RecordBatch>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        if batches.is_empty() {
            return Ok(batches);
        }

        let schema = batches[0].schema();
        let combined = arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| EngineError::Internal(format!("Concat error: {}", e)))?;

        let total_rows = combined.num_rows();
        let start = offset.min(total_rows);
        let end = (offset + limit).min(total_rows);

        if start >= end {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let sliced = combined.slice(start, end - start);
        self.stats.rows_output = sliced.num_rows() as u64;
        Ok(vec![sliced])
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

// ============================================================================
// Optimizer Integration with Db
// ============================================================================

/// Query optimization context - stores optimizer state and table statistics
pub struct OptimizationContext {
    pub optimizer: CostBasedOptimizer,
    pub enabled: bool,
}

impl std::fmt::Debug for OptimizationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizationContext")
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

impl Default for OptimizationContext {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        Self {
            optimizer: CostBasedOptimizer::new(parallelism),
            enabled: true,
        }
    }
}

impl OptimizationContext {
    pub fn new(parallelism: usize) -> Self {
        Self {
            optimizer: CostBasedOptimizer::new(parallelism),
            enabled: true,
        }
    }

    /// Update table statistics for better optimization
    pub fn update_table_stats(&mut self, database: &str, table: &str, stats: TableStats) {
        let key = format!("{}.{}", database, table);
        self.optimizer.update_stats(&key, stats);
    }

    /// Add index information for index selection
    pub fn add_index(&mut self, database: &str, table: &str, index: IndexInfo) {
        let key = format!("{}.{}", database, table);
        self.optimizer.add_index(&key, index);
    }

    /// Optimize a parsed query and return a physical plan
    pub fn optimize_query(&self, query: &ParsedQuery) -> Result<PhysicalPlan, EngineError> {
        if !self.enabled {
            // Return a simple sequential scan plan
            let database = query.database.clone().unwrap_or_else(|| "default".to_string());
            let table = query
                .table
                .clone()
                .ok_or_else(|| EngineError::InvalidArgument("Query must specify a table".into()))?;

            return Ok(PhysicalPlan::SeqScan {
                table,
                database,
                columns: query.projection.clone().unwrap_or_default(),
                filter: None,
                parallel_degree: 1,
            });
        }

        let logical_plan = parsed_query_to_logical_plan(query)?;
        let physical_plan = self.optimizer.optimize(logical_plan);
        Ok(physical_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_scan_conversion() {
        let query = ParsedQuery {
            database: Some("test".to_string()),
            table: Some("users".to_string()),
            projection: Some(vec!["id".to_string(), "name".to_string()]),
            filter: QueryFilter::default(),
            aggregation: None,
            order_by: None,
            distinct: false,
            joins: vec![],
            computed_columns: vec![],
            ctes: vec![],
            sample: None,
        };

        let plan = parsed_query_to_logical_plan(&query).unwrap();

        match plan {
            LogicalPlan::Project { input, columns } => {
                assert_eq!(columns.len(), 2);
                match *input {
                    LogicalPlan::Scan { database, table, .. } => {
                        assert_eq!(database, "test");
                        assert_eq!(table, "users");
                    }
                    _ => panic!("Expected Scan"),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_query_with_filter() {
        let mut filter = QueryFilter::default();
        filter.watermark_ge = Some(1000);
        filter.watermark_le = Some(2000);

        let query = ParsedQuery {
            database: Some("test".to_string()),
            table: Some("events".to_string()),
            projection: None,
            filter,
            aggregation: None,
            order_by: None,
            distinct: false,
            joins: vec![],
            computed_columns: vec![],
            ctes: vec![],
            sample: None,
        };

        let plan = parsed_query_to_logical_plan(&query).unwrap();

        match plan {
            LogicalPlan::Filter { predicate, .. } => match predicate {
                Predicate::And(preds) => {
                    assert_eq!(preds.len(), 2);
                }
                _ => panic!("Expected AND predicate"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn test_optimization_context() {
        let ctx = OptimizationContext::default();
        assert!(ctx.enabled);

        let query = ParsedQuery {
            database: Some("test".to_string()),
            table: Some("users".to_string()),
            projection: Some(vec!["id".to_string()]),
            filter: QueryFilter::default(),
            aggregation: None,
            order_by: None,
            distinct: false,
            joins: vec![],
            computed_columns: vec![],
            ctes: vec![],
            sample: None,
        };

        let plan = ctx.optimize_query(&query).unwrap();
        // Should produce a physical plan
        assert!(matches!(plan, PhysicalPlan::SeqScan { .. } | PhysicalPlan::Project { .. }));
    }
}
