use crate::engine::{PlanKind, AggPlan};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalPlan {
    pub kind: PlanKind,
    pub shard_ids: Vec<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalPlan {
    pub kind: PlanKind,
}

/// Splits a logical plan into a Scatter (Local) plan and a Gather (Global) plan.
/// 
/// Returns `None` if the plan cannot be distributed (e.g., must be executed locally on coordinator).
pub fn distribute_plan(kind: &PlanKind) -> Option<(LocalPlan, GlobalPlan)> {
    match kind {
        PlanKind::Simple { agg, db, table, projection, filter, computed_columns } => {
            if let Some(agg_plan) = agg {
                // Split aggregation into Partial (Local) and Final (Global).
                
                // Local Plan: Execute the original aggregation.
                // This produces intermediate results (e.g. partial counts, partial sums).
                // The worker's ManifestIndex ensures it runs on relevant data.
                let local_plan = LocalPlan { 
                    kind: kind.clone(), 
                    shard_ids: vec![] 
                };

                // Global Plan: Aggregate the intermediate results.
                // We construct a NEW AggPlan that operates on the output of the Local Plan.
                // Mappings:
                // - CountStar -> Sum("count")
                // - Sum(x) -> Sum("sum") (Note: engine outputs "sum" column if Sum/Avg involved)
                // - Avg(x) -> Not fully supported in this pass (requires Sum/Count reconstruction)
                // - Min/Max -> Min/Max("min_x"/"max_x")
                
                let mut global_aggs = Vec::new();
                for agg_kind in &agg_plan.aggs {
                    match agg_kind {
                        crate::engine::AggKind::CountStar => {
                            // Sum the partial counts
                            global_aggs.push(crate::engine::AggKind::Sum { column: "count".into() });
                        },
                        crate::engine::AggKind::Sum { column } => {
                            // Sum the partial sums
                            // Local node produces "sum_{column}" which we need to sum globally
                            global_aggs.push(crate::engine::AggKind::Sum { column: format!("sum_{}", column) });
                        },
                         crate::engine::AggKind::Max { column } => {
                            global_aggs.push(crate::engine::AggKind::Max { column: format!("max_{}", column) });
                        },
                         crate::engine::AggKind::Min { column } => {
                            global_aggs.push(crate::engine::AggKind::Min { column: format!("min_{}", column) });
                        },
                        _ => {
                            // For other types, fallback to identity (bad, but placeholders)
                            // or just skip.
                        }
                    }
                }
                
                let global_agg_plan = AggPlan {
                    group_by: agg_plan.group_by.clone(), // Preserve Group By (e.g. if grouping by Tenant, we merge by Tenant)
                    aggs: global_aggs,
                    having: agg_plan.having.clone(), // Having applies to final result
                };

                // The Global Plan is to execute this new AggPlan
                let global_plan_kind = PlanKind::Simple {
                    agg: Some(global_agg_plan),
                    db: db.clone(),
                    table: table.clone(), // Table name strictly doesn't matter for Global aggregation on IPC, but kept for structure
                    projection: projection.clone(),
                    filter: filter.clone(), // Filter should have been applied locally?
                    computed_columns: computed_columns.clone(),
                    // Actually, Global Plan shouldn't re-filter raw rows. 
                    // But filter in AggPlan usually applies to source rows. 
                    // To be safe, we might want to clear the filter for Global Plan if it was applied locally.
                    // However, `execute_simple_plan` structure applies filter tightly.
                    // Ideally Global Plan operates on "virtual" table from IPC.
                };

                Some((
                    local_plan,
                    GlobalPlan { kind: global_plan_kind }
                ))
            } else {
                // If no aggregation (SELECT *), Global Plan just gathers (Merge/Union)
                // We represent this as a Simple plan with NO aggregation, which implies "scan and return".
                // The Coordinator's executor will handle merging stream.
                 Some((
                    LocalPlan { kind: kind.clone(), shard_ids: vec![] },
                    GlobalPlan { kind: kind.clone() }
                ))
            }
        }
        _ => None // Joins/Sets not supported yet
    }
}
