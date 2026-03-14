//! Natural Language to SQL for BoyoDB
//!
//! Convert natural language queries to SQL:
//! - Intent recognition
//! - Entity extraction
//! - Schema-aware SQL generation
//! - Query disambiguation

use std::collections::HashMap;
use std::sync::RwLock;

/// Table schema for NL understanding
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Table description
    pub description: String,
    /// Columns
    pub columns: Vec<ColumnSchema>,
    /// Synonyms for table name
    pub synonyms: Vec<String>,
}

/// Column schema
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: String,
    /// Description
    pub description: String,
    /// Synonyms
    pub synonyms: Vec<String>,
    /// Sample values
    pub sample_values: Vec<String>,
    /// Is primary key
    pub is_primary_key: bool,
    /// Foreign key reference
    pub foreign_key: Option<(String, String)>,
}

/// Query intent
#[derive(Debug, Clone, PartialEq)]
pub enum QueryIntent {
    /// SELECT query
    Select,
    /// Aggregation query (COUNT, SUM, etc.)
    Aggregate,
    /// Comparison query
    Compare,
    /// Time-based query
    TimeSeries,
    /// Top-N query
    TopN,
    /// Trend analysis
    Trend,
    /// Join query
    Join,
    /// Unknown intent
    Unknown,
}

/// Extracted entity
#[derive(Debug, Clone)]
pub struct Entity {
    /// Entity type (table, column, value, etc.)
    pub entity_type: EntityType,
    /// Entity value
    pub value: String,
    /// Resolved reference (actual table/column name)
    pub resolved: Option<String>,
    /// Confidence score
    pub confidence: f64,
    /// Position in original query
    pub position: (usize, usize),
}

/// Entity types
#[derive(Debug, Clone, PartialEq)]
pub enum EntityType {
    Table,
    Column,
    Value,
    Aggregation,
    Comparison,
    TimeExpression,
    Number,
    OrderDirection,
    Limit,
}

/// Aggregation type
#[derive(Debug, Clone, Copy)]
pub enum AggregationType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

impl AggregationType {
    pub fn to_sql(&self) -> &'static str {
        match self {
            AggregationType::Count => "COUNT",
            AggregationType::Sum => "SUM",
            AggregationType::Avg => "AVG",
            AggregationType::Min => "MIN",
            AggregationType::Max => "MAX",
            AggregationType::CountDistinct => "COUNT(DISTINCT",
        }
    }
}

/// Comparison operator
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    Like,
    In,
    Between,
}

impl ComparisonOp {
    pub fn to_sql(&self) -> &'static str {
        match self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThan => "<",
            ComparisonOp::GreaterOrEqual => ">=",
            ComparisonOp::LessOrEqual => "<=",
            ComparisonOp::Like => "LIKE",
            ComparisonOp::In => "IN",
            ComparisonOp::Between => "BETWEEN",
        }
    }
}

/// Parsed NL query
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Original query
    pub original: String,
    /// Detected intent
    pub intent: QueryIntent,
    /// Extracted entities
    pub entities: Vec<Entity>,
    /// Target table
    pub table: Option<String>,
    /// Selected columns
    pub columns: Vec<String>,
    /// Aggregations
    pub aggregations: Vec<(AggregationType, String)>,
    /// Filter conditions
    pub conditions: Vec<Condition>,
    /// Group by columns
    pub group_by: Vec<String>,
    /// Order by
    pub order_by: Option<(String, bool)>,
    /// Limit
    pub limit: Option<usize>,
    /// Confidence score
    pub confidence: f64,
}

/// Filter condition
#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: ComparisonOp,
    pub value: String,
}

/// Generated SQL result
#[derive(Debug, Clone)]
pub struct GeneratedSQL {
    /// Generated SQL query
    pub sql: String,
    /// Confidence score (0-1)
    pub confidence: f64,
    /// Explanation of the query
    pub explanation: String,
    /// Alternative interpretations
    pub alternatives: Vec<(String, f64)>,
    /// Warnings or suggestions
    pub warnings: Vec<String>,
}

/// Keyword patterns for intent detection
struct IntentPatterns {
    select_keywords: Vec<&'static str>,
    aggregate_keywords: Vec<&'static str>,
    compare_keywords: Vec<&'static str>,
    time_keywords: Vec<&'static str>,
    top_keywords: Vec<&'static str>,
}

impl Default for IntentPatterns {
    fn default() -> Self {
        Self {
            select_keywords: vec![
                "show", "list", "get", "find", "display", "what", "which",
                "give me", "fetch", "retrieve", "select",
            ],
            aggregate_keywords: vec![
                "count", "how many", "total", "sum", "average", "avg",
                "maximum", "max", "minimum", "min", "number of",
            ],
            compare_keywords: vec![
                "compare", "versus", "vs", "difference", "between",
            ],
            time_keywords: vec![
                "yesterday", "today", "last week", "last month", "last year",
                "this week", "this month", "this year", "since", "before", "after",
            ],
            top_keywords: vec![
                "top", "best", "highest", "lowest", "most", "least", "first", "last",
            ],
        }
    }
}

/// NL to SQL converter
pub struct NLToSQL {
    /// Schema registry
    schemas: RwLock<HashMap<String, TableSchema>>,
    /// Intent patterns
    patterns: IntentPatterns,
    /// Word to column mapping cache
    word_mappings: RwLock<HashMap<String, (String, String)>>,
}

impl NLToSQL {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            patterns: IntentPatterns::default(),
            word_mappings: RwLock::new(HashMap::new()),
        }
    }

    /// Register a table schema
    pub fn register_schema(&self, schema: TableSchema) {
        let mut mappings = self.word_mappings.write().unwrap();

        // Add table synonyms
        for synonym in &schema.synonyms {
            mappings.insert(
                synonym.to_lowercase(),
                (schema.name.clone(), String::new()),
            );
        }
        mappings.insert(
            schema.name.to_lowercase(),
            (schema.name.clone(), String::new()),
        );

        // Add column synonyms
        for col in &schema.columns {
            for synonym in &col.synonyms {
                mappings.insert(
                    synonym.to_lowercase(),
                    (schema.name.clone(), col.name.clone()),
                );
            }
            mappings.insert(
                col.name.to_lowercase(),
                (schema.name.clone(), col.name.clone()),
            );
        }

        self.schemas.write().unwrap().insert(schema.name.clone(), schema);
    }

    /// Convert natural language to SQL
    pub fn convert(&self, query: &str) -> GeneratedSQL {
        let parsed = self.parse(query);
        self.generate_sql(&parsed)
    }

    /// Parse natural language query
    pub fn parse(&self, query: &str) -> ParsedQuery {
        let lower = query.to_lowercase();
        let words: Vec<&str> = lower.split_whitespace().collect();

        // Detect intent
        let intent = self.detect_intent(&lower);

        // Extract entities
        let entities = self.extract_entities(&lower, &words);

        // Resolve table and columns
        let (table, columns) = self.resolve_table_columns(&entities);

        // Extract aggregations
        let aggregations = self.extract_aggregations(&lower, &columns);

        // Extract conditions
        let conditions = self.extract_conditions(&lower, &words, &table);

        // Extract group by
        let group_by = self.extract_group_by(&lower, &columns);

        // Extract order by
        let order_by = self.extract_order_by(&lower, &columns);

        // Extract limit
        let limit = self.extract_limit(&lower);

        // Calculate confidence
        let confidence = self.calculate_confidence(&intent, &entities, &table);

        ParsedQuery {
            original: query.to_string(),
            intent,
            entities,
            table,
            columns,
            aggregations,
            conditions,
            group_by,
            order_by,
            limit,
            confidence,
        }
    }

    /// Detect query intent
    fn detect_intent(&self, query: &str) -> QueryIntent {
        // Check for aggregation keywords first (more specific)
        for keyword in &self.patterns.aggregate_keywords {
            if query.contains(keyword) {
                return QueryIntent::Aggregate;
            }
        }

        // Check for top-N patterns
        for keyword in &self.patterns.top_keywords {
            if query.contains(keyword) {
                return QueryIntent::TopN;
            }
        }

        // Check for comparison
        for keyword in &self.patterns.compare_keywords {
            if query.contains(keyword) {
                return QueryIntent::Compare;
            }
        }

        // Check for time-based
        for keyword in &self.patterns.time_keywords {
            if query.contains(keyword) {
                return QueryIntent::TimeSeries;
            }
        }

        // Default to select
        for keyword in &self.patterns.select_keywords {
            if query.contains(keyword) {
                return QueryIntent::Select;
            }
        }

        QueryIntent::Unknown
    }

    /// Extract entities from query
    fn extract_entities(&self, query: &str, words: &[&str]) -> Vec<Entity> {
        let mut entities = Vec::new();
        let mappings = self.word_mappings.read().unwrap();

        // Look for known tables and columns
        for (i, word) in words.iter().enumerate() {
            let clean_word = word.trim_matches(|c: char| !c.is_alphanumeric());

            if let Some((table, column)) = mappings.get(clean_word) {
                let entity_type = if column.is_empty() {
                    EntityType::Table
                } else {
                    EntityType::Column
                };

                entities.push(Entity {
                    entity_type,
                    value: clean_word.to_string(),
                    resolved: Some(if column.is_empty() {
                        table.clone()
                    } else {
                        column.clone()
                    }),
                    confidence: 0.9,
                    position: (i, i + 1),
                });
            }

            // Look for numbers
            if let Ok(num) = clean_word.parse::<i64>() {
                entities.push(Entity {
                    entity_type: EntityType::Number,
                    value: num.to_string(),
                    resolved: None,
                    confidence: 1.0,
                    position: (i, i + 1),
                });
            }

            // Look for aggregation keywords
            let agg_keywords = ["count", "sum", "average", "avg", "max", "min", "total"];
            if agg_keywords.contains(&clean_word) {
                entities.push(Entity {
                    entity_type: EntityType::Aggregation,
                    value: clean_word.to_string(),
                    resolved: None,
                    confidence: 0.95,
                    position: (i, i + 1),
                });
            }

            // Look for comparison keywords
            if ["greater", "more", "less", "equal", "above", "below", "over", "under"]
                .contains(&clean_word)
            {
                entities.push(Entity {
                    entity_type: EntityType::Comparison,
                    value: clean_word.to_string(),
                    resolved: None,
                    confidence: 0.85,
                    position: (i, i + 1),
                });
            }

            // Look for order direction
            if ["ascending", "descending", "asc", "desc", "highest", "lowest"]
                .contains(&clean_word)
            {
                entities.push(Entity {
                    entity_type: EntityType::OrderDirection,
                    value: clean_word.to_string(),
                    resolved: None,
                    confidence: 0.95,
                    position: (i, i + 1),
                });
            }
        }

        // Look for quoted values
        let mut in_quote = false;
        let mut quote_start = 0;
        let mut quote_content = String::new();

        for (i, c) in query.char_indices() {
            if c == '"' || c == '\'' {
                if in_quote {
                    entities.push(Entity {
                        entity_type: EntityType::Value,
                        value: quote_content.clone(),
                        resolved: None,
                        confidence: 1.0,
                        position: (quote_start, i),
                    });
                    quote_content.clear();
                    in_quote = false;
                } else {
                    in_quote = true;
                    quote_start = i;
                }
            } else if in_quote {
                quote_content.push(c);
            }
        }

        entities
    }

    /// Resolve table and columns from entities
    fn resolve_table_columns(&self, entities: &[Entity]) -> (Option<String>, Vec<String>) {
        let mut table = None;
        let mut columns = Vec::new();

        for entity in entities {
            match entity.entity_type {
                EntityType::Table => {
                    if table.is_none() {
                        table = entity.resolved.clone();
                    }
                }
                EntityType::Column => {
                    if let Some(ref col) = entity.resolved {
                        columns.push(col.clone());
                    }
                }
                _ => {}
            }
        }

        // If no table found, try to infer from schema
        if table.is_none() && !columns.is_empty() {
            let mappings = self.word_mappings.read().unwrap();
            for col in &columns {
                if let Some((tbl, _)) = mappings.get(&col.to_lowercase()) {
                    table = Some(tbl.clone());
                    break;
                }
            }
        }

        (table, columns)
    }

    /// Extract aggregations
    fn extract_aggregations(&self, query: &str, columns: &[String]) -> Vec<(AggregationType, String)> {
        let mut aggregations = Vec::new();

        if query.contains("count") || query.contains("how many") || query.contains("number of") {
            let col = columns.first().map(|c| c.as_str()).unwrap_or("*");
            aggregations.push((AggregationType::Count, col.to_string()));
        }

        if query.contains("sum") || query.contains("total") {
            if let Some(col) = columns.first() {
                aggregations.push((AggregationType::Sum, col.clone()));
            }
        }

        if query.contains("average") || query.contains("avg") || query.contains("mean") {
            if let Some(col) = columns.first() {
                aggregations.push((AggregationType::Avg, col.clone()));
            }
        }

        if query.contains("maximum") || query.contains("max") || query.contains("highest") {
            if let Some(col) = columns.first() {
                aggregations.push((AggregationType::Max, col.clone()));
            }
        }

        if query.contains("minimum") || query.contains("min") || query.contains("lowest") {
            if let Some(col) = columns.first() {
                aggregations.push((AggregationType::Min, col.clone()));
            }
        }

        aggregations
    }

    /// Extract filter conditions
    fn extract_conditions(&self, query: &str, words: &[&str], table: &Option<String>) -> Vec<Condition> {
        let mut conditions = Vec::new();

        // Look for comparison patterns
        let comparison_patterns = [
            ("greater than", ComparisonOp::GreaterThan),
            ("more than", ComparisonOp::GreaterThan),
            ("above", ComparisonOp::GreaterThan),
            ("over", ComparisonOp::GreaterThan),
            ("less than", ComparisonOp::LessThan),
            ("below", ComparisonOp::LessThan),
            ("under", ComparisonOp::LessThan),
            ("equal to", ComparisonOp::Equal),
            ("equals", ComparisonOp::Equal),
            ("is", ComparisonOp::Equal),
            ("like", ComparisonOp::Like),
            ("contains", ComparisonOp::Like),
        ];

        for (pattern, op) in comparison_patterns {
            if let Some(pos) = query.find(pattern) {
                // Look for column before pattern and value after
                let before = &query[..pos];
                let after = &query[pos + pattern.len()..];

                // Try to find a column name
                let mappings = self.word_mappings.read().unwrap();
                let mut column = None;

                for word in before.split_whitespace().rev() {
                    let clean = word.trim_matches(|c: char| !c.is_alphanumeric());
                    if mappings.contains_key(clean) {
                        column = Some(clean.to_string());
                        break;
                    }
                }

                // Try to find a value after
                let value: String = after
                    .split_whitespace()
                    .take(1)
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim_matches(|c: char| !c.is_alphanumeric())
                    .to_string();

                if let Some(col) = column {
                    if !value.is_empty() {
                        conditions.push(Condition {
                            column: col,
                            operator: op,
                            value,
                        });
                    }
                }
            }
        }

        // Look for "where X = Y" patterns
        if query.contains("where") {
            let after_where = query.split("where").nth(1).unwrap_or("");
            let parts: Vec<&str> = after_where.split_whitespace().collect();

            if parts.len() >= 3 {
                let col = parts[0].trim_matches(|c: char| !c.is_alphanumeric());
                let val = parts[2].trim_matches(|c: char| !c.is_alphanumeric());

                conditions.push(Condition {
                    column: col.to_string(),
                    operator: ComparisonOp::Equal,
                    value: val.to_string(),
                });
            }
        }

        conditions
    }

    /// Extract group by columns
    fn extract_group_by(&self, query: &str, columns: &[String]) -> Vec<String> {
        let mut group_by = Vec::new();

        if query.contains("by") || query.contains("per") || query.contains("for each") {
            // Look for column names after "by", "per", or "for each"
            let patterns = ["by", "per", "for each"];

            for pattern in patterns {
                if let Some(pos) = query.find(pattern) {
                    let after = &query[pos + pattern.len()..];
                    let mappings = self.word_mappings.read().unwrap();

                    for word in after.split_whitespace().take(3) {
                        let clean = word.trim_matches(|c: char| !c.is_alphanumeric());
                        if let Some((_, col)) = mappings.get(clean) {
                            if !col.is_empty() {
                                group_by.push(col.clone());
                            }
                        }
                    }
                }
            }
        }

        group_by
    }

    /// Extract order by
    fn extract_order_by(&self, query: &str, columns: &[String]) -> Option<(String, bool)> {
        let descending = query.contains("descending")
            || query.contains("desc")
            || query.contains("highest")
            || query.contains("most")
            || query.contains("top");

        let ascending = query.contains("ascending")
            || query.contains("asc")
            || query.contains("lowest")
            || query.contains("least")
            || query.contains("bottom");

        // Look for "order by" or "sort by"
        let order_patterns = ["order by", "sort by", "sorted by", "ordered by"];

        for pattern in order_patterns {
            if let Some(pos) = query.find(pattern) {
                let after = &query[pos + pattern.len()..];
                let mappings = self.word_mappings.read().unwrap();

                for word in after.split_whitespace().take(2) {
                    let clean = word.trim_matches(|c: char| !c.is_alphanumeric());
                    if let Some((_, col)) = mappings.get(clean) {
                        if !col.is_empty() {
                            return Some((col.clone(), descending));
                        }
                    }
                }
            }
        }

        // If top/bottom pattern, use first column
        if descending || ascending {
            if let Some(col) = columns.first() {
                return Some((col.clone(), descending));
            }
        }

        None
    }

    /// Extract limit
    fn extract_limit(&self, query: &str) -> Option<usize> {
        // Look for "top N", "first N", "N results"
        let limit_patterns = [
            ("top ", true),
            ("first ", true),
            ("limit ", true),
            (" results", false),
            (" rows", false),
            (" records", false),
        ];

        for (pattern, before) in limit_patterns {
            if let Some(pos) = query.find(pattern) {
                let search_area = if before {
                    &query[pos + pattern.len()..]
                } else {
                    &query[..pos]
                };

                for word in search_area.split_whitespace() {
                    if let Ok(num) = word.parse::<usize>() {
                        return Some(num);
                    }
                }
            }
        }

        None
    }

    /// Calculate confidence score
    fn calculate_confidence(
        &self,
        intent: &QueryIntent,
        entities: &[Entity],
        table: &Option<String>,
    ) -> f64 {
        let mut confidence = 0.5;

        // Intent confidence
        if *intent != QueryIntent::Unknown {
            confidence += 0.2;
        }

        // Table found
        if table.is_some() {
            confidence += 0.2;
        }

        // Entity confidence
        if !entities.is_empty() {
            let avg_entity_conf: f64 = entities.iter().map(|e| e.confidence).sum::<f64>()
                / entities.len() as f64;
            confidence += avg_entity_conf * 0.1;
        }

        confidence.min(1.0)
    }

    /// Generate SQL from parsed query
    fn generate_sql(&self, parsed: &ParsedQuery) -> GeneratedSQL {
        let mut sql = String::new();
        let mut explanation = String::new();
        let mut warnings = Vec::new();

        // SELECT clause
        sql.push_str("SELECT ");

        if !parsed.aggregations.is_empty() {
            let agg_strs: Vec<String> = parsed.aggregations
                .iter()
                .map(|(agg, col)| {
                    if matches!(agg, AggregationType::CountDistinct) {
                        format!("{}({}))", agg.to_sql(), col)
                    } else {
                        format!("{}({})", agg.to_sql(), col)
                    }
                })
                .collect();
            sql.push_str(&agg_strs.join(", "));
            explanation.push_str(&format!("Calculating {} ", agg_strs.join(" and ")));
        } else if parsed.columns.is_empty() {
            sql.push('*');
            explanation.push_str("Selecting all columns ");
        } else {
            sql.push_str(&parsed.columns.join(", "));
            explanation.push_str(&format!("Selecting {} ", parsed.columns.join(", ")));
        }

        // FROM clause
        if let Some(ref table) = parsed.table {
            sql.push_str(&format!(" FROM {}", table));
            explanation.push_str(&format!("from {} ", table));
        } else {
            warnings.push("No table specified - query may be incomplete".to_string());
        }

        // WHERE clause
        if !parsed.conditions.is_empty() {
            sql.push_str(" WHERE ");
            let cond_strs: Vec<String> = parsed.conditions
                .iter()
                .map(|c| {
                    let val = if c.value.parse::<f64>().is_ok() {
                        c.value.clone()
                    } else {
                        format!("'{}'", c.value)
                    };
                    format!("{} {} {}", c.column, c.operator.to_sql(), val)
                })
                .collect();
            sql.push_str(&cond_strs.join(" AND "));
            explanation.push_str(&format!("where {} ", cond_strs.join(" and ")));
        }

        // GROUP BY clause
        if !parsed.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", parsed.group_by.join(", ")));
            explanation.push_str(&format!("grouped by {} ", parsed.group_by.join(", ")));
        }

        // ORDER BY clause
        if let Some((ref col, desc)) = parsed.order_by {
            let direction = if desc { "DESC" } else { "ASC" };
            sql.push_str(&format!(" ORDER BY {} {}", col, direction));
            explanation.push_str(&format!(
                "ordered by {} {} ",
                col,
                if desc { "descending" } else { "ascending" }
            ));
        }

        // LIMIT clause
        if let Some(limit) = parsed.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
            explanation.push_str(&format!("limited to {} results", limit));
        }

        GeneratedSQL {
            sql,
            confidence: parsed.confidence,
            explanation: explanation.trim().to_string(),
            alternatives: vec![],
            warnings,
        }
    }
}

impl Default for NLToSQL {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_converter() -> NLToSQL {
        let converter = NLToSQL::new();

        converter.register_schema(TableSchema {
            name: "orders".to_string(),
            description: "Customer orders".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    description: "Order ID".to_string(),
                    synonyms: vec!["order_id".to_string()],
                    sample_values: vec![],
                    is_primary_key: true,
                    foreign_key: None,
                },
                ColumnSchema {
                    name: "customer_name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    description: "Customer name".to_string(),
                    synonyms: vec!["customer".to_string(), "name".to_string()],
                    sample_values: vec![],
                    is_primary_key: false,
                    foreign_key: None,
                },
                ColumnSchema {
                    name: "amount".to_string(),
                    data_type: "DECIMAL".to_string(),
                    description: "Order amount".to_string(),
                    synonyms: vec!["total".to_string(), "price".to_string()],
                    sample_values: vec![],
                    is_primary_key: false,
                    foreign_key: None,
                },
                ColumnSchema {
                    name: "status".to_string(),
                    data_type: "VARCHAR".to_string(),
                    description: "Order status".to_string(),
                    synonyms: vec![],
                    sample_values: vec!["pending".to_string(), "shipped".to_string()],
                    is_primary_key: false,
                    foreign_key: None,
                },
            ],
            synonyms: vec!["order".to_string(), "purchases".to_string()],
        });

        converter
    }

    #[test]
    fn test_simple_select() {
        let converter = setup_converter();
        let result = converter.convert("show all orders");

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("FROM orders"));
    }

    #[test]
    fn test_count_query() {
        let converter = setup_converter();
        let result = converter.convert("how many orders are there");

        assert!(result.sql.contains("COUNT"));
        assert!(result.sql.contains("FROM orders"));
    }

    #[test]
    fn test_top_n_query() {
        let converter = setup_converter();
        let result = converter.convert("top 10 orders by amount");

        assert!(result.sql.contains("LIMIT 10"));
        assert!(result.sql.contains("ORDER BY"));
    }

    #[test]
    fn test_condition_extraction() {
        let converter = setup_converter();
        let result = converter.convert("orders where status is pending");

        assert!(result.sql.contains("WHERE"));
    }

    #[test]
    fn test_intent_detection() {
        let converter = setup_converter();

        let parsed = converter.parse("count all orders");
        assert_eq!(parsed.intent, QueryIntent::Aggregate);

        let parsed = converter.parse("show me the top 5 orders");
        assert_eq!(parsed.intent, QueryIntent::TopN);
    }

    #[test]
    fn test_aggregation_with_group_by() {
        let converter = setup_converter();
        let result = converter.convert("total amount by customer");

        assert!(result.sql.contains("SUM"));
        assert!(result.sql.contains("GROUP BY"));
    }
}
