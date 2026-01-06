use boyodb_core::engine::{Db, QueryRequest};
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, FieldFormat, DescribeStatementResponse, DescribePortalResponse, Tag};
use pgwire::api::{ClientInfo, Type, MakeHandler};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireFrontendMessage;
use pgwire::messages::extendedquery::{Bind, Parse, ParseComplete, BindComplete, Close, Sync as PgSync, Execute};
use std::sync::Arc;
use futures::stream;
use pgwire::api::portal::Portal;
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::ClientPortalStore;
use pgwire::api::store::PortalStore;
use pgwire::api::stmt::StoredStatement;
use std::fmt::Debug;
use futures::{Sink, SinkExt};
use pgwire::messages::PgWireBackendMessage;
use pgwire::api::auth::{AuthSource, LoginInfo, Password, DefaultServerParameterProvider};
use std::collections::HashMap;
use std::sync::Mutex;
use std::pin::Pin;

#[derive(Clone, Default)]
pub struct BoyodbAuthSource;

#[async_trait::async_trait]
impl AuthSource for BoyodbAuthSource {
    async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
        Ok(Password::new(None, "boyo".as_bytes().to_vec()))
    }
}

pub struct BoyodbPgHandler {
    db: Arc<Db>,
    query_parser: Arc<NoopQueryParser>,
    #[allow(dead_code)]
    auth_handler: Arc<CleartextPasswordAuthStartupHandler<BoyodbAuthSource, DefaultServerParameterProvider>>,
    /// Map of Statement Name -> SQL
    statements: Arc<Mutex<HashMap<String, String>>>,
    /// Map of Portal Name -> SQL (with params substituted)
    portals: Arc<Mutex<HashMap<String, String>>>,
}

impl BoyodbPgHandler {
    pub fn new(db: Arc<Db>) -> Self {
        Self { 
            db,
            query_parser: Arc::new(NoopQueryParser::new()),
            auth_handler: Arc::new(CleartextPasswordAuthStartupHandler::new(
                BoyodbAuthSource,
                DefaultServerParameterProvider::default(),
            )),
            statements: Arc::new(Mutex::new(HashMap::new())),
            portals: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl StartupHandler for BoyodbPgHandler {
    async fn on_startup<C>(&self, client: &mut C, msg: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sink<PgWireBackendMessage>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.auth_handler.on_startup(client, msg).await
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for BoyodbPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync + ClientPortalStore + Sink<PgWireBackendMessage>,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement;

        // Reuse simple query logic
        tracing::debug!("PG Extended Query: {}", query);

        // Check for introspection queries first
        if let Some(result) = handle_introspection_query(query, &self.db) {
            // Convert Vec<Response> to single Response for extended query
            return match result {
                Ok(mut responses) => {
                    if responses.is_empty() {
                        Ok(Response::Execution(Tag::new("OK")))
                    } else {
                        Ok(responses.remove(0))
                    }
                }
                Err(e) => Err(e),
            };
        }

        let req = QueryRequest {
            sql: query.clone(),
            timeout_millis: 10000,
            collect_stats: false,
        };

        let db = self.db.clone();
        let resp = tokio::task::spawn_blocking(move || {
            db.query(req)
        }).await.map_err(|e| PgWireError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        match resp {
            Ok(result) => {
                 if result.records_ipc.is_empty() {
                    let tag_str = query.trim().to_uppercase();
                    let tag = if tag_str.starts_with("BEGIN") || tag_str.starts_with("START TRANSACTION") {
                        "BEGIN"
                    } else if tag_str.starts_with("COMMIT") || tag_str.starts_with("END") {
                        "COMMIT"
                    } else if tag_str.starts_with("ROLLBACK") {
                        "ROLLBACK"
                    } else {
                        "OK"
                    };
                    return Ok(Response::Execution(Tag::new(tag)));
                 }

                 let batches = boyodb_core::engine::read_ipc_batches(&result.records_ipc)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                 let schema = batches[0].schema();
                 let fields: Vec<FieldInfo> = schema.fields().iter().map(|f| {
                    let pg_type = arrow_to_pg_type(f.data_type());
                    FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                }).collect();

                let field_infos = Arc::new(fields);
                let mut results = Vec::new();
                for batch in batches {
                    let rows = batch.num_rows();
                    for i in 0..rows {
                        let mut encoder = DataRowEncoder::new(field_infos.clone());
                        for (col_idx, field) in schema.fields().iter().enumerate() {
                            let array = batch.column(col_idx);
                            encode_arrow_value(&mut encoder, array.as_ref(), field.data_type(), i)?;
                        }
                        results.push(encoder.finish());
                    }
                }
                Ok(Response::Query(QueryResponse::new(field_infos, stream::iter(results))))
            }
            Err(e) => Err(engine_error_to_pg(e)),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync + ClientPortalStore + Sink<PgWireBackendMessage>,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
         Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync + ClientPortalStore + Sink<PgWireBackendMessage>,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
         Ok(DescribePortalResponse::new(vec![]))
    }

    async fn on_bind<C>(
        &self,
        client: &mut C,
        msg: Bind,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + ClientPortalStore + Sink<PgWireBackendMessage>,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!("PG Bind: portal={:?} stmt={:?}", msg.portal_name, msg.statement_name);
        let portal_name = msg.portal_name.clone().unwrap_or_else(|| "".to_string());
        let stmt_name = msg.statement_name.clone().unwrap_or_else(|| "".to_string());
        
        let stmt = client.portal_store().get_statement(&stmt_name)
            .ok_or_else(|| PgWireError::ApiError(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "Statement not found"))))?;
        
        let portal = Portal::try_new(&msg, stmt)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            
        client.portal_store().put_portal(Arc::new(portal));
        client.send(PgWireBackendMessage::BindComplete(BindComplete::new())).await?;
        Ok(())
    }



    async fn on_parse<C>(
        &self,
        client: &mut C,
        msg: Parse,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + ClientPortalStore + Sink<PgWireBackendMessage>,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // println!("DEBUG: In on_parse name={:?}", msg.name);
        let name = msg.name.clone().unwrap_or_else(|| "".to_string());
        let query = msg.query.clone();
        
        let stmt = StoredStatement::new(name.clone(), query, vec![]);
        client.portal_store().put_statement(Arc::new(stmt));
        client.send(PgWireBackendMessage::ParseComplete(ParseComplete::new())).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for BoyodbPgHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        tracing::debug!("PG Query: {}", query);

        // Check for introspection queries first
        if let Some(result) = handle_introspection_query(query, &self.db) {
            return result;
        }

        let req = QueryRequest {
            sql: query.to_string(),
            timeout_millis: 10000,
            collect_stats: false,
        };

        let db = self.db.clone();
        let resp = tokio::task::spawn_blocking(move || {
            db.query(req)
        }).await.map_err(|e| PgWireError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        match resp {
            Ok(result) => {
                if result.records_ipc.is_empty() {
                    let tag_str = query.trim().to_uppercase();
                    let tag = if tag_str.starts_with("BEGIN") || tag_str.starts_with("START TRANSACTION") {
                        "BEGIN"
                    } else if tag_str.starts_with("COMMIT") || tag_str.starts_with("END") {
                        "COMMIT"
                    } else if tag_str.starts_with("ROLLBACK") {
                        "ROLLBACK"
                    } else {
                        "OK"
                    };
                    return Ok(vec![Response::Execution(Tag::new(tag))]);
                }

                let batches = boyodb_core::engine::read_ipc_batches(&result.records_ipc)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                let schema = batches[0].schema();
                let fields: Vec<FieldInfo> = schema.fields().iter().map(|f| {
                    let pg_type = arrow_to_pg_type(f.data_type());
                    FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                }).collect();

                let field_infos = Arc::new(fields);
                let mut results = Vec::new();

                for batch in batches {
                    let rows = batch.num_rows();
                    for i in 0..rows {
                        let mut encoder = DataRowEncoder::new(field_infos.clone());
                        for (col_idx, field) in schema.fields().iter().enumerate() {
                            let array = batch.column(col_idx);
                            encode_arrow_value(&mut encoder, array.as_ref(), field.data_type(), i)?;
                        }
                        results.push(encoder.finish());
                    }
                }

                let stream = stream::iter(results);
                
                Ok(vec![Response::Query(QueryResponse::new(field_infos, stream))])
            }
            Err(e) => Err(engine_error_to_pg(e)),
        }
    }
}

pub struct MakeBoyodbPgHandler {
    db: Arc<Db>,
}

impl MakeBoyodbPgHandler {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

impl MakeHandler for MakeBoyodbPgHandler {
    type Handler = Arc<BoyodbPgHandler>;
    fn make(&self) -> Self::Handler {
        Arc::new(BoyodbPgHandler::new(self.db.clone()))
    }
}

/// Map Arrow data types to PostgreSQL types for wire protocol
fn arrow_to_pg_type(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::INT8, // PostgreSQL doesn't have unsigned types
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
        DataType::Interval(_) => Type::INTERVAL,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Type::UNKNOWN, // Could map to array types
        DataType::Struct(_) => Type::UNKNOWN, // Could use JSON
        DataType::Map(_, _) => Type::UNKNOWN,
        DataType::Union(_, _) => Type::UNKNOWN,
        DataType::Dictionary(_, value_type) => arrow_to_pg_type(value_type),
        DataType::Null => Type::UNKNOWN,
        _ => Type::UNKNOWN,
    }
}

/// Encode an Arrow array value at index i to the PostgreSQL DataRowEncoder
fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &dyn arrow_array::Array,
    data_type: &arrow_schema::DataType,
    row: usize,
) -> Result<(), PgWireError> {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(row) {
        encoder.encode_field(&None::<i32>).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        return Ok(());
    }

    match data_type {
        DataType::Boolean => {
            let val = array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Int8 => {
            let val = array.as_any().downcast_ref::<Int8Array>().unwrap().value(row);
            encoder.encode_field(&(val as i16)).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Int16 => {
            let val = array.as_any().downcast_ref::<Int16Array>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Int32 => {
            let val = array.as_any().downcast_ref::<Int32Array>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Int64 => {
            let val = array.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::UInt8 => {
            let val = array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row);
            encoder.encode_field(&(val as i16)).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::UInt16 => {
            let val = array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row);
            encoder.encode_field(&(val as i32)).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::UInt32 => {
            let val = array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row);
            encoder.encode_field(&(val as i64)).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::UInt64 => {
            let val = array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
            // Cast to i64 (may lose precision for very large values)
            encoder.encode_field(&(val as i64)).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Float32 => {
            let val = array.as_any().downcast_ref::<Float32Array>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Float64 => {
            let val = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Utf8 => {
            let val = array.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::LargeUtf8 => {
            let val = array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Binary => {
            let val = array.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::LargeBinary => {
            let val = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        DataType::Date32 => {
            // Days since Unix epoch -> convert to ISO date string
            let val = array.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(val + 719163); // Days from 0001-01-01 to 1970-01-01
            if let Some(d) = date {
                let date_str: String = d.to_string();
                encoder.encode_field(&date_str).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            } else {
                encoder.encode_field(&None::<i32>).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
        }
        DataType::Date64 => {
            // Milliseconds since Unix epoch -> convert to ISO date string
            let val = array.as_any().downcast_ref::<Date64Array>().unwrap().value(row);
            let secs = val / 1000;
            let nsecs = ((val % 1000) * 1_000_000) as u32;
            let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs);
            if let Some(dt) = ts {
                let date_str = dt.format("%Y-%m-%d").to_string();
                encoder.encode_field(&date_str).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            } else {
                encoder.encode_field(&None::<i32>).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
        }
        DataType::Timestamp(unit, _tz) => {
            use arrow_schema::TimeUnit;
            let micros = match unit {
                TimeUnit::Second => {
                    array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(row) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row) * 1_000
                }
                TimeUnit::Microsecond => {
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row)
                }
                TimeUnit::Nanosecond => {
                    array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row) / 1_000
                }
            };
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1000) as u32;
            let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs);
            if let Some(dt) = ts {
                let ts_str = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                encoder.encode_field(&ts_str).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            } else {
                encoder.encode_field(&None::<i32>).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
        }
        DataType::Decimal128(_, scale) => {
            let val = array.as_any().downcast_ref::<Decimal128Array>().unwrap().value(row);
            let scale = *scale as i32;
            let divisor = 10_i128.pow(scale as u32);
            let int_part = val / divisor;
            let frac_part = (val % divisor).abs();
            let formatted = format!("{}.{:0width$}", int_part, frac_part, width = scale as usize);
            encoder.encode_field(&formatted).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        _ => {
            // Fallback: encode as string representation
            encoder.encode_field(&"?".to_string()).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
    }
    Ok(())
}

/// Handle introspection queries that PostgreSQL clients expect.
/// Returns Some(response) if this is an introspection query, None otherwise.
fn handle_introspection_query<'a>(query: &str, db: &Arc<Db>) -> Option<PgWireResult<Vec<Response<'a>>>> {
    let query_upper = query.trim().to_uppercase();
    let query_normalized = query_upper.replace('\n', " ").replace('\r', "");

    // Handle version() function
    if query_normalized.contains("SELECT VERSION()") || query_normalized.starts_with("SELECT VERSION()") {
        return Some(make_single_row_response(
            "version",
            Type::VARCHAR,
            "BoyoDB 0.1.0 - PostgreSQL protocol compatible",
        ));
    }

    // Handle current_database()
    if query_normalized.contains("CURRENT_DATABASE()") {
        let db_name = db.current_database().unwrap_or_else(|| "boyodb".to_string());
        return Some(make_single_row_response(
            "current_database",
            Type::VARCHAR,
            &db_name,
        ));
    }

    // Handle current_schema() / current_schema
    if query_normalized.contains("CURRENT_SCHEMA") {
        return Some(make_single_row_response(
            "current_schema",
            Type::VARCHAR,
            "public",
        ));
    }

    // Handle current_user / session_user
    if query_normalized.contains("CURRENT_USER") || query_normalized.contains("SESSION_USER") {
        return Some(make_single_row_response(
            "current_user",
            Type::VARCHAR,
            "boyodb",
        ));
    }

    // Handle SHOW commands
    if query_normalized.starts_with("SHOW ") {
        return handle_show_command(&query_normalized);
    }

    // Handle SET commands (just acknowledge them)
    if query_normalized.starts_with("SET ") {
        return Some(Ok(vec![Response::Execution(Tag::new("SET"))]));
    }

    // Handle pg_catalog.pg_type lookups (common for type OID resolution)
    if query_normalized.contains("PG_CATALOG.PG_TYPE") || query_normalized.contains("PG_TYPE") {
        return Some(handle_pg_type_query(&query_normalized));
    }

    // Handle pg_catalog.pg_namespace queries
    if query_normalized.contains("PG_NAMESPACE") {
        return Some(handle_pg_namespace_query());
    }

    // Handle pg_tables / pg_catalog.pg_tables / \dt command
    if query_normalized.contains("PG_TABLES") || query_normalized.contains("PG_CATALOG.PG_CLASS") {
        return Some(handle_pg_tables_query(db));
    }

    // Handle pg_database
    if query_normalized.contains("PG_DATABASE") {
        return Some(handle_pg_database_query(db));
    }

    // Handle pg_settings
    if query_normalized.contains("PG_SETTINGS") {
        return Some(handle_pg_settings_query());
    }

    // Handle pg_stat_activity (for connection info)
    if query_normalized.contains("PG_STAT_ACTIVITY") {
        return Some(make_empty_response());
    }

    // Handle pg_am (access methods) - empty
    if query_normalized.contains("PG_AM") {
        return Some(make_empty_response());
    }

    // Handle pg_index - empty
    if query_normalized.contains("PG_INDEX") {
        return Some(make_empty_response());
    }

    // Handle pg_attribute
    if query_normalized.contains("PG_ATTRIBUTE") {
        return Some(make_empty_response());
    }

    None
}

fn handle_show_command<'a>(query: &str) -> Option<PgWireResult<Vec<Response<'a>>>> {
    if query.contains("SERVER_VERSION") {
        return Some(make_single_row_response("server_version", Type::VARCHAR, "14.0"));
    }
    if query.contains("CLIENT_ENCODING") {
        return Some(make_single_row_response("client_encoding", Type::VARCHAR, "UTF8"));
    }
    if query.contains("SERVER_ENCODING") {
        return Some(make_single_row_response("server_encoding", Type::VARCHAR, "UTF8"));
    }
    if query.contains("STANDARD_CONFORMING_STRINGS") {
        return Some(make_single_row_response("standard_conforming_strings", Type::VARCHAR, "on"));
    }
    if query.contains("TRANSACTION ISOLATION") || query.contains("TRANSACTION_ISOLATION") {
        return Some(make_single_row_response("transaction_isolation", Type::VARCHAR, "read committed"));
    }
    if query.contains("DATESTYLE") || query.contains("DATE_STYLE") {
        return Some(make_single_row_response("DateStyle", Type::VARCHAR, "ISO, MDY"));
    }
    if query.contains("TIMEZONE") || query.contains("TIME ZONE") {
        return Some(make_single_row_response("TimeZone", Type::VARCHAR, "UTC"));
    }
    if query.contains("SEARCH_PATH") {
        return Some(make_single_row_response("search_path", Type::VARCHAR, "\"$user\", public"));
    }
    if query.contains("IS_SUPERUSER") {
        return Some(make_single_row_response("is_superuser", Type::VARCHAR, "on"));
    }
    if query.contains("SESSION_AUTHORIZATION") {
        return Some(make_single_row_response("session_authorization", Type::VARCHAR, "boyodb"));
    }
    // Default for unknown SHOW commands
    Some(make_single_row_response("", Type::VARCHAR, ""))
}

fn handle_pg_type_query<'a>(_query: &str) -> PgWireResult<Vec<Response<'a>>> {
    // Return common PostgreSQL types
    let types = vec![
        (16, "bool", "b"),
        (20, "int8", "b"),
        (21, "int2", "b"),
        (23, "int4", "b"),
        (25, "text", "b"),
        (700, "float4", "b"),
        (701, "float8", "b"),
        (1043, "varchar", "b"),
        (1082, "date", "b"),
        (1114, "timestamp", "b"),
        (1184, "timestamptz", "b"),
        (1700, "numeric", "b"),
        (17, "bytea", "b"),
    ];

    let fields = vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("typname".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("typtype".to_string(), None, None, Type::CHAR, FieldFormat::Text),
    ];
    let field_infos = Arc::new(fields);

    let mut results = Vec::new();
    for (oid, name, typtype) in types {
        let mut encoder = DataRowEncoder::new(field_infos.clone());
        encoder.encode_field(&oid).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        encoder.encode_field(&name).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        encoder.encode_field(&typtype).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        results.push(encoder.finish());
    }

    Ok(vec![Response::Query(QueryResponse::new(field_infos, stream::iter(results)))])
}

fn handle_pg_namespace_query<'a>() -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("nspname".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
    ];
    let field_infos = Arc::new(fields);

    let mut results = Vec::new();

    // Public schema
    let mut encoder = DataRowEncoder::new(field_infos.clone());
    encoder.encode_field(&2200i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    encoder.encode_field(&"public").map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    results.push(encoder.finish());

    // pg_catalog schema
    let mut encoder = DataRowEncoder::new(field_infos.clone());
    encoder.encode_field(&11i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    encoder.encode_field(&"pg_catalog").map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    results.push(encoder.finish());

    Ok(vec![Response::Query(QueryResponse::new(field_infos, stream::iter(results)))])
}

fn handle_pg_tables_query<'a>(db: &Arc<Db>) -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![
        FieldInfo::new("schemaname".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("tablename".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("tableowner".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
    ];
    let field_infos = Arc::new(fields);

    let mut results = Vec::new();

    // Get tables from the engine (pass None to get all tables across all databases)
    if let Ok(tables) = db.list_tables(None) {
        for table in tables {
            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&"public").map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&table.name.as_str()).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&"boyodb").map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            results.push(encoder.finish());
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(field_infos, stream::iter(results)))])
}

fn handle_pg_database_query<'a>(db: &Arc<Db>) -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![
        FieldInfo::new("datname".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("datdba".to_string(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("encoding".to_string(), None, None, Type::INT4, FieldFormat::Text),
    ];
    let field_infos = Arc::new(fields);

    let mut results = Vec::new();

    // List databases
    if let Ok(databases) = db.list_databases() {
        for dbname in databases {
            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&dbname.as_str()).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&10i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;  // owner OID
            encoder.encode_field(&6i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;   // UTF8 encoding
            results.push(encoder.finish());
        }
    } else {
        // Return default database
        let mut encoder = DataRowEncoder::new(field_infos.clone());
        let db_name = db.current_database().unwrap_or_else(|| "boyodb".to_string());
        encoder.encode_field(&db_name.as_str()).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        encoder.encode_field(&10i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        encoder.encode_field(&6i32).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        results.push(encoder.finish());
    }

    Ok(vec![Response::Query(QueryResponse::new(field_infos, stream::iter(results)))])
}

fn handle_pg_settings_query<'a>() -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![
        FieldInfo::new("name".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("setting".to_string(), None, None, Type::VARCHAR, FieldFormat::Text),
    ];
    let field_infos = Arc::new(fields);

    let settings = vec![
        ("server_version", "14.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("standard_conforming_strings", "on"),
    ];

    let mut results = Vec::new();
    for (name, value) in settings {
        let mut encoder = DataRowEncoder::new(field_infos.clone());
        encoder.encode_field(&name).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        encoder.encode_field(&value).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        results.push(encoder.finish());
    }

    Ok(vec![Response::Query(QueryResponse::new(field_infos, stream::iter(results)))])
}

fn make_single_row_response<'a>(
    column_name: &str,
    pg_type: Type,
    value: &str,
) -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![FieldInfo::new(
        column_name.to_string(),
        None,
        None,
        pg_type,
        FieldFormat::Text,
    )];
    let field_infos = Arc::new(fields);

    let mut encoder = DataRowEncoder::new(field_infos.clone());
    encoder.encode_field(&value).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let row = encoder.finish();

    Ok(vec![Response::Query(QueryResponse::new(
        field_infos,
        stream::iter(vec![row]),
    ))])
}

fn make_empty_response<'a>() -> PgWireResult<Vec<Response<'a>>> {
    let fields = vec![FieldInfo::new(
        "".to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )];
    let field_infos = Arc::new(fields);
    Ok(vec![Response::Query(QueryResponse::new(
        field_infos,
        stream::iter(vec![]),
    ))])
}

/// PostgreSQL SQLSTATE codes for common errors
/// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
mod sqlstate {
    pub const SYNTAX_ERROR: &str = "42601";           // Syntax error
    pub const UNDEFINED_TABLE: &str = "42P01";        // Table does not exist
    pub const UNDEFINED_COLUMN: &str = "42703";       // Column does not exist
    pub const UNDEFINED_FUNCTION: &str = "42883";     // Function does not exist
    pub const UNDEFINED_DATABASE: &str = "3D000";     // Database does not exist
    pub const INVALID_CATALOG_NAME: &str = "3D000";   // Invalid catalog name
    pub const DATA_EXCEPTION: &str = "22000";         // Data exception
    pub const INTEGRITY_CONSTRAINT: &str = "23000";   // Integrity constraint violation
    pub const INVALID_AUTH: &str = "28000";           // Invalid authorization
    pub const INTERNAL_ERROR: &str = "XX000";         // Internal error
    pub const CONNECTION_EXCEPTION: &str = "08000";   // Connection exception
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";  // Feature not supported
    pub const QUERY_CANCELED: &str = "57014";         // Query canceled
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501"; // Insufficient privilege
}

/// Convert an EngineError to a PostgreSQL-compatible UserError
fn engine_error_to_pg(e: boyodb_core::engine::EngineError) -> PgWireError {
    use boyodb_core::engine::EngineError;

    let (code, message) = match &e {
        EngineError::NotFound(msg) => {
            // Determine if it's a table, column, or database based on message content
            if msg.to_lowercase().contains("table") {
                (sqlstate::UNDEFINED_TABLE, format!("table does not exist: {}", msg))
            } else if msg.to_lowercase().contains("column") {
                (sqlstate::UNDEFINED_COLUMN, format!("column does not exist: {}", msg))
            } else if msg.to_lowercase().contains("database") {
                (sqlstate::UNDEFINED_DATABASE, format!("database does not exist: {}", msg))
            } else {
                (sqlstate::INTERNAL_ERROR, format!("not found: {}", msg))
            }
        }
        EngineError::InvalidArgument(msg) => {
            // Check for syntax-related errors
            if msg.to_lowercase().contains("syntax") || msg.to_lowercase().contains("parse") {
                (sqlstate::SYNTAX_ERROR, format!("syntax error: {}", msg))
            } else {
                (sqlstate::DATA_EXCEPTION, format!("invalid argument: {}", msg))
            }
        }
        EngineError::Internal(msg) => (sqlstate::INTERNAL_ERROR, format!("internal error: {}", msg)),
        EngineError::NotImplemented(msg) => (sqlstate::FEATURE_NOT_SUPPORTED, format!("feature not supported: {}", msg)),
        EngineError::Io(msg) => (sqlstate::INTERNAL_ERROR, format!("I/O error: {}", msg)),
        EngineError::Timeout(msg) => (sqlstate::QUERY_CANCELED, format!("query timed out: {}", msg)),
        EngineError::Remote(msg) => (sqlstate::CONNECTION_EXCEPTION, format!("remote error: {}", msg)),
        EngineError::Configuration(msg) => (sqlstate::INTERNAL_ERROR, format!("configuration error: {}", msg)),
    };

    let error_info = ErrorInfo::new("ERROR".to_string(), code.to_string(), message);
    PgWireError::UserError(Box::new(error_info))
}

/// Create a user error with custom SQLSTATE code and message
fn make_user_error(code: &str, message: &str) -> PgWireError {
    let error_info = ErrorInfo::new("ERROR".to_string(), code.to_string(), message.to_string());
    PgWireError::UserError(Box::new(error_info))
}

/// Create a user error with hint
fn make_user_error_with_hint(code: &str, message: &str, hint: &str) -> PgWireError {
    let mut error_info = ErrorInfo::new("ERROR".to_string(), code.to_string(), message.to_string());
    error_info.hint = Some(hint.to_string());
    PgWireError::UserError(Box::new(error_info))
}
