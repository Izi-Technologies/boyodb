pub mod auth;
pub mod auto_tuner;
pub mod bloom_utils;
pub mod cdc_sink;
pub mod cluster;
pub mod column_masking;
pub mod connection_pool;
pub mod engine;
pub mod ffi;
pub mod fts;
pub mod geospatial;
pub mod hnsw;
pub mod http_api;
pub mod incremental_backup;
pub mod incremental_mv;
pub mod index_advisor;
pub mod io_uring;
pub mod jit;
pub mod kafka_sink;
pub mod lakehouse;
pub mod query_hints;
pub mod replica_sync;
pub mod replication;
pub mod replication_worker;
pub mod vector_quantization;
pub use replication::{BundlePayload, BundleSegment};
pub mod analytics;
pub mod audit_log;
pub mod compression;
pub mod data_formats;
pub mod delta_encoding;
pub mod distributed;
pub mod execution;
pub mod executor_distributed;
pub mod external;
pub mod flight;
pub mod functions;
pub mod gorilla;
pub mod gpu;
pub mod high_availability;
pub mod mergetree;
pub mod mutations;
pub mod observability;
pub mod operations;
pub mod optimizer;
pub mod planner_distributed;
pub mod projections;
pub mod rbac_v2;
pub mod realtime;
pub mod resource_governance;
pub mod security;
pub mod sql;
pub mod storage;
pub mod streaming;
pub mod telemetry;
pub mod tiering;
pub mod time_travel;
pub mod tooling;
pub mod ttl;
pub mod types;
pub mod vector;
pub mod vectorized;
pub mod vectorized_join;
pub mod wal;

// --- Financial-Grade ACID Transaction Support (New Modules) ---
pub mod btree;
pub mod constraint_validator;
pub mod lock_manager;
pub mod mvcc;
pub mod optimizer_integration;
pub mod pitr;
pub mod transaction;
pub mod undo_log;
pub mod wal_archive;

// --- Enterprise Features (Phase 20) ---
pub mod enterprise_auth;
pub mod plan_cache;
pub mod procedures;
pub mod stats_functions;
pub mod triggers;
pub mod two_phase_commit;

// --- Production Stability Features (Phase 21) ---
pub mod cross_db;
pub mod extensions;
pub mod fdw;
pub mod logical_replication;
pub mod online_ddl;
pub mod partitioning;
pub mod user_types;

// --- SQL Compatibility Features (Phase 22) ---
pub mod advisory_locks;
pub mod generated_columns;
pub mod lateral_joins;
pub mod pubsub;
pub mod sequences;

// --- Production Hardening Features (Phase 23) ---
pub mod connection_management;
pub mod data_integrity;
pub mod pg_stat;
pub mod query_safety;
pub mod replica_mode;

// --- Operational Features (Phase 24) ---
pub mod advanced_indexes;
pub mod bulk_operations;
pub mod deadlock_detection;
pub mod graceful_shutdown;
pub mod monitoring_endpoints;
pub mod protocol_security;
pub mod query_enhancements;

// --- Query Execution & Maintenance (Phase 25) ---
pub mod cursor_support;
pub mod parallel_query;
pub mod planner_hints;
pub mod vacuum_analyze;

// --- Advanced SQL Features (Phase 26) ---
pub mod domain_types;
pub mod event_triggers;
pub mod rules_system;
pub mod table_inheritance;

// --- Storage & Replication (Phase 27) ---
pub mod large_objects;
pub mod logical_decoding;
pub mod physical_backup;
pub mod tablespaces;

// --- Internationalization & Search (Phase 28) ---
pub mod collation;
pub mod fts_enhanced;
pub mod icu_support;

// --- Advanced Data Types (Phase 29) ---
pub mod geometric_types;
pub mod network_types;
pub mod range_types;
pub mod xml_support;

// --- Performance Optimizations (Phase 30) ---
pub mod hot_updates;
pub mod index_only_scans;
pub mod memory_context;
pub mod parallel_index;

// --- Replication Enhancements (Phase 31) ---
pub mod conflict_resolution;
pub mod subscription;
pub mod sync_replication;

// --- Operational Excellence (Phase 32) ---
pub mod connection_pooler;
pub mod pg_dump;
pub mod pg_upgrade;
pub mod query_explain;

// --- Competitive Features (Phase 33) ---
pub mod cdc_webhooks;
pub mod graphql_api;
pub mod ml_inference;
pub mod otel_integration;

// --- Advanced ML Features (Phase 34) ---
pub mod automl;
pub mod embeddings_engine;
pub mod feature_store;
pub mod ml_explainability;
pub mod model_monitoring;
pub mod online_learning;

pub use auth::{
    AuthError, AuthManager, PasswordPolicy, Privilege, PrivilegeGrant, PrivilegeTarget, Role,
    RoleInfo, Session, SessionInfo, User, UserInfo, UserStatus,
};
pub use engine::{
    apply_computed_columns, apply_window_functions, evaluate_expr, execute_query_with_ctes,
    merge_cte_results, start_auto_repair_task, validate_identifier, AutoRepairConfig,
    AutoRepairState, AutoRepairStats, ComputedValue, CteContext, Db, EngineConfig, EngineError,
    EvalContext, ExplainPlan, HealthStatus, IngestBatch, Metrics, QueryExecutionStats,
    QueryRequest, QueryResponse, ScalarValue, StreamDefinition, StreamRegistry, StreamState,
    TableDescription, UdfRegistry, UserDefinedFunction, VacuumResult,
};
pub use replica_sync::{
    spawn_replica_sync_worker, ReplicaSyncConfig, ReplicaSyncMetrics, ReplicaSyncMetricsSnapshot,
    ReplicaSyncWorker,
};
pub use replication::{
    BundlePlan, BundleRequest, DatabaseMeta, Manifest, ManifestEntry, SegmentTier, TableMeta,
};
pub use sql::{
    parse_ctes, parse_expr, parse_select_items_extended, parse_sql, AuthCommand, CteDefinition,
    DdlCommand, DeduplicationConfig, DeduplicationMode, DeleteCommand, GrantTargetType, GroupBy,
    GroupByColumn, InsertCommand, JoinClause, JoinCondition, JoinTable, JoinType, LiteralValue,
    MergeCommand, MergeWhenMatched, MergeWhenNotMatched, OnConflict, OnConflictAction,
    OrderByClause, ParsedQuery, PreparedStatementCommand, PubSubCommand,
    QueryFilter as SqlQueryFilter, ScalarFunction, SelectColumn, SelectExpr, SqlStatement,
    SqlValue, UpdateCommand, UserOptions, WindowFrame, WindowFrameBound, WindowFrameUnit,
    WindowFunction, WindowSpec,
};
pub use types::{BoyodbStatus, OwnedBuffer};

// Cluster module re-exports
pub use cluster::{
    ClusterConfig, ClusterManager, ClusterStatus, ElectionConfig, ElectionState, GossipConfig,
    GossipProtocol, Membership, NodeId, NodeMeta, NodeRole, NodeState, ReadConsistency,
    WriteResult,
};

// Real-time & Streaming re-exports (Phase 4)
pub use realtime::{
    CdcCapture, CdcCheckpoint, CdcError, CdcEvent, CdcEventType, CdcSourceConfig, CdcSourceType,
    ConsumerOffsets, DeltaOp, DeltaValue, ExactlyOnceIngestor, IngestRecord, IngestionError,
    IngestionState, IngestionTransaction, KafkaConsumer, MaterializedColumn, MaterializedView,
    MaterializedViewDef, OffsetReset, PulsarConsumer, RefreshMode, RowDelta, SecurityProtocol,
    StreamConsumer, StreamError, StreamMessage, StreamPipeline, StreamProducer, StreamSourceConfig,
    StreamStage,
};

// Operations & Production re-exports (Phase 5)
pub use operations::{
    BackupConfig, BackupError, BackupFile, BackupManager, BackupManifest, BackupProgress,
    BackupState, BackupType, OnlineSchemaChangeManager, OperatorProfile, QueryProfile,
    QueryProfiler, QuotaManager, QuotaViolation, ResourceQuota, ResourceUsage, RestoreConfig,
    RestoreResult, SchemaChange, SchemaChangeError, SchemaChangeProgress, SchemaChangeState,
    SchemaChangeType, StageProfile, StorageTier, TtlAction, TtlManager, TtlRule, TtlScanResult,
};

// External Integrations re-exports (Phase 6)
pub use external::{
    CachedObjectStore, ColumnChunkMetadata, ColumnStatistics, DictionaryError, DictionaryKey,
    DictionaryManager, DictionarySource, DictionaryStats, DictionaryTable, DictionaryValue,
    ExternalColumn, ExternalDataType, ExternalFormat, ExternalFormatType, ExternalLocation,
    ExternalPredicate, ExternalSchema, ExternalTable, ExternalTableError, ExternalTableManager,
    ExternalTableScan, ExternalTableType, InMemoryObjectStore, ListResult, MultipartUpload,
    ObjectMeta, ObjectPath, ObjectStore, ParquetColumn, ParquetCompression, ParquetError,
    ParquetLogicalType, ParquetManager, ParquetMetadata, ParquetPhysicalType, ParquetPredicate,
    ParquetReadOptions, ParquetRow, ParquetSchema, ParquetValue, ParquetWriteOptions,
    ParquetWriteResult, PredicateValue, RowGroupMetadata, StorageConfig, StorageError,
    StorageProvider, TimeUnit, UploadPart,
};

// Security Hardening re-exports (Phase 7)
pub use security::{
    AuditConfig, AuditEntry, AuditEventType, AuditFilter, AuditLogger, AuditSeverity,
    ColumnEncryptionManager, ColumnEncryptionPolicy, EncryptedValue, EncryptionAlgorithm,
    EncryptionKey, RlsCommand, RlsContext, RlsError, RlsExpression, RlsManager, RlsPolicy,
    RlsPolicyType, RlsResult, RlsValue,
};

// Advanced Compression re-exports (Phase 8)
pub use compression::{
    BitPackEncoder, Codec, CodecType, CompressionError, CompressionManager, CompressionStats,
    DeltaEncoder, DictionaryEncoded, DictionaryEncoder, DoubleDeltaEncoder, Lz4Codec, RleEncoder,
    ZstdCodec,
};

// Mutations & Specialized Engines re-exports (Phase 9)
pub use mutations::{
    AggState, ColumnUpdate, DeleteMask, DeleteStats, LightweightDeleteManager, MergeRow,
    MergeTreeConfig, MergeTreeEngine, MergeTreeMerger, MergeTreeSettings, Mutation, MutationError,
    MutationExpr, MutationManager, MutationPart, MutationPredicate, MutationProgress,
    MutationState, MutationType, MutationValue,
};

// Observability re-exports (Phase 10)
pub use observability::{
    AsyncMetric, CachedResult, ErrorEntry, HistogramBucket, MetricDefinition, ObservabilityManager,
    PartEventType, PartLogEntry, ProcessEntry, PrometheusExporter, PrometheusHistogram,
    PrometheusMetric, PrometheusMetricType, QueryCache, QueryCacheConfig, QueryCacheStats,
    QueryKind, QueryLogEntry, QueryType, SystemTable, SystemTablesManager,
};

// Advanced Functions re-exports (Phase 11)
pub use functions::{
    ArrayFunctions, FunctionError, FunctionResult, GeoFunctions, InvertedIndex, IpFunctions,
    JsonFunctions, MapFunctions, NgramTokenizer, Posting, Sampler, SamplingMethod, SimpleTokenizer,
    Token, Tokenizer, UrlFunctions, Value as FunctionValue,
};

// Distributed DDL & Global Indexes re-exports (Phase 12)
pub use distributed::{
    BufferLayers, ClusterDefinition, ClusterNode, ColumnDefinition, DataType as DdlDataType,
    DdlError, DdlNodeTask, DdlOperation, DdlQueueEntry, DdlTask, DdlTaskStatus,
    DistributedDdlManager, GlobalIndex, GlobalIndexConfig, GlobalIndexManager, GlobalIndexStats,
    GlobalIndexType, IndexDefinition, IndexType, IndexValue, ProjectionDefinition, ShardLocation,
    TableAlteration, TableEngine, TableSchema,
};

// Query Execution re-exports (Phase 13)
pub use execution::{
    AdaptiveJoinExecutor, AggregateFunction, AggregateStage, ColumnData, DataBatch, DataPartition,
    ExecutionConfig, ExecutionError, ExecutionStats, FilterStage, JoinAlgorithm, JoinConfig,
    JoinStats, JoinType as ExecutionJoinType, LimitStage, ParallelTask, PipelineStage,
    QueryPipeline, ScanStage, SortStage, SpillFormat, SpillManager, SpillMetadata, TaskState,
};

// Data Formats re-exports (Phase 14)
pub use data_formats::{
    AddFile, ColumnChunkMeta, ColumnStatsMeta, CommitInfo, DataFileInfo, DataFileStats,
    DataFormatError, DataLakeField, DataLakeOperation, DataLakeSchema, DataLakeTable, DataLakeType,
    IsolationLevel, MergeAction, MergeActionType, NativeOrcReader, NativeOrcWriter,
    NativeParquetReader, NativeParquetWriter, OptimizeResult, OrcColumnData, OrcColumnMeta,
    OrcColumnStats, OrcCompression, OrcField, OrcFileMeta, OrcPredicate, OrcReadOpts, OrcRowBatch,
    OrcSchema, OrcStripeMeta, OrcType, OrcValue, OrcWriteOpts, ParquetColumnData,
    ParquetCompressionCodec, ParquetEncoding, ParquetFieldDef, ParquetFileMeta,
    ParquetLogicalType as NativeParquetLogicalType, ParquetPredicate as NativeParquetPredicate,
    ParquetReadOpts, ParquetRepetition, ParquetRowBatch, ParquetSchemaDef, ParquetType,
    ParquetValue as NativeParquetValue, ParquetWriteOpts, ParquetWriterVersion, ProtocolChange,
    RemoveFile, RowGroupMeta, TableFormat, TableFormatSpec, TableMetadataChange, TableSnapshot,
    TimeUnitType, TransactionAction, TransactionLogEntry, VacuumResult as DataLakeVacuumResult,
    WriteMode, ZOrderStats,
};

// High Availability re-exports (Phase 15)
pub use high_availability::{
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClusterHaStatus,
    // Multi-Region Replication
    ConflictResolution,
    ElectionState as HaElectionState,
    FailoverEvent,
    FailoverEventType,
    FailoverManager,
    HaConfig,
    HaError,
    HaManager,
    HealthCheckResult,
    HealthMonitor,
    LeaderElection,
    LoadBalanceStrategy,
    LogEntry,
    LogEntryType,
    MultiRegionManager,
    MultiRegionStats,
    PendingWrite,
    QuorumWriter,
    ReadPreference,
    RegionConfig,
    RegionReplicationMode,
    RegionReplicationStatus,
    RegionState,
    RegionWriteConcern,
    ReplicaInfo,
    ReplicaRole,
    ReplicaSelector,
    ReplicaState,
    ReplicationConflict,
    VoteRequest,
    VoteResponse,
    WriteAck,
    WriteConsistency,
};

// Resource Governance re-exports (Phase 16)
pub use resource_governance::{
    IoPriority, IoRequest, IoScheduler, IoStats, IoType, MemoryAllocation, MemoryError,
    MemoryManager, MemoryPool, MemoryPoolStats, QueryAdmission, QueryContext, QueryThrottler,
    QuotaError, RateLimiter, ResourceError, ResourceGovernor, ResourceGovernorConfig,
    ResourceGovernorStats, ResourceQuotaDef, ResourceTracker, ResourceUsageInfo, ThrottlerStats,
    WorkloadError, WorkloadGroup, WorkloadGroupState, WorkloadGroupStats, WorkloadManager,
    WorkloadPriority,
};

// Tooling re-exports (Phase 17)
pub use tooling::{
    BackupConfig as ToolingBackupConfig, BenchmarkConfig, BenchmarkResult, CliCommand,
    ColumnDef as ToolingColumnDef, ColumnTransform, ColumnType, CommandParser, CompressionType,
    ConversionComplexity, CsvOptions, DataExporter, DataFormat, DataImporter, ErrorHandling,
    ExportConfig, ExportResult, FormatConverter, FormatOptions, ImportConfig, ImportError,
    ImportResult, InferenceSettings, JsonOptions, LocalEngine, LocalTable, MigrateDirection,
    Migration, MigrationManager, ParquetCompression as ToolingParquetCompression,
    ParquetOptions as ToolingParquetOptions, QueryResult as ToolingQueryResult,
    RestoreConfig as ToolingRestoreConfig, SchemaInferrer, TableSchema as ToolingTableSchema,
    ToolingError, ToolingResult, TypeCoercion,
};

// GPU Acceleration re-exports (Phase 19)
pub use gpu::{
    AggregateResult,
    AggregationType,
    FilterOp,
    FilterPredicate,
    GpuConfig,
    GpuDecision,
    GpuDeviceInfo,
    GpuError,
    GpuExecutionStats,
    GpuExecutor,
    // GPU Memory Pool
    GpuMemoryAllocation,
    GpuMemoryPool,
    GpuMemoryPoolStats,
    GpuOperation,
    GpuStatus,
    // GPU Vector Operations
    GpuVectorOps,
    // Metal support (macOS)
    MetalDeviceInfo,
    MetalExecutor,
    VectorOp,
};

// --- Financial-Grade ACID Transaction Support re-exports ---

// Transaction Management (ACID compliance)
pub use transaction::{
    IsolationLevel as TxnIsolationLevel, PrepareResult, Savepoint, Transaction, TransactionId,
    TransactionManager, TransactionState, TransactionStats, NO_TRANSACTION,
};

// Lock Manager (Concurrency Control)
pub use lock_manager::{
    LockHandle, LockManager, LockManagerConfig, LockMode, LockStats, LockTarget,
};

// Undo Log (Rollback Support)
pub use undo_log::{UndoLog, UndoLogConfig, UndoLogStats, UndoRecord};

// MVCC (Snapshot Isolation)
pub use mvcc::{MvccManager, MvccStats, MvccVisibility, RowKey, Snapshot};

// B-Tree Index (Range Queries)
pub use btree::{
    build_btree_from_sorted, BTree, BTreeBuilder, BTreeHeader, BTreeKey, BTreeNode, BTreeRange,
    NodeType,
};

// WAL Archiving (for PITR)
pub use wal_archive::{WalArchiveConfig, WalArchiveInfo, WalArchiveStats, WalArchiver};

// Point-in-Time Recovery
pub use pitr::{BackupInfo, RecoveryConfig, RecoveryManager, RecoveryResult, WalStatus};

// Constraint Validation (Data Integrity)
pub use constraint_validator::{
    ConstraintType, ConstraintValidator, ConstraintViolation, ExistingDataProvider,
    InMemoryDataProvider, ValidationConfig, ValidationResult,
};

// Extended SQL types for financial features
pub use sql::{ForeignKeyAction, SqlIsolationLevel, TableConstraint, TransactionCommand};

// Optimizer and Index Advisor
pub use optimizer::{
    // Adaptive Query Execution
    AdaptiveCheckpoint,
    AdaptiveDecision,
    AdaptiveExecutionConfig,
    AdaptiveExecutionSummary,
    AdaptiveExecutor,
    // Index Advisor
    ColumnAccessPattern,
    ColumnStatistics as OptimizerColumnStats,
    // Complexity Scoring
    ComplexityBasedAdmission,
    // Cost Model Tuning
    CostBasedOptimizer,
    CostCalibrationStats,
    // Aliased to avoid conflicts
    CostModelParams as OptimizerCostParams,
    CostModelTuner,
    ExistingIndex,
    FilterType,
    Histogram as OptimizerHistogram,
    HistogramBucket as OptimizerHistogramBucket,
    IndexAdvisor,
    IndexAdvisorConfig,
    IndexAdvisorStats,
    IndexRecommendation,
    JoinStrategy,
    QueryComplexity,
    QueryCost,
    QueryExecutionStats as StoredQueryStats,
    // Query Store
    QueryFingerprint,
    QueryPlanSnapshot,
    QueryResourceCost,
    QueryStore,
    QueryStoreConfig,
    QueryStoreSummary,
    RecommendedIndexType,
    RuntimeStats,
    StatValue,
    StoredQuery,
    TableAccessPattern,
    TableStats as OptimizerTableStats,
    TenantQueryToken,
    // Per-Tenant Resource Quotas
    TenantQuota,
    TenantQuotaError,
    TenantResourceManager,
    TenantUsage,
};

// --- Competitive Features re-exports (Phase 33) ---

// GraphQL API
pub use graphql_api::{
    ArgumentDefinition, DirectiveDefinition, DirectiveLocation, FieldDefinition, GraphQLConfig,
    GraphQLError, GraphQLExecutor, GraphQLLocation, GraphQLRequest, GraphQLResponse, GraphQLSchema,
    GraphQLType, ScalarType, TypeDefinition, TypeKind,
};

// ML Inference
pub use ml_inference::{
    sql_predict, FeatureSpec, FeatureType, InferenceRequest, InferenceResult, MLConfig, MLError,
    MLModel, ModelFormat, ModelRegistry, ModelType, OutputType, Prediction, PredictionExplanation,
    Preprocessing, RegistryStats,
};

// OpenTelemetry Integration
pub use otel_integration::{
    AttributeValue, BoyoDbInstrumentation, Meter, MetricDataPoint, MetricKind, OtelConfig,
    OtlpExporter, PrometheusExporter as OtelPrometheusExporter, Span, SpanEvent, SpanKind,
    SpanLink, SpanStatus, TraceContext, Tracer,
};

// CDC Webhooks
pub use cdc_webhooks::{
    ChangeEvent, ChangeOperation, ChangeTracker, DeliveryAttempt, DeliveryStatus, PendingDelivery,
    WebhookBuilder, WebhookConfig, WebhookDeliveryWorker, WebhookError, WebhookRegistry,
    WebhookStats,
};

// --- Advanced ML Features re-exports (Phase 34) ---

// Feature Store
pub use feature_store::{
    EntityRow, FeatureDataType, FeatureDefinition, FeatureGroup, FeaturePipeline, FeatureSource,
    FeatureStore, FeatureStoreError, FeatureStoreStats, FeatureValue, FeatureVector, FeatureView,
    MissingStrategy, OnlineStore, PipelineStep, PointInTimeRequest, Transformation,
};

// Model Monitoring
pub use model_monitoring::{
    Alert, AlertChannel, AlertConfig, AlertSeverity, ComparisonOp, DistributionStats, DriftResult,
    DriftSeverity, DriftTest, ModelMonitor, MonitoredMetric, MonitoringRegistry,
    PerformanceMetrics, PredictionLog, ReferenceDistribution,
};

// Embeddings Engine
pub use embeddings_engine::{
    Embedding, EmbeddingConfig, EmbeddingModelType, EmbeddingRegistry, EmbeddingStats,
    EmbeddingsEngine, SentenceTransformerModel, Tokenizer as EmbeddingTokenizer,
};

// Online Learning
pub use online_learning::{
    ArmStats, BanditAlgorithm, ContextualBandit, Example, LearningRateSchedule, MultiArmedBandit,
    OnlineAlgorithm, OnlineConfig, OnlineLearningRegistry, OnlineLinearModel, OnlineModelStats,
};

// AutoML
pub use automl::{
    AutoML, AutoMLConfig, AutoMLRegistry, AutoMLResult, CrossValidation, FeatureImportance,
    HyperparameterSpace, HyperparameterValue, Metric as AutoMLMetric, ModelType as AutoMLModelType,
    SearchStrategy, TaskType, TrainingData, TrialResult, TrialStatus,
};

// ML Explainability
pub use ml_explainability::{
    Counterfactual, CounterfactualExplainer, ExplainabilityRegistry, ExplainableModel,
    ExplanationType, FeatureChange, FeatureContribution, LimeExplainer, LimeExplanation,
    LinearExplainableModel, PartialDependence, PermutationImportance, ShapExplainer,
    ShapExplanation,
};

// --- Data Platform Features (Phase 35) ---
pub mod blockchain_ledger;
pub mod data_catalog;
pub mod data_quality;
pub mod graph_engine;
pub mod nl_to_sql;
pub mod time_series_engine;
pub mod workflow_engine;

// Time Series Engine
pub use time_series_engine::{
    AggregatedBucket, DataPoint, DownsamplePolicy, GapFillStrategy, InterpolationMethod,
    TimeAggregation, TimeBucket, TimeRange, TimeSeries, TimeSeriesFunctions, TimeSeriesQuery,
    TimeSeriesStore,
};

// Graph Engine
pub use graph_engine::{
    Direction, Edge, EdgePattern, GraphDatabase, GraphQuery, GraphStats, Node, NodePattern, Path,
    PropertyValue, QueryCondition, QueryResult as GraphQueryResult, ResultValue,
};

// Data Quality
pub use data_quality::{
    Anomaly, AnomalyType, CellValue, ColumnProfile, DataProfile, DataQualityEngine,
    DataType as DqDataType, QualityMetric, QualityReport, Severity,
    ValidationResult as DqValidationResult, ValidationRule,
};

// Natural Language to SQL
pub use nl_to_sql::{
    AggregationType as NlAggregationType, ColumnSchema, ComparisonOp as NlComparisonOp, Condition,
    Entity, EntityType, GeneratedSQL, NLToSQL, ParsedQuery as NlParsedQuery, QueryIntent,
    TableSchema as NlTableSchema,
};

// Data Catalog
pub use data_catalog::{
    CatalogEntry, CatalogStats, Classification, ColumnEntry, ColumnStats, DataCatalog, EntryType,
    GlossaryTerm, LineageDirection, LineageEdge, LineageEdgeType, LineageGraph, LineageNode,
    LineageNodeType, SearchQuery, SearchResult, TermStatus,
};

// Blockchain Ledger
pub use blockchain_ledger::{
    AuditEntry as LedgerAuditEntry, AuditTrail, Block, BlockchainLedger, ChainVerification,
    LedgerRegistry, LedgerStats, MerkleNode, MerkleTree, Transaction as LedgerTransaction,
    TransactionType,
};

// Workflow Engine
pub use workflow_engine::{
    LogEntry as WorkflowLogEntry, LogLevel, RetryPolicy, RunStats, RunStatus, Schedule,
    ScheduleType, Task as WorkflowTask, TaskState as WorkflowTaskState, TaskStatus,
    TaskType as WorkflowTaskType, TriggerType, Workflow, WorkflowEngine, WorkflowError,
    WorkflowRegistry, WorkflowRun,
};

// --- Advanced Analytics Features (Phase 36) ---
pub mod data_contracts;
pub mod lakehouse_formats;
pub mod query_federation;
pub mod realtime_dashboard;
pub mod sql_extensions;
pub mod vector_search;

// Vector Search
pub use vector_search::{
    DistanceMetric, HnswConfig, HnswIndex, HnswStats, ProductQuantizer,
    SearchResult as VectorSearchResult, Vector, VectorIndexRegistry,
};

// Query Federation
pub use query_federation::{
    AggregateExpr, ColumnStats as FederationColumnStats, DataSource, DataSourceType,
    ExecutionStats as FederationExecutionStats, FederatedPlan, FederatedQuery, FederatedResult,
    FederatedValue, FederationEngine, FederationRegistry, JoinType as FederatedJoinType, PlanStep,
    QueryPart, SourceCapabilities, SourceConfig, SourceStats, TableStats as FederationTableStats,
};

// Real-time Dashboard
pub use realtime_dashboard::{
    Alert as DashboardAlert, AlertSeverity as DashboardAlertSeverity, Dashboard, DashboardManager,
    LogEntry as DashboardLogEntry, MessagePayload, MessageType, MetricPoint, MetricSeries,
    MetricsRegistry, StreamingHub, Subscription, SubscriptionManager, SubscriptionType, Threshold,
    Widget, WidgetLayout, WidgetOptions, WidgetType, WsMessage,
};

// Data Contracts
pub use data_contracts::{
    ChangeType, CompatibilityChecker, CompatibilityMode, CompatibilityResult, ContractRegistry,
    ContractStore, DataContract, DataType as ContractDataType,
    FieldDefinition as ContractFieldDefinition, IndexDefinition as ContractIndexDefinition,
    SchemaChange as ContractSchemaChange, SchemaDefinition, SemanticVersion,
    ValidationResult as ContractValidationResult, ValidationRule as ContractValidationRule,
    ValidationRuleType, VersionBump,
};

// Lakehouse Formats
pub use lakehouse_formats::{
    AddFile as DeltaAddFile, CommitInfoAction, DeltaAction, DeltaField, DeltaSchema, DeltaTable,
    DeltaTransaction, DeltaType, FileFormat, IcebergField, IcebergSchema, IcebergSnapshot,
    IcebergTable, IcebergType, LakehouseRegistry, LakehouseStore, ManifestContent, ManifestFile,
    MetadataAction, PartitionField, PartitionSpec, PartitionTransform, ProtocolAction,
    RemoveFile as DeltaRemoveFile, SnapshotOperation, SnapshotSummary, SortDirection, SortField,
    SortOrder, TableIdentifier, TxnAction,
};

// SQL Extensions
pub use sql_extensions::{
    ExtensionCategory, FunctionCall, Param, ParamType, QueryRewriter, RewriteResult, SqlExtension,
    SqlExtensionRegistry, SqlValue as ExtSqlValue,
};

// --- Performance & Testing (Phase 37) ---
pub mod benchmarks;

// Benchmarks
pub use benchmarks::{
    BenchmarkResult as PerfBenchmarkResult, BenchmarkRunner, BenchmarkSuite, LatencyHistogram,
    ThroughputSummary, ThroughputTracker,
};

// --- Gap Analysis Features (Phase 38: ClickHouse & PostgreSQL Parity) ---
pub mod approximate;
pub mod async_insert;
pub mod cdc;
pub mod external_tables;
pub mod query_profiler;
pub mod wasm_udf;

// Approximate Functions (HyperLogLog, T-Digest, Count-Min Sketch)
pub use approximate::{
    approx_count_distinct, approx_median, approx_quantile, CountMinSketch, HyperLogLog, TDigest,
};

// Change Data Capture (Debezium-compatible)
pub use cdc::{
    CdcConnector, CdcConnectorConfig, CdcError as CdcErrorV2, CdcEvent as CdcEventV2,
    CdcEventType as CdcEventTypeV2, CdcPayload, CdcSchema, CdcSchemaField, CdcSink as CdcSinkV2,
    CdcSource, CdcStats, CdcTransaction, DecimalHandling, MemorySink, OutputFormat, SnapshotMode,
};

// WebAssembly UDFs
pub use wasm_udf::{
    WasmContext, WasmError, WasmModule, WasmSignature, WasmType, WasmUdfRegistry, WasmValue,
};

// Query Profiler (Flame Graphs)
pub use query_profiler::{
    IoProfile, MemoryProfile, OperatorProfile as QueryOperatorProfile, PhaseGuard, ProfileBuilder,
    ProfilePhase, QueryProfile as DetailedQueryProfile, QueryProfiler as DetailedQueryProfiler,
    RowProfile, WaitEvent, WaitEventType,
};

// Async Insert Buffering
pub use async_insert::{
    AsyncInsertBuffer, AsyncInsertConfig, AsyncInsertStats, BufferResult, BufferStatus,
    BufferedRow, FlushCallback, FlushResult, TableBuffer,
};

// External Tables (S3, URL, HDFS, File)
pub use external_tables::{
    ColumnDef as ExternalColumnDef, ExternalScanOptions, ExternalScanner, ExternalTableConfig,
    ExternalTableError as ExternalTableErrorV2, ExternalTableRegistry,
    ExternalTableType as ExternalTableTypeV2, FileFormat as ExtFileFormat, S3Options, ScanBatch,
};

// --- Gap Analysis Features (Phase 39: Advanced Features) ---
pub mod ai_query_optimizer;
pub mod exclusion_constraints;
pub mod gin_gist_indexes;
pub mod parallel_replicas;
pub mod tiered_compilation;
pub mod zero_copy_replication;

// Parallel Replicas (ClickHouse-style)
pub use parallel_replicas::{
    CoordinatorStats, LoadBalanceStrategy as ReplicaLoadBalanceStrategy, MergedResult,
    ParallelQueryPlan, ParallelReplicaConfig, ParallelReplicaCoordinator, ParallelReplicaError,
    PartResult, QueryPart as ReplicaQueryPart, ReplicaEndpoint,
};

// Zero-Copy Replication
pub use zero_copy_replication::{
    GcResult, LockType, SegmentHandle, SegmentRef, StorageTier as ZeroCopyStorageTier, SyncResult,
    ZeroCopyConfig, ZeroCopyError, ZeroCopyManager, ZeroCopyMetadata, ZeroCopyStats,
};

// Exclusion Constraints (PostgreSQL-compatible)
pub use exclusion_constraints::{
    ColumnType as ExclusionColumnType, ConstraintValue, ExclusionConstraint,
    ExclusionConstraintManager, ExclusionElement, ExclusionError, ExclusionOperator,
    ExclusionStats, ExclusionViolation, IndexMethod, RangeValue, RowId,
};

// GIN/GiST Indexes
pub use gin_gist_indexes::{
    AdvancedIndexMeta, AdvancedIndexRegistry, AdvancedIndexType, BoxKey, GinConfig, GinIndex,
    GinStats, GistConfig, GistEntry, GistIndex, GistKey, GistNode, GistStats, RangeKey,
};

// AI Query Optimizer
pub use ai_query_optimizer::{
    AiQueryOptimizer, CardinalityModel, ColumnStats as AiColumnStats, CostModel, ExecutionHistory,
    HistogramBucket as AiHistogramBucket, OptimizationResult, OptimizerStats as AiOptimizerStats,
    PlanAlternative, PlanScoringWeights, Predicate, PredicateOp, QueryFeatures,
    TableStats as AiTableStats,
};

// Tiered Compilation
pub use tiered_compilation::{
    CacheInfo, CompilationError, CompilationRequest, CompilationStats, CompilationTier,
    CompiledCode, IRAggregate, IROp, IRPredicate, IRValue, QueryIR, TieredCompilationConfig,
    TieredCompilationManager,
};

// --- Operational & Data Platform Features (Phase 40) ---
pub mod data_lineage;
pub mod deployment;
pub mod integrations;
pub mod schema_migrations;

// Schema Migrations (aliased to avoid conflict with tooling module)
pub use schema_migrations::{
    ChecksumMismatch, Migration as SchemaMigration, MigrationConfig, MigrationError, MigrationId,
    MigrationManager as SchemaMigrationManager, MigrationRecord, MigrationStatus,
    MigrationStatusReport, MigrationVersion,
};

// Deployment (Blue-Green, Connection Draining, Resource Pools)
pub use deployment::{
    BlueGreenConfig, ConnectionInfo, ConnectionTracker, DeploymentError, DeploymentManager,
    DeploymentRecord, DeploymentSlot, DeploymentStatus, DrainConfig, DrainHandle, DrainResult,
    PoolStatus, PoolUsage, ResourceError as DeploymentResourceError, ResourceGrant, ResourcePool,
    ResourcePoolManager, SlotInfo, SlotStatus,
};

// Data Lineage, Schema Evolution, Data Quality Monitoring
pub use data_lineage::{
    Anomaly as LineageAnomaly, AnomalyConfig, AnomalyDetector, AnomalyType as LineageAnomalyType,
    AssetId, AssetType, ColumnId, ColumnInfo, ColumnRef as LineageColumnRef,
    ColumnStats as LineageColumnStats, CompatibilityLevel,
    CompatibilityResult as LineageCompatibilityResult, CompatibilityViolation, DataAsset,
    DataQualityManager, DataQualityRule, DataSensitivity, ImpactAnalysis,
    LineageEdge as DataLineageEdge, LineageGraph as DataLineageGraph, LineageManager, LineageType,
    QualityCheckResult, RuleSeverity, RuleType, SchemaChange as LineageSchemaChange,
    SchemaChangeType as LineageSchemaChangeType, SchemaEvolutionManager, SchemaVersion,
};

// Data Platform Integrations (Spark, Flink, dbt, Airflow, Presto/Trino)
pub use integrations::{
    AirflowOperatorConfig, AirflowOperatorGenerator,
    ConnectionConfig as IntegrationConnectionConfig, DbtAdapterConfig, DbtColumn,
    DbtMaterialization, DbtModel, DbtProfileGenerator, FlinkConnectorConfig, FlinkDdlGenerator,
    FlinkLookupCache, IntegrationManager, PrestoConnectorConfig, PrestoConnectorGenerator,
    PrestoPluginInfo, SparkConnector, SparkConnectorConfig, SparkField, SparkPartition,
    SparkSchema, SparkWriteMode, TypeMapping,
};

// --- Enterprise Operations Features (Phase 41) ---
pub mod auto_scaling;
pub mod cdc_lake;
pub mod data_retention;
pub mod multi_region_dr;
pub mod query_cache;
pub mod query_cost_api;
pub mod query_replay;
pub mod tenant_isolation;

// Query Result Caching
pub use query_cache::{
    CacheConfig, CacheEntry, CacheInvalidator, CacheKey, CacheStatsSnapshot, CacheValue,
    CachedResult as QueryCachedResult, InvalidationEvent, QueryResultCache, RedisCacheCommand,
    RedisCacheServer, RedisResponse,
};

// Multi-Region Disaster Recovery
pub use multi_region_dr::{
    DRError, DRMetrics, DisasterRecoveryConfig, DnsRecord, DnsRecordType, DnsRoutingManager,
    FailoverEvent as DRFailoverEvent, FailoverReason, FailoverState, MultiRegionDRManager,
    RegionConfig as DRRegionConfig, RegionHealth, RegionId, RegionStatus, ReplicationBatch,
    ReplicationEvent, ReplicationStatus as DRReplicationStatus, RoutingPolicy,
};

// Query Cost Estimation API
pub use query_cost_api::{
    ColumnStatistics as CostColumnStatistics, CostEstimationService, CostModelConfig, CostWarning,
    FilterPredicate as CostFilterPredicate, Histogram as CostHistogram,
    HistogramBucket as CostHistogramBucket, IndexStatistics as CostIndexStatistics, OperationCost,
    OperationType, PlanOperation, QueryComplexity as CostQueryComplexity, QueryCostEstimate,
    QueryCostEstimator, QueryPlan as CostQueryPlan, QuickEstimate, ResourcePredictions,
    TableStatistics as CostTableStatistics, WarningSeverity,
};

// Tenant Isolation
pub use tenant_isolation::{
    BackupError as TenantBackupError, BackupStatus, BackupStorageLocation,
    BackupType as TenantBackupType, DataEncryptionKey, DataResidency, EncryptedData,
    EncryptionAlgorithm as TenantEncryptionAlgorithm, EncryptionError, KeyManagement,
    NamespaceEncryptionManager, QuotaCheckOperation, RestoreRequest, RestoreState, RestoreStatus,
    TenantBackup, TenantBackupConfig, TenantBackupManager, TenantConfig, TenantEncryption,
    TenantError, TenantId, TenantManager, TenantQuotas, TenantStatus,
    TenantUsage as TenantResourceUsage,
};

// CDC to Data Lakes
pub use cdc_lake::{
    BatchConfig, CdcColumn as LakeCdcColumn, CdcDataType as LakeCdcDataType,
    CdcEvent as LakeCdcEvent, CdcLakeSinkManager, CdcOperation, CdcSchema as LakeCdcSchema,
    CdcValue as LakeCdcValue, CompactionConfig, DeltaLakeWriter, IcebergWriter, LakeError,
    LakeFormat, LakeSink, LakeSinkConfig, LakeStorage, LakeWriterStatsSnapshot,
    WriteMode as LakeWriteMode, WriteResult as LakeWriteResult,
};

// Query Replay/Shadowing
pub use query_replay::{
    CaptureConfig, CaptureFilters, CaptureStatsSnapshot, CaptureStorage, CapturedQuery, ParamValue,
    PerformanceComparison, QueryCaptureService, QueryParam, QueryReplayResult, QueryReplayService,
    ReplayConfig, ReplayMode, ReplayProgress, ReplayReport, ReplayStatsSnapshot, ReplayStatus,
    ResultComparison, RowDiff, ShadowStatsSnapshot, ShadowTarget, ShadowTrafficService,
};

// Auto-Scaling
pub use auto_scaling::{
    AutoScalingConfig, AutoScalingManager, AutoScalingStatus, CapacityForecast,
    ComparisonOperator as ScalingComparisonOperator, MetricAggregation,
    MetricDataPoint as ScalingMetricDataPoint, MetricsCollector, PolicyType, PredictiveScaler,
    ScalingAction, ScalingDecision, ScalingError, ScalingEvent, ScalingEventType, ScalingMetric,
    ScalingPolicy,
};

// Data Retention & Compliance
pub use data_retention::{
    AnonymizationStrategy, ComplianceReport, ComplianceRequirement, DataCategory,
    DataRetentionManager, DataSubjectRequest, DataSubjectRequestType, ExportFormat, LegalHold,
    LegalHoldScope, LegalHoldStatus, PurgeAction, PurgeEvent, PurgeSchedule, RequestNote,
    RequestResult, RequestStatus, RetentionError, RetentionPeriod, RetentionPolicy, RetentionScope,
    RetentionStatsSnapshot, TableRef as RetentionTableRef,
};
