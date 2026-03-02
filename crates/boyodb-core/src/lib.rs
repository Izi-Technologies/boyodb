pub mod auth;
pub mod bloom_utils;
pub mod cluster;
pub mod engine;
pub mod ffi;
pub mod replication;
pub mod replication_worker;
pub use replication::{BundlePayload, BundleSegment};
pub mod sql;
pub mod types;
pub mod planner_distributed;
pub mod executor_distributed;
pub mod storage;
pub mod tiering;
pub mod vectorized;
pub mod delta_encoding;
pub mod mergetree;
pub mod gorilla;
pub mod projections;
pub mod flight;
pub mod vector;
pub mod optimizer;
pub mod streaming;
pub mod analytics;
pub mod realtime;
pub mod operations;
pub mod external;
pub mod security;
pub mod compression;
pub mod mutations;
pub mod observability;
pub mod functions;
pub mod distributed;
pub mod execution;
pub mod data_formats;
pub mod high_availability;
pub mod resource_governance;
pub mod tooling;
pub mod wal;

pub use auth::{
    AuthError, AuthManager, PasswordPolicy, Privilege, PrivilegeGrant, PrivilegeTarget, Role,
    RoleInfo, Session, SessionInfo, User, UserInfo, UserStatus,
};
pub use engine::{
    apply_computed_columns, apply_window_functions, ComputedValue, CteContext, Db, EngineConfig,
    EvalContext, evaluate_expr, execute_query_with_ctes, ExplainPlan, HealthStatus, IngestBatch,
    merge_cte_results, Metrics, QueryExecutionStats, QueryRequest, QueryResponse, TableDescription,
    validate_identifier, VacuumResult,
};
pub use replication::{
    BundlePlan, BundleRequest, DatabaseMeta, Manifest, ManifestEntry, SegmentTier, TableMeta,
};
pub use sql::{
    parse_sql, parse_expr, parse_select_items_extended, parse_ctes,
    AuthCommand, CteDefinition, DdlCommand, DeduplicationConfig, DeduplicationMode, DeleteCommand,
    GrantTargetType, GroupBy, GroupByColumn, InsertCommand, JoinClause, JoinCondition, JoinType,
    LiteralValue, OrderByClause, ParsedQuery, QueryFilter as SqlQueryFilter, ScalarFunction,
    SelectColumn, SelectExpr, SqlStatement, SqlValue, UpdateCommand, UserOptions, WindowFrame,
    WindowFrameBound, WindowFrameUnit, WindowFunction, WindowSpec,
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
    CdcCapture, CdcCheckpoint, CdcError, CdcEvent, CdcEventType, CdcSourceConfig,
    CdcSourceType, ConsumerOffsets, DeltaOp, DeltaValue, ExactlyOnceIngestor,
    IngestionError, IngestionState, IngestionTransaction, IngestRecord, KafkaConsumer,
    MaterializedColumn, MaterializedView, MaterializedViewDef, OffsetReset, PulsarConsumer,
    RefreshMode, RowDelta, SecurityProtocol, StreamConsumer, StreamError, StreamMessage,
    StreamPipeline, StreamProducer, StreamSourceConfig, StreamStage,
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
    DeltaEncoder, DictionaryEncoded, DictionaryEncoder, DoubleDeltaEncoder, Lz4Codec,
    RleEncoder, ZstdCodec,
};

// Mutations & Specialized Engines re-exports (Phase 9)
pub use mutations::{
    AggState, ColumnUpdate, DeleteMask, DeleteStats, LightweightDeleteManager, MergeRow,
    MergeTreeConfig, MergeTreeEngine, MergeTreeMerger, MergeTreeSettings, Mutation,
    MutationError, MutationExpr, MutationManager, MutationPart, MutationPredicate,
    MutationProgress, MutationState, MutationType, MutationValue,
};

// Observability re-exports (Phase 10)
pub use observability::{
    AsyncMetric, CachedResult, ErrorEntry, HistogramBucket, MetricDefinition,
    ObservabilityManager, PartEventType, PartLogEntry, ProcessEntry, PrometheusExporter,
    PrometheusHistogram, PrometheusMetric, PrometheusMetricType, QueryCache, QueryCacheConfig,
    QueryCacheStats, QueryKind, QueryLogEntry, QueryType, SystemTable, SystemTablesManager,
};

// Advanced Functions re-exports (Phase 11)
pub use functions::{
    ArrayFunctions, FunctionError, FunctionResult, GeoFunctions, InvertedIndex, IpFunctions,
    JsonFunctions, MapFunctions, NgramTokenizer, Posting, Sampler, SamplingMethod,
    SimpleTokenizer, Token, Tokenizer, UrlFunctions, Value as FunctionValue,
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
    DataFormatError, DataLakeField, DataLakeOperation, DataLakeSchema, DataLakeTable,
    DataLakeType, IsolationLevel, MergeAction, MergeActionType, NativeOrcReader, NativeOrcWriter,
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
    AppendEntriesRequest, AppendEntriesResponse, ClusterHaStatus,
    ElectionState as HaElectionState, FailoverEvent, FailoverEventType, FailoverManager,
    HaConfig, HaError, HaManager, HealthCheckResult, HealthMonitor, LeaderElection,
    LoadBalanceStrategy, LogEntry, LogEntryType, PendingWrite, QuorumWriter, ReadPreference,
    ReplicaInfo, ReplicaRole, ReplicaSelector, ReplicaState, VoteRequest, VoteResponse,
    WriteAck, WriteConsistency,
};

// Resource Governance re-exports (Phase 16)
pub use resource_governance::{
    IoRequest, IoScheduler, IoStats, IoType, IoPriority, MemoryAllocation, MemoryError,
    MemoryManager, MemoryPool, MemoryPoolStats, QueryAdmission, QueryContext, QueryThrottler,
    QuotaError, RateLimiter, ResourceError, ResourceGovernor, ResourceGovernorConfig,
    ResourceGovernorStats, ResourceQuotaDef, ResourceTracker, ResourceUsageInfo, ThrottlerStats,
    WorkloadError, WorkloadGroup, WorkloadGroupState, WorkloadGroupStats, WorkloadManager,
    WorkloadPriority,
};

// Tooling re-exports (Phase 17)
pub use tooling::{
    BackupConfig as ToolingBackupConfig, BenchmarkConfig, BenchmarkResult, CliCommand,
    ColumnDef as ToolingColumnDef, ColumnTransform, ColumnType, CommandParser,
    CompressionType, ConversionComplexity, CsvOptions, DataExporter, DataFormat, DataImporter,
    ErrorHandling, ExportConfig, ExportResult, FormatConverter, FormatOptions, ImportConfig,
    ImportError, ImportResult, InferenceSettings, JsonOptions, LocalEngine, LocalTable,
    MigrateDirection, Migration, MigrationManager, ParquetCompression as ToolingParquetCompression,
    ParquetOptions as ToolingParquetOptions, QueryResult as ToolingQueryResult,
    RestoreConfig as ToolingRestoreConfig, SchemaInferrer, TableSchema as ToolingTableSchema,
    ToolingError, ToolingResult, TypeCoercion,
};
