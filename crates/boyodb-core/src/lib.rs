pub mod auth;
pub mod bloom_utils;
pub mod cluster;
pub mod engine;
pub mod ffi;
pub mod replication;
pub mod replication_worker;
pub use replication::{BundlePayload, BundleSegment};
pub mod analytics;
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
pub mod realtime;
pub mod resource_governance;
pub mod security;
pub mod sql;
pub mod storage;
pub mod streaming;
pub mod tiering;
pub mod tooling;
pub mod types;
pub mod vector;
pub mod vectorized;
pub mod wal;

// --- Financial-Grade ACID Transaction Support (New Modules) ---
pub mod btree;
pub mod constraint_validator;
pub mod lock_manager;
pub mod mvcc;
pub mod pitr;
pub mod transaction;
pub mod undo_log;
pub mod wal_archive;
pub mod optimizer_integration;

pub use auth::{
    AuthError, AuthManager, PasswordPolicy, Privilege, PrivilegeGrant, PrivilegeTarget, Role,
    RoleInfo, Session, SessionInfo, User, UserInfo, UserStatus,
};
pub use engine::{
    apply_computed_columns, apply_window_functions, evaluate_expr, execute_query_with_ctes,
    merge_cte_results, validate_identifier, ComputedValue, CteContext, Db, EngineConfig,
    EvalContext, ExplainPlan, HealthStatus, IngestBatch, Metrics, QueryExecutionStats,
    QueryRequest, QueryResponse, TableDescription, VacuumResult,
};
pub use replication::{
    BundlePlan, BundleRequest, DatabaseMeta, Manifest, ManifestEntry, SegmentTier, TableMeta,
};
pub use sql::{
    parse_ctes, parse_expr, parse_select_items_extended, parse_sql, AuthCommand, CteDefinition,
    DdlCommand, DeduplicationConfig, DeduplicationMode, DeleteCommand, GrantTargetType, GroupBy,
    GroupByColumn, InsertCommand, JoinClause, JoinCondition, JoinType, LiteralValue, OrderByClause,
    ParsedQuery, QueryFilter as SqlQueryFilter, ScalarFunction, SelectColumn, SelectExpr,
    SqlStatement, SqlValue, UpdateCommand, UserOptions, WindowFrame, WindowFrameBound,
    WindowFrameUnit, WindowFunction, WindowSpec,
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
    AppendEntriesRequest, AppendEntriesResponse, ClusterHaStatus, ElectionState as HaElectionState,
    FailoverEvent, FailoverEventType, FailoverManager, HaConfig, HaError, HaManager,
    HealthCheckResult, HealthMonitor, LeaderElection, LoadBalanceStrategy, LogEntry, LogEntryType,
    PendingWrite, QuorumWriter, ReadPreference, ReplicaInfo, ReplicaRole, ReplicaSelector,
    ReplicaState, VoteRequest, VoteResponse, WriteAck, WriteConsistency,
    // Multi-Region Replication
    ConflictResolution, MultiRegionManager, MultiRegionStats, RegionConfig, RegionReplicationMode,
    RegionReplicationStatus, RegionState, RegionWriteConcern, ReplicationConflict,
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
    AggregateResult, AggregationType, FilterOp, FilterPredicate, GpuConfig, GpuDecision,
    GpuDeviceInfo, GpuError, GpuExecutionStats, GpuExecutor, GpuOperation, GpuStatus,
    // Metal support (macOS)
    MetalDeviceInfo, MetalExecutor,
    // GPU Vector Operations
    GpuVectorOps, VectorOp,
    // GPU Memory Pool
    GpuMemoryAllocation, GpuMemoryPool, GpuMemoryPoolStats,
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
    // Index Advisor
    ColumnAccessPattern, ExistingIndex, FilterType, IndexAdvisor, IndexAdvisorConfig,
    IndexAdvisorStats, IndexRecommendation, RecommendedIndexType, TableAccessPattern,
    // Query Store
    QueryFingerprint, QueryPlanSnapshot, QueryStore, QueryStoreConfig, QueryStoreSummary,
    StoredQuery,
    // Complexity Scoring
    ComplexityBasedAdmission, QueryComplexity, QueryResourceCost,
    // Cost Model Tuning
    CostBasedOptimizer, CostCalibrationStats, CostModelTuner, QueryCost, StatValue,
    // Adaptive Query Execution
    AdaptiveCheckpoint, AdaptiveDecision, AdaptiveExecutionConfig, AdaptiveExecutionSummary,
    AdaptiveExecutor, JoinStrategy, RuntimeStats,
    // Per-Tenant Resource Quotas
    TenantQuota, TenantQuotaError, TenantQueryToken, TenantResourceManager, TenantUsage,
    // Aliased to avoid conflicts
    CostModelParams as OptimizerCostParams,
    ColumnStatistics as OptimizerColumnStats,
    Histogram as OptimizerHistogram,
    HistogramBucket as OptimizerHistogramBucket,
    QueryExecutionStats as StoredQueryStats,
    TableStats as OptimizerTableStats,
};
