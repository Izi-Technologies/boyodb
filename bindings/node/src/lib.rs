use boyodb_core::{BundleRequest, Db, EngineConfig, IngestBatch, QueryRequest};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

#[napi(object)]
pub struct DatabaseOptions {
    pub data_dir: String,
    pub wal_dir: Option<String>,
    pub shard_count: Option<u16>,
    pub cache_bytes: Option<BigInt>,
    pub wal_max_bytes: Option<BigInt>,
    pub wal_max_segments: Option<BigInt>,
    pub allow_manifest_import: Option<bool>,
}

#[napi]
pub struct Database {
    inner: Arc<Db>,
}

fn bigint_to_u64(b: &BigInt) -> u64 {
    let (_, val, _) = b.get_u64();
    val
}

#[napi]
impl Database {
    #[napi(constructor)]
    pub fn new(opts: DatabaseOptions) -> Result<Self> {
        let shard_count = opts.shard_count.unwrap_or(4);
        let cache_bytes = opts
            .cache_bytes
            .map(|b| bigint_to_u64(&b))
            .unwrap_or(512 * 1024 * 1024);
        let mut cfg =
            EngineConfig::new(&opts.data_dir, shard_count as usize).with_cache_bytes(cache_bytes);
        if let Some(wal) = opts.wal_dir {
            cfg = cfg.with_wal_dir(wal);
        }
        if let Some(ref max_bytes) = opts.wal_max_bytes {
            cfg = cfg.with_wal_max_bytes(bigint_to_u64(max_bytes));
        }
        if let Some(ref max_segments) = opts.wal_max_segments {
            cfg = cfg.with_wal_max_segments(bigint_to_u64(max_segments));
        }
        if opts.allow_manifest_import.unwrap_or(false) {
            cfg = cfg.with_allow_manifest_import(true);
        }

        let db = Db::open(cfg).map_err(to_napi_error)?;
        Ok(Database {
            inner: Arc::new(db),
        })
    }

    #[napi]
    pub fn ingest(
        &self,
        payload: Buffer,
        watermark_micros: BigInt,
        shard_key: Option<BigInt>,
    ) -> Result<()> {
        let batch = IngestBatch {
            payload_ipc: payload.to_vec(),
            watermark_micros: bigint_to_u64(&watermark_micros),
            shard_override: shard_key.as_ref().map(bigint_to_u64),
            database: None,
            table: None,
        };
        self.inner.ingest_ipc(batch).map_err(to_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn ingest_into(
        &self,
        payload: Buffer,
        watermark_micros: BigInt,
        shard_key: Option<BigInt>,
        database: Option<String>,
        table: Option<String>,
    ) -> Result<()> {
        let batch = IngestBatch {
            payload_ipc: payload.to_vec(),
            watermark_micros: bigint_to_u64(&watermark_micros),
            shard_override: shard_key.as_ref().map(bigint_to_u64),
            database,
            table,
        };
        self.inner.ingest_ipc(batch).map_err(to_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn query(&self, sql: String, timeout_millis: u32) -> Result<Buffer> {
        let request = QueryRequest {
            sql,
            timeout_millis,
            collect_stats: false,
        };
        let resp = self.inner.query(request).map_err(to_napi_error)?;
        Ok(Buffer::from(resp.records_ipc))
    }

    #[napi]
    pub fn plan_bundle(
        &self,
        prefer_hot: Option<bool>,
        max_bytes: Option<BigInt>,
        since_version: Option<BigInt>,
        target_bytes_per_sec: Option<BigInt>,
        max_entries: Option<u32>,
    ) -> Result<Buffer> {
        let req = BundleRequest {
            max_bytes: max_bytes.as_ref().map(bigint_to_u64),
            since_version: since_version.as_ref().map(bigint_to_u64),
            prefer_hot: prefer_hot.unwrap_or(true),
            target_bytes_per_sec: target_bytes_per_sec.as_ref().map(bigint_to_u64),
            max_entries: max_entries.map(|v| v as usize),
        };
        let plan = self.inner.plan_bundle(req).map_err(to_napi_error)?;
        let json = serde_json::to_vec(&plan).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("failed to serialize bundle plan: {e}"),
            )
        })?;
        Ok(Buffer::from(json))
    }

    #[napi]
    pub fn create_database(&self, name: String) -> Result<()> {
        self.inner.create_database(&name).map_err(to_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn create_table(
        &self,
        database: String,
        table: String,
        schema_json: Option<String>,
    ) -> Result<()> {
        self.inner
            .create_table(&database, &table, schema_json)
            .map_err(to_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn healthcheck(&self) -> Result<()> {
        self.inner.health_check().map_err(to_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn list_databases(&self) -> Result<Buffer> {
        let dbs = self.inner.list_databases().map_err(to_napi_error)?;
        let json = serde_json::to_vec(&dbs).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("failed to serialize databases: {e}"),
            )
        })?;
        Ok(Buffer::from(json))
    }

    #[napi]
    pub fn list_tables(&self, database: Option<String>) -> Result<Buffer> {
        let tables = self
            .inner
            .list_tables(database.as_deref())
            .map_err(to_napi_error)?;
        let json = serde_json::to_vec(&tables).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("failed to serialize tables: {e}"),
            )
        })?;
        Ok(Buffer::from(json))
    }

    #[napi]
    pub fn manifest(&self) -> Result<Buffer> {
        let manifest = self.inner.export_manifest().map_err(to_napi_error)?;
        Ok(Buffer::from(manifest))
    }

    #[napi]
    pub fn import_manifest(&self, manifest: Buffer, overwrite: Option<bool>) -> Result<()> {
        let overwrite = overwrite.unwrap_or(false);
        self.inner
            .import_manifest(&manifest, overwrite)
            .map_err(to_napi_error)?;
        Ok(())
    }
}

fn to_napi_error(err: boyodb_core::engine::EngineError) -> Error {
    Error::new(Status::GenericFailure, err.to_string())
}
