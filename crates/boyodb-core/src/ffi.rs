//! FFI bindings for C ABI access to the database engine.
//!
//! These functions are designed to be called from C code and intentionally
//! dereference raw pointers. The safety guarantees are provided by the caller.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::engine::{Db, EngineConfig, EngineError, IngestBatch, QueryRequest};
use crate::replication::{BundlePayload, BundleRequest};
use crate::types::{BoyodbStatus, OwnedBuffer};
use std::cell::RefCell;
use std::ffi::{c_char, CStr};
use std::thread_local;

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = RefCell::new(None);
}

#[repr(C)]
pub struct BoyodbHandle {
    db: Db,
}

#[repr(C)]
pub struct BoyodbOpenOptions {
    pub data_dir: *const c_char,
    pub wal_dir: *const c_char,
    pub shard_count: u16,
    pub cache_bytes: u64,
    pub wal_max_bytes: u64,
    pub wal_max_segments: u64,
    pub allow_manifest_import: bool,
    pub tier_warm_compression: *const c_char,
    pub tier_cold_compression: *const c_char,
    pub cache_hot_segments: bool,
    pub cache_warm_segments: bool,
    pub cache_cold_segments: bool,
}

#[repr(C)]
pub struct BoyodbQueryRequest {
    pub sql: *const c_char,
    pub timeout_millis: u32,
}

#[repr(C)]
pub struct BoyodbBundleRequest {
    pub max_bytes: u64,
    pub has_max_bytes: bool,
    pub since_version: u64,
    pub has_since_version: bool,
    pub prefer_hot: bool,
    pub target_bytes_per_sec: u64,
    pub has_target_bytes_per_sec: bool,
}

#[no_mangle]
pub extern "C" fn boyodb_open(
    opts: *const BoyodbOpenOptions,
    out_handle: *mut *mut BoyodbHandle,
) -> BoyodbStatus {
    if opts.is_null() || out_handle.is_null() {
        return BoyodbStatus::InvalidArgument;
    }

    let opts = unsafe { &*opts };
    let data_dir = match cstr_to_str(opts.data_dir) {
        Ok(s) => s,
        Err(status) => return status,
    };

    let wal_dir = if opts.wal_dir.is_null() {
        None
    } else {
        match cstr_to_str(opts.wal_dir) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    };
    let tier_warm_compression = if opts.tier_warm_compression.is_null() {
        None
    } else {
        match cstr_to_str(opts.tier_warm_compression) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    };
    let tier_cold_compression = if opts.tier_cold_compression.is_null() {
        None
    } else {
        match cstr_to_str(opts.tier_cold_compression) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    };

    let mut cfg =
        EngineConfig::new(data_dir, opts.shard_count as usize).with_cache_bytes(opts.cache_bytes);
    if let Some(wal) = wal_dir {
        cfg = cfg.with_wal_dir(wal);
    }
    if opts.wal_max_bytes > 0 {
        cfg = cfg.with_wal_max_bytes(opts.wal_max_bytes);
    }
    if opts.wal_max_segments > 0 {
        cfg = cfg.with_wal_max_segments(opts.wal_max_segments);
    }
    if opts.allow_manifest_import {
        cfg = cfg.with_allow_manifest_import(true);
    }
    if tier_warm_compression.is_some() {
        cfg = cfg.with_tier_warm_compression(tier_warm_compression);
    }
    if tier_cold_compression.is_some() {
        cfg = cfg.with_tier_cold_compression(tier_cold_compression);
    }
    cfg = cfg
        .with_cache_hot_segments(opts.cache_hot_segments)
        .with_cache_warm_segments(opts.cache_warm_segments)
        .with_cache_cold_segments(opts.cache_cold_segments);

    match Db::open(cfg) {
        Ok(db) => {
            let handle = Box::new(BoyodbHandle { db });
            unsafe { *out_handle = Box::into_raw(handle) };
            record_error(None);
            BoyodbStatus::Ok
        }
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_close(handle: *mut BoyodbHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[no_mangle]
pub extern "C" fn boyodb_ingest_ipc(
    handle: *mut BoyodbHandle,
    payload: *const u8,
    len: usize,
    watermark_micros: u64,
) -> BoyodbStatus {
    boyodb_ingest_ipc_v2(handle, payload, len, watermark_micros, 0, false)
}

#[no_mangle]
pub extern "C" fn boyodb_ingest_ipc_v2(
    handle: *mut BoyodbHandle,
    payload: *const u8,
    len: usize,
    watermark_micros: u64,
    shard_hint: u64,
    has_shard_hint: bool,
) -> BoyodbStatus {
    boyodb_ingest_ipc_v3(
        handle,
        payload,
        len,
        watermark_micros,
        shard_hint,
        has_shard_hint,
        std::ptr::null(),
        false,
        std::ptr::null(),
        false,
    )
}

#[no_mangle]
pub extern "C" fn boyodb_ingest_ipc_v3(
    handle: *mut BoyodbHandle,
    payload: *const u8,
    len: usize,
    watermark_micros: u64,
    shard_hint: u64,
    has_shard_hint: bool,
    database: *const c_char,
    has_database: bool,
    table: *const c_char,
    has_table: bool,
) -> BoyodbStatus {
    if handle.is_null() || payload.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let data = unsafe { std::slice::from_raw_parts(payload, len) }.to_vec();

    let db_name = if has_database {
        match cstr_to_str(database) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    } else {
        None
    };
    let table_name = if has_table {
        match cstr_to_str(table) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    } else {
        None
    };
    let batch = IngestBatch {
        payload_ipc: data,
        watermark_micros,
        shard_override: if has_shard_hint {
            Some(shard_hint)
        } else {
            None
        },
        database: db_name,
        table: table_name,
    };
    status_from_result(db.db.ingest_ipc(batch))
}

#[no_mangle]
pub extern "C" fn boyodb_query_ipc(
    handle: *mut BoyodbHandle,
    request: *const BoyodbQueryRequest,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || request.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }

    let req = unsafe { &*request };
    let sql = match cstr_to_str(req.sql) {
        Ok(s) => s,
        Err(status) => return status,
    };

    let query = QueryRequest {
        sql,
        timeout_millis: req.timeout_millis,
        collect_stats: false,
    };
    match unsafe { &mut *handle }.db.query(query) {
        Ok(resp) => {
            let buf = OwnedBuffer::from_vec(resp.records_ipc);
            unsafe { *out_buffer = buf };
            record_error(None);
            BoyodbStatus::Ok
        }
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_plan_bundle(
    handle: *mut BoyodbHandle,
    request: *const BoyodbBundleRequest,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || request.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let req = unsafe { &*request };
    let bundle_req = BundleRequest {
        max_bytes: if req.has_max_bytes {
            Some(req.max_bytes)
        } else {
            None
        },
        since_version: if req.has_since_version {
            Some(req.since_version)
        } else {
            None
        },
        prefer_hot: req.prefer_hot,
        target_bytes_per_sec: if req.has_target_bytes_per_sec {
            Some(req.target_bytes_per_sec)
        } else {
            None
        },
        max_entries: None,
    };

    match unsafe { &mut *handle }.db.plan_bundle(bundle_req) {
        Ok(plan) => match serde_json::to_vec(&plan) {
            Ok(json) => {
                let buf = OwnedBuffer::from_vec(json);
                unsafe { *out_buffer = buf };
                record_error(None);
                BoyodbStatus::Ok
            }
            Err(_) => BoyodbStatus::Internal,
        },
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_export_bundle(
    handle: *mut BoyodbHandle,
    request: *const BoyodbBundleRequest,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || request.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let req = unsafe { &*request };
    let bundle_req = BundleRequest {
        max_bytes: if req.has_max_bytes {
            Some(req.max_bytes)
        } else {
            None
        },
        since_version: if req.has_since_version {
            Some(req.since_version)
        } else {
            None
        },
        prefer_hot: req.prefer_hot,
        target_bytes_per_sec: if req.has_target_bytes_per_sec {
            Some(req.target_bytes_per_sec)
        } else {
            None
        },
        max_entries: None,
    };

    match unsafe { &mut *handle }.db.export_bundle(bundle_req) {
        Ok(payload) => match serde_json::to_vec(&payload) {
            Ok(json) => {
                let buf = OwnedBuffer::from_vec(json);
                unsafe { *out_buffer = buf };
                record_error(None);
                BoyodbStatus::Ok
            }
            Err(_) => BoyodbStatus::Internal,
        },
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_apply_bundle(
    handle: *mut BoyodbHandle,
    payload_json: *const u8,
    len: usize,
) -> BoyodbStatus {
    if handle.is_null() || payload_json.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let payload_slice = unsafe { std::slice::from_raw_parts(payload_json, len) };
    let payload: BundlePayload = match serde_json::from_slice(payload_slice) {
        Ok(p) => p,
        Err(e) => {
            record_error(Some(format!("invalid bundle payload: {e}")));
            return BoyodbStatus::InvalidArgument;
        }
    };
    status_from_result(db.db.apply_bundle(payload))
}

#[no_mangle]
pub extern "C" fn boyodb_validate_bundle(
    handle: *mut BoyodbHandle,
    payload_json: *const u8,
    len: usize,
) -> BoyodbStatus {
    if handle.is_null() || payload_json.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let payload_slice = unsafe { std::slice::from_raw_parts(payload_json, len) };
    let payload: BundlePayload = match serde_json::from_slice(payload_slice) {
        Ok(p) => p,
        Err(e) => {
            record_error(Some(format!("invalid bundle payload: {e}")));
            return BoyodbStatus::InvalidArgument;
        }
    };
    status_from_result(db.db.validate_bundle(&payload))
}

#[no_mangle]
pub extern "C" fn boyodb_last_error_message(out_buffer: *mut OwnedBuffer) -> BoyodbStatus {
    if out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let message = last_error().unwrap_or_default();
    unsafe {
        *out_buffer = OwnedBuffer::from_vec(message.into_bytes());
    }
    BoyodbStatus::Ok
}

#[no_mangle]
pub extern "C" fn boyodb_free_buffer(buffer: *mut OwnedBuffer) {
    if buffer.is_null() {
        return;
    }

    let buffer = unsafe { &mut *buffer };
    if let Some(dtor) = buffer.destructor {
        dtor(
            buffer.destructor_state,
            buffer.data,
            buffer.len,
            buffer.capacity,
        );
    }
    buffer.data = std::ptr::null_mut();
    buffer.len = 0;
    buffer.capacity = 0;
    buffer.destructor = None;
    buffer.destructor_state = std::ptr::null_mut();
}

#[no_mangle]
pub extern "C" fn boyodb_create_database(
    handle: *mut BoyodbHandle,
    name: *const c_char,
) -> BoyodbStatus {
    if handle.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let name = match cstr_to_str(name) {
        Ok(s) => s,
        Err(status) => return status,
    };
    status_from_result(db.db.create_database(&name))
}

#[no_mangle]
pub extern "C" fn boyodb_create_table(
    handle: *mut BoyodbHandle,
    database: *const c_char,
    table: *const c_char,
    schema_json: *const c_char,
) -> BoyodbStatus {
    if handle.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let database = match cstr_to_str(database) {
        Ok(s) => s,
        Err(status) => return status,
    };
    let table = match cstr_to_str(table) {
        Ok(s) => s,
        Err(status) => return status,
    };
    let schema = if schema_json.is_null() {
        None
    } else {
        match cstr_to_str(schema_json) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    };
    status_from_result(db.db.create_table(&database, &table, schema))
}

#[no_mangle]
pub extern "C" fn boyodb_healthcheck(handle: *mut BoyodbHandle) -> BoyodbStatus {
    if handle.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    status_from_result(db.db.health_check())
}

#[no_mangle]
pub extern "C" fn boyodb_checkpoint(handle: *mut BoyodbHandle) -> BoyodbStatus {
    if handle.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    status_from_result(db.db.checkpoint())
}

#[no_mangle]
pub extern "C" fn boyodb_list_databases(
    handle: *mut BoyodbHandle,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    match db.db.list_databases() {
        Ok(dbs) => match serde_json::to_vec(&dbs) {
            Ok(buf) => {
                unsafe { *out_buffer = OwnedBuffer::from_vec(buf) };
                record_error(None);
                BoyodbStatus::Ok
            }
            Err(_) => BoyodbStatus::Internal,
        },
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_list_tables(
    handle: *mut BoyodbHandle,
    database: *const c_char,
    has_database: bool,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let database_name = if has_database {
        match cstr_to_str(database) {
            Ok(s) => Some(s),
            Err(status) => return status,
        }
    } else {
        None
    };

    match db.db.list_tables(database_name.as_deref()) {
        Ok(tables) => match serde_json::to_vec(&tables) {
            Ok(buf) => {
                unsafe { *out_buffer = OwnedBuffer::from_vec(buf) };
                record_error(None);
                BoyodbStatus::Ok
            }
            Err(_) => BoyodbStatus::Internal,
        },
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_manifest(
    handle: *mut BoyodbHandle,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    match db.db.export_manifest() {
        Ok(buf) => {
            unsafe { *out_buffer = OwnedBuffer::from_vec(buf) };
            record_error(None);
            BoyodbStatus::Ok
        }
        Err(e) => status_from_error(e),
    }
}

#[no_mangle]
pub extern "C" fn boyodb_import_manifest(
    handle: *mut BoyodbHandle,
    payload: *const u8,
    len: usize,
    overwrite: bool,
) -> BoyodbStatus {
    if handle.is_null() || payload.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let data = unsafe { std::slice::from_raw_parts(payload, len) };
    status_from_result(db.db.import_manifest(data, overwrite))
}

#[no_mangle]
pub extern "C" fn boyodb_describe_table(
    handle: *mut BoyodbHandle,
    database: *const c_char,
    table: *const c_char,
    out_buffer: *mut OwnedBuffer,
) -> BoyodbStatus {
    if handle.is_null() || database.is_null() || table.is_null() || out_buffer.is_null() {
        return BoyodbStatus::InvalidArgument;
    }
    let db = unsafe { &mut *handle };
    let database = match cstr_to_str(database) {
        Ok(s) => s,
        Err(status) => return status,
    };
    let table = match cstr_to_str(table) {
        Ok(s) => s,
        Err(status) => return status,
    };

    match db.db.describe_table(&database, &table) {
        Ok(desc) => match serde_json::to_vec(&desc) {
            Ok(buf) => {
                unsafe { *out_buffer = OwnedBuffer::from_vec(buf) };
                record_error(None);
                BoyodbStatus::Ok
            }
            Err(_) => BoyodbStatus::Internal,
        },
        Err(e) => status_from_error(e),
    }
}

fn status_from_result(res: Result<(), EngineError>) -> BoyodbStatus {
    match res {
        Ok(_) => {
            record_error(None);
            BoyodbStatus::Ok
        }
        Err(e) => status_from_error(e),
    }
}

fn status_from_error(err: EngineError) -> BoyodbStatus {
    record_error(Some(err.to_string()));
    match err {
        EngineError::InvalidArgument(_) => BoyodbStatus::InvalidArgument,
        EngineError::NotFound(_) => BoyodbStatus::NotFound,
        EngineError::NotImplemented(_) => BoyodbStatus::NotImplemented,
        EngineError::Internal(_) => BoyodbStatus::Internal,
        EngineError::Io(_) => BoyodbStatus::Io,
        EngineError::Timeout(_) => BoyodbStatus::Timeout,
        EngineError::Remote(_) => BoyodbStatus::Internal,
        EngineError::Configuration(_) => BoyodbStatus::Internal,
    }
}

fn cstr_to_str(ptr: *const c_char) -> Result<String, BoyodbStatus> {
    if ptr.is_null() {
        record_error(Some("null string pointer".into()));
        return Err(BoyodbStatus::InvalidArgument);
    }
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| {
                record_error(Some("invalid utf-8 string".into()));
                BoyodbStatus::InvalidArgument
            })
    }
}

fn record_error(msg: Option<String>) {
    LAST_ERROR.with(|last| {
        *last.borrow_mut() = msg;
    });
}

fn last_error() -> Option<String> {
    LAST_ERROR.with(|last| last.borrow().clone())
}
