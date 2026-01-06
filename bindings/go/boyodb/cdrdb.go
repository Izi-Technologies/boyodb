package boyodb

/*
#cgo CFLAGS: -I../../../crates/boyodb-core/include
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -lboyodb_core
#include <stdlib.h>
#include "boyodb.h"
*/
import "C"
import (
	"encoding/json"
	"errors"
	"unsafe"
)

type Handle struct {
	ptr *C.boyodb_handle
}

type OpenOptions struct {
	DataDir    string
	WalDir     string
	ShardCount uint16
	CacheBytes uint64
	WalMaxBytes uint64
	WalMaxSegments uint64
	AllowManifestImport bool
}

type BundleRequest struct {
	MaxBytes            uint64
	HasMaxBytes         bool
	SinceVersion        uint64
	HasSinceVersion     bool
	PreferHot           bool
	TargetBytesPerSec   uint64
	HasTargetBytesPerSec bool
}

type TableMeta struct {
	Database   string  `json:"database"`
	Name       string  `json:"name"`
	SchemaJSON *string `json:"schema_json"`
}

func Open(opts OpenOptions) (*Handle, error) {
	dataDir := C.CString(opts.DataDir)
	defer C.free(unsafe.Pointer(dataDir))

	var walDir *C.char
	if opts.WalDir != "" {
		walDir = C.CString(opts.WalDir)
		defer C.free(unsafe.Pointer(walDir))
	}

	cOpts := C.boyodb_open_options{
		data_dir:         dataDir,
		wal_dir:          walDir,
		shard_count:      C.uint16_t(opts.ShardCount),
		cache_bytes:      C.uint64_t(opts.CacheBytes),
		wal_max_bytes:    C.uint64_t(opts.WalMaxBytes),
		wal_max_segments: C.uint64_t(opts.WalMaxSegments),
		allow_manifest_import: C.bool(opts.AllowManifestImport),
	}

	var handle *C.boyodb_handle
	if status := C.boyodb_open(&cOpts, &handle); status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}

	return &Handle{ptr: handle}, nil
}

func (h *Handle) Close() {
	if h == nil || h.ptr == nil {
		return
	}
	C.boyodb_close(h.ptr)
	h.ptr = nil
}

func (h *Handle) Ingest(ipc []byte, watermarkMicros uint64) error {
	return h.ingestInternal(ipc, watermarkMicros, 0, false, "", "")
}

// IngestWithShard routes via a caller-provided shard key (e.g., tenant hash) to reduce contention.
func (h *Handle) IngestWithShard(ipc []byte, watermarkMicros uint64, shardKey uint64) error {
	return h.ingestInternal(ipc, watermarkMicros, shardKey, true, "", "")
}

// IngestInto routes with optional shard key and attaches database/table metadata.
func (h *Handle) IngestInto(ipc []byte, watermarkMicros uint64, shardKey uint64, hasShardKey bool, database, table string) error {
	return h.ingestInternal(ipc, watermarkMicros, shardKey, hasShardKey, database, table)
}

func (h *Handle) ingestInternal(ipc []byte, watermarkMicros uint64, shardKey uint64, hasShardKey bool, database, table string) error {
	if h == nil || h.ptr == nil {
		return errors.New("boyodb: nil handle")
	}
	if len(ipc) == 0 {
		return errors.New("boyodb: empty ingest payload")
	}

	var cDb *C.char
	var cTable *C.char
	hasDb := C.bool(false)
	hasTable := C.bool(false)
	if database != "" {
		cDb = C.CString(database)
		defer C.free(unsafe.Pointer(cDb))
		hasDb = C.bool(true)
	}
	if table != "" {
		cTable = C.CString(table)
		defer C.free(unsafe.Pointer(cTable))
		hasTable = C.bool(true)
	}

	status := C.boyodb_ingest_ipc_v3(
		h.ptr,
		(*C.uchar)(unsafe.Pointer(&ipc[0])),
		C.size_t(len(ipc)),
		C.uint64_t(watermarkMicros),
		C.uint64_t(shardKey),
		C.bool(hasShardKey),
		cDb,
		hasDb,
		cTable,
		hasTable,
	)
	return statusToError(status)
}

func (h *Handle) Query(sql string, timeoutMillis uint32) ([]byte, error) {
	if h == nil || h.ptr == nil {
		return nil, errors.New("boyodb: nil handle")
	}

	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	req := C.boyodb_query_request{
		sql:            cSQL,
		timeout_millis: C.uint32_t(timeoutMillis),
	}

	var buf C.boyodb_owned_buffer
	status := C.boyodb_query_ipc(h.ptr, &req, &buf)
	if status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}
	defer C.boyodb_free_buffer(&buf)

	goBytes := C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	return goBytes, nil
}

func (h *Handle) PlanBundle(req BundleRequest) ([]byte, error) {
	if h == nil || h.ptr == nil {
		return nil, errors.New("boyodb: nil handle")
	}

	cReq := C.boyodb_bundle_request{
		max_bytes:                 C.uint64_t(req.MaxBytes),
		has_max_bytes:             C.bool(req.HasMaxBytes),
		since_version:             C.uint64_t(req.SinceVersion),
		has_since_version:         C.bool(req.HasSinceVersion),
		prefer_hot:                C.bool(req.PreferHot),
		target_bytes_per_sec:      C.uint64_t(req.TargetBytesPerSec),
		has_target_bytes_per_sec:  C.bool(req.HasTargetBytesPerSec),
	}

	var buf C.boyodb_owned_buffer
	status := C.boyodb_plan_bundle(h.ptr, &cReq, &buf)
	if status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}
	defer C.boyodb_free_buffer(&buf)

	goBytes := C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	return goBytes, nil
}

func (h *Handle) CreateDatabase(name string) error {
	if h == nil || h.ptr == nil {
		return errors.New("boyodb: nil handle")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return statusToError(C.boyodb_create_database(h.ptr, cName))
}

func (h *Handle) CreateTable(database, table, schemaJSON string) error {
	if h == nil || h.ptr == nil {
		return errors.New("boyodb: nil handle")
	}
	cDb := C.CString(database)
	defer C.free(unsafe.Pointer(cDb))
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	var cSchema *C.char
	if schemaJSON != "" {
		cSchema = C.CString(schemaJSON)
		defer C.free(unsafe.Pointer(cSchema))
	}
	return statusToError(C.boyodb_create_table(h.ptr, cDb, cTable, cSchema))
}

func (h *Handle) HealthCheck() error {
	if h == nil || h.ptr == nil {
		return errors.New("boyodb: nil handle")
	}
	return statusToError(C.boyodb_healthcheck(h.ptr))
}

func (h *Handle) ListDatabases() ([]string, error) {
	if h == nil || h.ptr == nil {
		return nil, errors.New("boyodb: nil handle")
	}
	var buf C.boyodb_owned_buffer
	status := C.boyodb_list_databases(h.ptr, &buf)
	if status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}
	defer C.boyodb_free_buffer(&buf)
	raw := C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handle) ListTables(database string) ([]TableMeta, error) {
	if h == nil || h.ptr == nil {
		return nil, errors.New("boyodb: nil handle")
	}

	var cDb *C.char
	hasDb := C.bool(false)
	if database != "" {
		cDb = C.CString(database)
		defer C.free(unsafe.Pointer(cDb))
		hasDb = C.bool(true)
	}

	var buf C.boyodb_owned_buffer
	status := C.boyodb_list_tables(h.ptr, cDb, hasDb, &buf)
	if status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}
	defer C.boyodb_free_buffer(&buf)

	raw := C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	var out []TableMeta
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handle) Manifest() ([]byte, error) {
	if h == nil || h.ptr == nil {
		return nil, errors.New("boyodb: nil handle")
	}
	var buf C.boyodb_owned_buffer
	status := C.boyodb_manifest(h.ptr, &buf)
	if status != C.BOYODB_STATUS_OK {
		return nil, statusToError(status)
	}
	defer C.boyodb_free_buffer(&buf)
	return C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len)), nil
}

func (h *Handle) ImportManifest(manifest []byte, overwrite bool) error {
	if h == nil || h.ptr == nil {
		return errors.New("boyodb: nil handle")
	}
	if len(manifest) == 0 {
		return errors.New("boyodb: empty manifest payload")
	}
	status := C.boyodb_import_manifest(
		h.ptr,
		(*C.uchar)(unsafe.Pointer(&manifest[0])),
		C.size_t(len(manifest)),
		C.bool(overwrite),
	)
	return statusToError(status)
}

func statusToError(status C.boyodb_status) error {
	switch status {
	case C.BOYODB_STATUS_OK:
		return nil
	case C.BOYODB_STATUS_INVALID_ARGUMENT:
		return errors.New("boyodb: invalid argument")
	case C.BOYODB_STATUS_NOT_FOUND:
		return errors.New("boyodb: not found")
	case C.BOYODB_STATUS_NOT_IMPLEMENTED:
		return errors.New("boyodb: not implemented")
	case C.BOYODB_STATUS_IO:
		return errors.New("boyodb: io error")
	default:
		return errors.New("boyodb: internal error")
	}
}
