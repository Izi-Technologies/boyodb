#ifndef BOYODB_H
#define BOYODB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  BOYODB_STATUS_OK = 0,
  BOYODB_STATUS_INVALID_ARGUMENT = 1,
  BOYODB_STATUS_NOT_FOUND = 2,
  BOYODB_STATUS_INTERNAL = 3,
  BOYODB_STATUS_NOT_IMPLEMENTED = 4,
  BOYODB_STATUS_IO = 5,
  BOYODB_STATUS_TIMEOUT = 6
} boyodb_status;

typedef struct {
  uint8_t* data;
  size_t len;
  size_t capacity;
  void (*destructor)(void* state, uint8_t* data, size_t len, size_t capacity);
  void* destructor_state;
} boyodb_owned_buffer;

typedef struct boyodb_handle boyodb_handle;

typedef struct {
  const char* data_dir;
  const char* wal_dir;
  uint16_t shard_count;
  uint64_t cache_bytes;
  uint64_t wal_max_bytes;
  uint64_t wal_max_segments;
  bool allow_manifest_import;
} boyodb_open_options;

typedef struct {
  const char* sql;
  uint32_t timeout_millis;
} boyodb_query_request;

typedef struct {
  uint64_t max_bytes;
  bool has_max_bytes;
  uint64_t since_version;
  bool has_since_version;
  bool prefer_hot;
  uint64_t target_bytes_per_sec;
  bool has_target_bytes_per_sec;
} boyodb_bundle_request;

boyodb_status boyodb_open(const boyodb_open_options* opts, boyodb_handle** out_handle);
void boyodb_close(boyodb_handle* handle);

boyodb_status boyodb_ingest_ipc(boyodb_handle* handle, const uint8_t* payload, size_t len, uint64_t watermark_micros);
boyodb_status boyodb_ingest_ipc_v2(boyodb_handle* handle, const uint8_t* payload, size_t len, uint64_t watermark_micros, uint64_t shard_hint, bool has_shard_hint);
boyodb_status boyodb_ingest_ipc_v3(boyodb_handle* handle, const uint8_t* payload, size_t len, uint64_t watermark_micros, uint64_t shard_hint, bool has_shard_hint, const char* database, bool has_database, const char* table, bool has_table);
boyodb_status boyodb_query_ipc(boyodb_handle* handle, const boyodb_query_request* request, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_plan_bundle(boyodb_handle* handle, const boyodb_bundle_request* request, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_export_bundle(boyodb_handle* handle, const boyodb_bundle_request* request, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_apply_bundle(boyodb_handle* handle, const uint8_t* payload_json, size_t len);
boyodb_status boyodb_validate_bundle(boyodb_handle* handle, const uint8_t* payload_json, size_t len);
boyodb_status boyodb_create_database(boyodb_handle* handle, const char* name);
boyodb_status boyodb_create_table(boyodb_handle* handle, const char* database, const char* table, const char* schema_json);
boyodb_status boyodb_healthcheck(boyodb_handle* handle);
boyodb_status boyodb_checkpoint(boyodb_handle* handle);
boyodb_status boyodb_list_databases(boyodb_handle* handle, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_list_tables(boyodb_handle* handle, const char* database, bool has_database, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_manifest(boyodb_handle* handle, boyodb_owned_buffer* out_buffer);
boyodb_status boyodb_import_manifest(boyodb_handle* handle, const uint8_t* payload, size_t len, bool overwrite);
boyodb_status boyodb_last_error_message(boyodb_owned_buffer* out_buffer);

void boyodb_free_buffer(boyodb_owned_buffer* buffer);

#ifdef __cplusplus
}
#endif

#endif /* BOYODB_H */
