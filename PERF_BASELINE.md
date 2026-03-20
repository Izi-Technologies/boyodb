# Performance Baseline (boyodb-bench)

Run command:
```
make bench-core
```
(Release build; runs `boyodb-bench` with rows=1_000_000, batch-size=10_000, query-iters=5.)

### Tiering knobs (optional)
Use these flags to mirror production tiering in perf runs:

```
cargo run -p boyodb-bench --release -- \
  --tier-warm-compression zstd \
  --tier-cold-compression zstd \
  --cache-hot-segments true \
  --cache-warm-segments true \
  --cache-cold-segments false
```

## Latest run
- Date: 2026-03-16
- Host: local (macOS)
- Command: `./target/release/boyodb-bench --rows 100000 --batch-size 10000 --query-iters 5`
- Ingest: 100,000 rows in 247ms | ~403k rows/s | ~23.1 MiB/s
- Query latency (5 iters each):
  - Baseline (PK Filter): avg 6.87µs | p50 6.38µs | p90 9.00µs | p99 9.00µs
  - Clustered (status_code=500): avg 6.17µs | p50 6.04µs | p90 6.88µs | p99 6.88µs
  - Control (latency > 0.95): avg 13.98µs | p50 13.58µs | p90 15.92µs | p99 15.92µs

## Network IPC Streaming (Zero-Copy)
- Date: 2026-03-20
- Host: local (macOS)
- Command: `cargo bench -p boyodb-core`
- Distributed Query Payload Parsing (100,000 rows / 2.7 MB):
  - Legacy Base64: 1.50 ms | 1.87 GiB/s
  - Zero-Copy Binary: 0.19 ms | 14.7 GiB/s  (8x faster)
- Distributed Query Payload Parsing (10,000 rows / 270 KB):
  - Legacy Base64: 117 µs | 2.33 GiB/s
  - Zero-Copy Binary: 6.31 µs | 43.2 GiB/s  (18x faster)

## Previous run (1M rows)
- Date: 2025-12-30
- Host: local (macOS)
- Command: `make bench-core`
- Ingest: 1,000,000 rows in 1.7462s | ~572.7k rows/s | ~32.8 MiB/s
- Query latency (5 iters each):
  - Baseline (PK Filter): avg 14.55ms | p50 15.08ms | p90 15.94ms | p99 15.94ms
  - Clustered (status_code=500): avg 15.44ms | p50 15.74ms | p90 15.94ms | p99 15.94ms
  - Control (latency > 0.95): avg 15.21ms | p50 16.00ms | p90 16.03ms | p99 16.03ms

## Notes
- Batch size 10k was used; adjust to your workload (e.g., 64k/128k) and record results.
- Keep `RUST_LOG=info`, release build, and avoid request logging during perf runs.
- Place data_dir on SSD/NVMe; isolate WAL if possible for steadier ingest.
