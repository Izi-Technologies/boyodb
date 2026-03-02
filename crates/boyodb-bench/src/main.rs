use anyhow::Result;
use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::engine::{Db, EngineConfig};
use clap::Parser;
use rand::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    rows: usize,

    #[arg(short, long, default_value_t = 10_000)]
    batch_size: usize,

    /// Number of query iterations when timing
    #[arg(long, default_value_t = 5)]
    query_iters: usize,

    /// Recompress Warm segments (e.g. zstd)
    #[arg(long)]
    tier_warm_compression: Option<String>,

    /// Recompress Cold segments (e.g. zstd)
    #[arg(long)]
    tier_cold_compression: Option<String>,

    /// Cache Hot segments in memory
    #[arg(long, value_parser = clap::value_parser!(bool), default_value_t = true)]
    cache_hot_segments: bool,

    /// Cache Warm segments in memory
    #[arg(long, value_parser = clap::value_parser!(bool), default_value_t = true)]
    cache_warm_segments: bool,

    /// Cache Cold segments in memory
    #[arg(long, value_parser = clap::value_parser!(bool), default_value_t = false)]
    cache_cold_segments: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    println!(
        "Starting Benchmark: {} rows, batch size {}, query iters {}",
        args.rows, args.batch_size, args.query_iters
    );

    let dir = tempdir()?;
    // Use EngineConfig::new and disable query cache
    let config = EngineConfig::new(dir.path(), 8)
        .with_query_cache_size(0)
        .with_query_cache_bytes(0)
        .with_query_cache_ttl_secs(0)
        .with_wal_max_bytes(256 * 1024 * 1024) // 256MB
        .with_batch_cache_bytes(4 * 1024 * 1024 * 1024) // 4GB
        .with_tier_warm_compression(args.tier_warm_compression.clone())
        .with_tier_cold_compression(args.tier_cold_compression.clone())
        .with_cache_hot_segments(args.cache_hot_segments)
        .with_cache_warm_segments(args.cache_warm_segments)
        .with_cache_cold_segments(args.cache_cold_segments);
    
    // Db::open is synchronous
    let db = Db::open(config)?;
    
    // Create Database
    db.create_database("default")?;
    
    // Create Table
    // define schema json
    let schema_json = r#"[
        {"name": "event_time", "type": "uint64", "nullable": false},
        {"name": "tenant_id", "type": "uint64", "nullable": false},
        {"name": "latency", "type": "float64", "nullable": false},
        {"name": "status_code", "type": "uint64", "nullable": false},
        {"name": "request_id", "type": "string", "nullable": false}
    ]"#;
    
    // create_table only takes 3 args
    db.create_table(
        "default",
        "logs",
        Some(schema_json.to_string()),
    )?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::UInt64, false),
        Field::new("tenant_id", DataType::UInt64, false),
        Field::new("latency", DataType::Float64, false),
        Field::new("status_code", DataType::UInt64, false),
        Field::new("request_id", DataType::Utf8, false),
    ]));

    // Generate Data
    println!("Generating and ingesting data...");
    let start_ingest = Instant::now();
    let mut rng = StdRng::seed_from_u64(42);
    
    let total_batches = (args.rows + args.batch_size - 1) / args.batch_size;
    let mut ingest_bytes: usize = 0;

    for b in 0..total_batches {
        let this_batch = if b == total_batches - 1 {
            // Handle remainder so total rows matches request
            args.rows - (b * args.batch_size)
        } else {
            args.batch_size
        };

        let mut event_times = Vec::with_capacity(this_batch);
        let mut tenant_ids = Vec::with_capacity(this_batch);
        let mut latencies = Vec::with_capacity(this_batch);
        let mut status_codes = Vec::with_capacity(this_batch);
        let mut request_ids = Vec::with_capacity(this_batch);
        
        // Simulate clustered status codes: 
        // Batches 0-8: STRICTLY 200 (Min=200, Max=200) -> Pruned when querying 500
        // Batch 9: STRICTLY 500 (Min=500, Max=500) -> Scanned
        let status_val = if b % 10 == 9 { 500 } else { 200 };
        
        // Ensure monotonically increasing event_time for time pruning
        let start_time = b as u64 * 1000 * args.batch_size as u64;

        for i in 0..this_batch {
            event_times.push(start_time + i as u64);
            tenant_ids.push(rng.gen_range(0..10));
            latencies.push(rng.gen::<f64>());
            status_codes.push(status_val);
            request_ids.push(format!("req-{}", rng.gen::<u64>()));
        }
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(event_times)),
                Arc::new(UInt64Array::from(tenant_ids)),
                Arc::new(Float64Array::from(latencies)),
                Arc::new(UInt64Array::from(status_codes)),
                Arc::new(StringArray::from(request_ids)),
            ],
        )?;
        
        // Write to IPC
        let mut payload = Vec::new();
        {
             let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut payload, &schema)?;
             writer.write(&batch)?;
             writer.finish()?;
        }
           ingest_bytes += payload.len();
        
        // Ingest Sync
        let ingest_batch = boyodb_core::engine::IngestBatch {
            payload_ipc: payload,
            watermark_micros: start_time, // rough approximation
            shard_override: None,
            database: Some("default".to_string()),
            table: Some("logs".to_string()),
        };
        db.ingest_ipc(ingest_batch)?;
    }
    
    let ingest_elapsed = start_ingest.elapsed();
    let ingest_rps = args.rows as f64 / ingest_elapsed.as_secs_f64();
    let ingest_mib = ingest_bytes as f64 / (1024.0 * 1024.0);
    let ingest_mibps = ingest_mib / ingest_elapsed.as_secs_f64();
    println!(
        "Ingest took {:?} | {:.1} rows/s | {:.1} MiB/s",
        ingest_elapsed, ingest_rps, ingest_mibps
    );
    
    // Validate count
    // Use Db::query with blocking API
    let resp = db.query(boyodb_core::engine::QueryRequest {
        sql: "SELECT count(*) FROM logs".to_string(),
        timeout_millis: 10000,
        collect_stats: false,
    }).map_err(|e| anyhow::anyhow!("query failed: {e}"))?;
    
    // Decode response IPC to print
    let cursor = std::io::Cursor::new(&resp.records_ipc);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    while let Some(batch) = reader.next() {
        let b = batch?;
        arrow::util::pretty::print_batches(&[b])?;
    }


    println!("\n--- Running Queries ---");

    // 1. Baseline: PK Filter
    let q1 = "SELECT count(*) FROM logs WHERE event_time > 5000 AND event_time < 6000";
    measure_query(&db, "Baseline (PK Filter)", q1, args.query_iters)?;

    // 2. New Index: Status Code 500
    // This should prune ~90% of segments (the ones strictly 200).
    let q2 = "SELECT count(*) FROM logs WHERE status_code = 500";
    measure_query(&db, "New Index (Clustered)", q2, args.query_iters)?;
    
    // 3. Control: Random Latency
    let q3 = "SELECT count(*) FROM logs WHERE latency > 0.95";
    measure_query(&db, "Control (Random Col)", q3, args.query_iters)?;
    
    Ok(())
}

fn measure_query(db: &Db, name: &str, sql: &str, iters: usize) -> Result<()> {
    print!("{}: ", name);
    // Warmup
    let _ = db.query(boyodb_core::engine::QueryRequest {
        sql: sql.to_string(),
        timeout_millis: 10000,
        collect_stats: false,
    }).map_err(|e| anyhow::anyhow!(e))?;

    let mut durs = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let _ = db.query(boyodb_core::engine::QueryRequest {
            sql: sql.to_string(),
            timeout_millis: 10000,
            collect_stats: false,
        }).map_err(|e| anyhow::anyhow!(e))?;
        durs.push(start.elapsed());
    }

    durs.sort();
    let p50 = percentile(&durs, 0.50);
    let p90 = percentile(&durs, 0.90);
    let p99 = percentile(&durs, 0.99);
    let avg = durs.iter().sum::<Duration>() / (durs.len() as u32);

    println!(
        "avg={:?} p50={:?} p90={:?} p99={:?}",
        avg, p50, p90, p99
    );
    Ok(())
}

fn percentile(durs: &[Duration], quantile: f64) -> Duration {
    if durs.is_empty() {
        return Duration::from_millis(0);
    }
    let idx = ((durs.len() as f64 - 1.0) * quantile).round() as usize;
    durs[idx]
}
