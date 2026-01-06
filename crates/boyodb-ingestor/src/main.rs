//! CDR Ingestor for Orange CDR files
//!
//! Watches SFTP directory for new CDR files, decodes them from ASN.1 BER format,
//! and ingests into BOYODB via TCP connection.
//!
//! Supports:
//! - PGW-CDR (data/packet records) - B*.dat, DATA-*, *EPC*.dat files
//! - MSC-CDR (voice call records) - c*.dat, VOICE-* files
//! - SMS-CDR (SMS records) - SMS-*, SMC*.dat files
//!
//! Features concurrent processing for high throughput.

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    ArrayRef, Int64Builder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

// Thread-local connection pool for BOYODB connections
thread_local! {
    static BOYODB_CONNECTION: RefCell<Option<TcpStream>> = const { RefCell::new(None) };
}

#[derive(Parser, Debug)]
#[command(name = "cdr-ingestor")]
#[command(about = "CDR file ingestor for BOYODB")]
struct Args {
    /// Directory to watch for CDR files
    #[arg(short, long)]
    watch_dir: PathBuf,

    /// BOYODB server address
    #[arg(short = 'H', long, default_value = "127.0.0.1:8765")]
    host: String,

    /// Database name
    #[arg(short, long, default_value = "orange")]
    database: String,

    /// Table name
    #[arg(short, long, default_value = "pgw_cdr")]
    table: String,

    /// Batch size for ingestion
    #[arg(short, long, default_value_t = 1000)]
    batch_size: usize,

    /// Process existing files on startup
    #[arg(long, default_value_t = true)]
    process_existing: bool,

    /// Directory to move processed files
    #[arg(long)]
    processed_dir: Option<PathBuf>,

    /// Run once (process existing files and exit)
    #[arg(long, default_value_t = false)]
    once: bool,

    /// File prefix filter (only process files starting with this prefix)
    /// Can be specified multiple times for multiple prefixes
    #[arg(long)]
    prefix: Vec<String>,

    /// Number of concurrent files to process (default: 8)
    #[arg(short = 'j', long, default_value_t = 8)]
    concurrency: usize,
}

/// Decoded PGW-CDR record
#[derive(Debug, Clone, Default)]
struct PgwCdr {
    record_type: i64,
    served_imsi: String,
    served_imei: String,
    served_msisdn: String,
    pgw_address: String,
    charging_id: u64,
    apn: String,
    pdp_type: String,
    served_pdp_address: String,
    record_opening_time: i64,  // timestamp millis
    duration: u64,
    cause_for_record_closing: i64,
    node_id: String,
    record_sequence_number: u64,
    local_sequence_number: u64,
    data_volume_uplink: u64,
    data_volume_downlink: u64,
    rat_type: i64,
    charging_characteristics: String,
    mcc_mnc: String,
    source_file: String,
}

/// Decoded MSC-CDR (voice call) record
#[derive(Debug, Clone, Default)]
struct MscCdr {
    record_type: i64,           // MOC=0, MTC=1, etc.
    calling_number: String,     // A-party number
    called_number: String,      // B-party number
    served_imsi: String,
    served_imei: String,
    served_msisdn: String,
    recording_entity: String,   // MSC ID
    msc_address: String,
    call_reference: u64,
    call_duration: u64,         // seconds
    answer_time: i64,           // timestamp millis
    release_time: i64,          // timestamp millis
    seizure_time: i64,          // timestamp millis
    cause_for_termination: i64,
    basic_service: String,      // teleservice/bearer code
    location_area_code: String,
    cell_id: String,
    sequence_number: u64,
    mcc_mnc: String,
    source_file: String,
}

/// Decoded SMS-CDR record (3GPP TS 32.205 SMS record)
#[derive(Debug, Clone, Default)]
struct SmsCdr {
    record_type: i64,           // SMS-MO=0, SMS-MT=1
    originating_number: String, // SMS sender
    destination_number: String, // SMS recipient
    served_imsi: String,
    served_imei: String,
    served_msisdn: String,
    recording_entity: String,   // MSC/SMSC ID
    smsc_address: String,       // SMS Center address
    message_reference: u64,
    submit_time: i64,           // timestamp millis
    delivery_time: i64,         // timestamp millis
    message_size: u64,          // bytes
    message_class: String,
    delivery_status: i64,
    cause_for_termination: i64,
    location_area_code: String,
    cell_id: String,
    sequence_number: u64,
    mcc_mnc: String,
    source_file: String,
}

/// ASN.1 BER tag types
#[allow(dead_code)]
const TAG_CONTEXT_PRIMITIVE: u8 = 0x80;
#[allow(dead_code)]
const TAG_CONTEXT_CONSTRUCTED: u8 = 0xA0;
const PGW_CDR_TAG: u8 = 0x4F;  // Context [79] for PGW-CDR

/// CDR file type classification
#[derive(Debug, Clone, Copy, PartialEq)]
enum CdrType {
    /// Data/Packet CDRs (PGW-CDR) - stored in pgw_cdr table
    Data,
    /// Voice call CDRs (MSC-CDR) - stored in voice_cdr table
    Voice,
    /// SMS CDRs - stored in sms_cdr table
    Sms,
}

/// Determine CDR type from filename
fn classify_cdr_file(filename: &str) -> CdrType {
    let upper = filename.to_uppercase();

    // SMS CDRs: SMS-*, SMC* files
    if upper.starts_with("SMS-") || upper.starts_with("SMC") {
        return CdrType::Sms;
    }

    // Voice CDRs: VOICE-*, c* (but not cdr*) files
    if upper.starts_with("VOICE-") || (filename.starts_with('c') && !filename.starts_with("cdr")) {
        return CdrType::Voice;
    }

    // Data CDRs: Everything else - B*, DATA-*, *EPC*, etc.
    // This covers: BUKVEPC*, BZNVEPC*, DATA-*, LUBEPC*, MBUEPC*, MTCVEPC*, KISEPC*, B*.dat
    CdrType::Data
}

/// Get priority value for sorting (lower = higher priority)
/// Voice = 0 (highest), SMS = 1, Data = 2 (lowest)
fn get_cdr_priority(filename: &str) -> u8 {
    match classify_cdr_file(filename) {
        CdrType::Voice => 0,  // Highest priority
        CdrType::Sms => 1,    // Second priority
        CdrType::Data => 2,   // Lowest priority
    }
}

/// Check if a filename matches any of the specified prefixes
fn matches_prefix(filename: &str, prefixes: &[String]) -> bool {
    if prefixes.is_empty() {
        return true; // No prefix filter = match all
    }
    prefixes.iter().any(|p| filename.starts_with(p))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    info!("CDR Ingestor starting");
    info!("Watch directory: {:?}", args.watch_dir);
    info!("BOYODB server: {}", args.host);
    info!("Database: {}, Table: {}", args.database, args.table);
    info!("Concurrency: {} parallel files", args.concurrency);
    if !args.prefix.is_empty() {
        info!("File prefix filter: {:?}", args.prefix);
    } else {
        info!("No prefix filter - processing all CDR files");
    }

    // Ensure watch directory exists
    if !args.watch_dir.exists() {
        return Err(anyhow!("Watch directory does not exist: {:?}", args.watch_dir));
    }

    // Create processed directory if specified
    if let Some(ref processed_dir) = args.processed_dir {
        fs::create_dir_all(processed_dir)?;
    }

    // First, ensure database and table exist
    ensure_schema(&args.host, &args.database, &args.table)?;

    let processed_files: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));

    // Process existing files if requested
    if args.process_existing {
        info!("Processing existing files with {} concurrent workers...", args.concurrency);
        process_existing_files_concurrent(&args, &processed_files).await?;
    }

    if args.once {
        info!("--once flag set, exiting after processing existing files");
        return Ok(());
    }

    // Continuous processing mode - rescan every 60 seconds to pick up new files with priority
    info!("Entering continuous processing mode with priority rescans...");

    loop {
        // Rescan for new files with priority ordering
        info!("Rescanning for new files...");
        process_existing_files_concurrent(&args, &processed_files).await?;

        // Wait before next rescan
        info!("Sleeping 60 seconds before next rescan...");
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Clean up processed_files set periodically to prevent memory growth
        // Only keep entries for files that still exist
        {
            let mut processed = processed_files
                .lock()
                .expect("processed_files mutex poisoned");
            let before_count = processed.len();
            processed.retain(|p| p.exists());
            let removed = before_count - processed.len();
            if removed > 0 {
                debug!("Cleaned up {} entries from processed files cache", removed);
            }
        }
    }
}

/// Minimal args struct for process_file to avoid cloning full Args
struct ProcessArgs {
    host: String,
    database: String,
    batch_size: usize,
}

/// Trait for args that can be used for file processing
trait FileProcessArgs {
    fn host(&self) -> &str;
    fn database(&self) -> &str;
    fn batch_size(&self) -> usize;
}

impl FileProcessArgs for Args {
    fn host(&self) -> &str { &self.host }
    fn database(&self) -> &str { &self.database }
    fn batch_size(&self) -> usize { self.batch_size }
}

impl FileProcessArgs for ProcessArgs {
    fn host(&self) -> &str { &self.host }
    fn database(&self) -> &str { &self.database }
    fn batch_size(&self) -> usize { self.batch_size }
}

fn ensure_schema(host: &str, database: &str, _table: &str) -> Result<()> {
    info!("Ensuring database '{}' and tables exist...", database);

    // Create database
    let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS {}", database);
    execute_sql(host, &create_db_sql)?;

    // Create PGW-CDR table (data records)
    let create_pgw_table_sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.pgw_cdr (
            record_type INT,
            served_imsi VARCHAR,
            served_imei VARCHAR,
            served_msisdn VARCHAR,
            pgw_address VARCHAR,
            charging_id BIGINT,
            apn VARCHAR,
            pdp_type VARCHAR,
            served_pdp_address VARCHAR,
            record_opening_time TIMESTAMP,
            duration BIGINT,
            cause_for_record_closing INT,
            node_id VARCHAR,
            record_sequence_number BIGINT,
            local_sequence_number BIGINT,
            data_volume_uplink BIGINT,
            data_volume_downlink BIGINT,
            rat_type INT,
            charging_characteristics VARCHAR,
            mcc_mnc VARCHAR,
            source_file VARCHAR
        )"#,
        database
    );
    execute_sql(host, &create_pgw_table_sql)?;

    // Create MSC-CDR table (voice call records)
    let create_msc_table_sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.voice_cdr (
            record_type INT,
            calling_number VARCHAR,
            called_number VARCHAR,
            served_imsi VARCHAR,
            served_imei VARCHAR,
            served_msisdn VARCHAR,
            recording_entity VARCHAR,
            msc_address VARCHAR,
            call_reference BIGINT,
            call_duration BIGINT,
            answer_time TIMESTAMP,
            release_time TIMESTAMP,
            seizure_time TIMESTAMP,
            cause_for_termination INT,
            basic_service VARCHAR,
            location_area_code VARCHAR,
            cell_id VARCHAR,
            sequence_number BIGINT,
            mcc_mnc VARCHAR,
            source_file VARCHAR
        )"#,
        database
    );
    execute_sql(host, &create_msc_table_sql)?;

    // Create SMS-CDR table (SMS records)
    let create_sms_table_sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.sms_cdr (
            record_type INT,
            originating_number VARCHAR,
            destination_number VARCHAR,
            served_imsi VARCHAR,
            served_imei VARCHAR,
            served_msisdn VARCHAR,
            recording_entity VARCHAR,
            smsc_address VARCHAR,
            message_reference BIGINT,
            submit_time TIMESTAMP,
            delivery_time TIMESTAMP,
            message_size BIGINT,
            message_class VARCHAR,
            delivery_status INT,
            cause_for_termination INT,
            location_area_code VARCHAR,
            cell_id VARCHAR,
            sequence_number BIGINT,
            mcc_mnc VARCHAR,
            source_file VARCHAR
        )"#,
        database
    );
    execute_sql(host, &create_sms_table_sql)?;

    info!("Schema ready (pgw_cdr, voice_cdr, and sms_cdr tables)");
    Ok(())
}

fn execute_sql(host: &str, sql: &str) -> Result<String> {
    let mut stream = TcpStream::connect(host)
        .with_context(|| format!("Failed to connect to BOYODB at {}", host))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    // BOYODB protocol: send SQL as length-prefixed message
    let sql_bytes = sql.as_bytes();
    let len = sql_bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(sql_bytes)?;
    stream.flush()?;

    // Read response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let response_len = u32::from_be_bytes(len_buf) as usize;

    let mut response = vec![0u8; response_len];
    stream.read_exact(&mut response)?;

    Ok(String::from_utf8_lossy(&response).to_string())
}

/// Process existing files concurrently using tokio tasks
/// Files are sorted by priority: Voice (highest) > SMS > Data (lowest)
/// Uses HashSet to prevent duplicate processing
async fn process_existing_files_concurrent(args: &Args, processed_files: &Arc<Mutex<HashSet<PathBuf>>>) -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinSet;

    // Use HashSet to collect files and eliminate duplicates
    let mut file_set: HashSet<PathBuf> = HashSet::new();
    let already_processed: HashSet<PathBuf> = {
        processed_files
            .lock()
            .expect("processed_files mutex poisoned")
            .clone()
    };

    for entry in WalkDir::new(&args.watch_dir)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "dat"))
        .filter(|e| {
            // Apply prefix filter if specified
            e.path()
                .file_name()
                .map(|f| matches_prefix(&f.to_string_lossy(), &args.prefix))
                .unwrap_or(false)
        })
    {
        let path = entry.path().to_path_buf();
        if !already_processed.contains(&path) {
            file_set.insert(path);
        }
    }

    // Convert to Vec and sort by priority (Voice > SMS > Data), then by filename
    let mut files: Vec<PathBuf> = file_set.into_iter().collect();
    files.sort_by(|a, b| {
        let a_name = a.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
        let b_name = b.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
        let a_priority = get_cdr_priority(&a_name);
        let b_priority = get_cdr_priority(&b_name);
        // First sort by priority (lower = higher priority), then by filename
        a_priority.cmp(&b_priority).then_with(|| a_name.cmp(&b_name))
    });

    let total_files = files.len();

    // Count files by priority for logging
    let voice_count = files.iter().filter(|f| {
        f.file_name().map(|n| get_cdr_priority(&n.to_string_lossy()) == 0).unwrap_or(false)
    }).count();
    let sms_count = files.iter().filter(|f| {
        f.file_name().map(|n| get_cdr_priority(&n.to_string_lossy()) == 1).unwrap_or(false)
    }).count();
    let data_count = total_files - voice_count - sms_count;

    info!("Priority queue: {} Voice (highest), {} SMS, {} Data (lowest)", voice_count, sms_count, data_count);

    if !args.prefix.is_empty() {
        info!("Found {} CDR files matching prefixes: {:?}", total_files, args.prefix);
    } else {
        info!("Found {} existing CDR files", total_files);
    }

    if total_files == 0 {
        info!("No files to process");
        return Ok(());
    }

    // Shared counters for progress tracking
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let processed_count = Arc::new(AtomicUsize::new(0));

    // Create semaphore for concurrency control
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    // Use JoinSet for concurrent processing
    let mut join_set = JoinSet::new();

    let start_time = std::time::Instant::now();

    for path in files {
        let sem = semaphore.clone();
        let processed_files_clone = processed_files.clone();
        let success_count_clone = success_count.clone();
        let error_count_clone = error_count.clone();
        let processed_count_clone = processed_count.clone();
        let path_for_log = path.clone();

        let process_args = ProcessArgs {
            host: args.host.clone(),
            database: args.database.clone(),
            batch_size: args.batch_size,
        };
        let processed_dir = args.processed_dir.clone();

        join_set.spawn(async move {
            let permit = match sem.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    warn!("Semaphore closed while processing {:?}: {}", path, e);
                    return;
                }
            };
            let _permit = permit;

            let result = match tokio::task::spawn_blocking(move || {
                let res = process_file(&path, &process_args);
                (path, res, processed_dir)
            })
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    warn!("Join error while processing {:?}: {}", path_for_log, e);
                    return;
                }
            };

            let (path, res, processed_dir) = result;
            let count = processed_count_clone.fetch_add(1, Ordering::Relaxed) + 1;

            match res {
                Ok(_) => {
                    success_count_clone.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut processed = processed_files_clone
                            .lock()
                            .expect("processed_files mutex poisoned");
                        processed.insert(path.clone());
                    }
                    move_processed_file(&path, &processed_dir);
                }
                Err(e) => {
                    error_count_clone.fetch_add(1, Ordering::Relaxed);
                    warn!("Failed to process {:?}: {}", path, e);
                }
            }

            // Log progress every 100 files
            if count % 100 == 0 {
                info!("Progress: {}/{} files processed", count, total_files);
            }
        });
    }

    // Wait for all tasks to complete
    while join_set.join_next().await.is_some() {}

    let elapsed = start_time.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let files_per_sec = if elapsed.as_secs() > 0 {
        total_files as f64 / elapsed.as_secs_f64()
    } else {
        total_files as f64
    };

    info!(
        "Processed {} files successfully, {} errors in {:.1}s ({:.1} files/sec)",
        success, errors, elapsed.as_secs_f64(), files_per_sec
    );
    Ok(())
}

fn process_file<A: FileProcessArgs>(path: &Path, args: &A) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    let source_file = path.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    // Classify CDR type by filename pattern
    let cdr_type = classify_cdr_file(&source_file);

    match cdr_type {
        CdrType::Voice => {
            // Process voice CDR (MSC-CDR) - VOICE-*, c* files
            let cdrs = decode_msc_cdr_file(&data, &source_file)?;

            if cdrs.is_empty() {
                warn!("No voice CDRs decoded from {:?}", path);
                return Ok(());
            }

            info!("Decoded {} voice CDRs from {:?}", cdrs.len(), path);

            // Convert to Arrow and ingest
            for chunk in cdrs.chunks(args.batch_size()) {
                let batch = msc_cdrs_to_arrow_batch(chunk)?;
                ingest_batch(args.host(), args.database(), "voice_cdr", batch)?;
            }
        }
        CdrType::Sms => {
            // Process SMS CDR - SMS-*, SMC* files
            // SMS records use similar format to voice CDRs (MSC-CDR with SMS-specific fields)
            let cdrs = decode_sms_cdr_file(&data, &source_file)?;

            if cdrs.is_empty() {
                warn!("No SMS CDRs decoded from {:?}", path);
                return Ok(());
            }

            info!("Decoded {} SMS CDRs from {:?}", cdrs.len(), path);

            // Convert to Arrow and ingest
            for chunk in cdrs.chunks(args.batch_size()) {
                let batch = sms_cdrs_to_arrow_batch(chunk)?;
                ingest_batch(args.host(), args.database(), "sms_cdr", batch)?;
            }
        }
        CdrType::Data => {
            // Process data CDR (PGW-CDR) - B*, DATA-*, *EPC* files
            let cdrs = decode_cdr_file(&data, &source_file)?;

            if cdrs.is_empty() {
                warn!("No data CDRs decoded from {:?}", path);
                return Ok(());
            }

            info!("Decoded {} data CDRs from {:?}", cdrs.len(), path);

            // Convert to Arrow and ingest
            for chunk in cdrs.chunks(args.batch_size()) {
                let batch = cdrs_to_arrow_batch(chunk)?;
                ingest_batch(args.host(), args.database(), "pgw_cdr", batch)?;
            }
        }
    }

    Ok(())
}

fn move_processed_file(path: &Path, processed_dir: &Option<PathBuf>) {
    if let Some(ref dir) = processed_dir {
        if let Some(filename) = path.file_name() {
            let dest = dir.join(filename);
            if let Err(e) = fs::rename(path, &dest) {
                warn!("Failed to move {:?} to {:?}: {}", path, dest, e);
            }
        }
    }
}

/// Decode a CDR file containing multiple PGW-CDR records
fn decode_cdr_file(data: &[u8], source_file: &str) -> Result<Vec<PgwCdr>> {
    let mut cdrs = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        // Look for PGW-CDR tag (BF 4F = context [79] constructed)
        if offset + 2 > data.len() {
            break;
        }

        if data[offset] == 0xBF && data[offset + 1] == PGW_CDR_TAG {
            match decode_pgw_cdr(&data[offset..], source_file) {
                Ok((cdr, consumed)) => {
                    cdrs.push(cdr);
                    offset += consumed;
                }
                Err(e) => {
                    // Try to skip to next record
                    offset += 1;
                    if cdrs.is_empty() {
                        // Only warn if we haven't decoded any records yet
                        warn!("Failed to decode CDR at offset {}: {}", offset, e);
                    }
                }
            }
        } else {
            offset += 1;
        }
    }

    Ok(cdrs)
}

/// Decode a single PGW-CDR record
fn decode_pgw_cdr(data: &[u8], source_file: &str) -> Result<(PgwCdr, usize)> {
    let mut cdr = PgwCdr::default();
    cdr.source_file = source_file.to_string();

    // Skip tag bytes (BF 4F)
    let mut pos = 2;

    // Read length
    let (length, len_bytes) = read_ber_length(&data[pos..])?;
    let is_indefinite = length == 0 && data.get(pos) == Some(&0x80);
    pos += len_bytes;

    // Calculate end position
    let end_pos = if is_indefinite {
        // Find EOC marker for indefinite-length encoding
        pos + find_indefinite_end(&data[pos..]).unwrap_or(data.len() - pos)
    } else {
        pos + length
    };

    // Parse fields within the record
    while pos < end_pos && pos < data.len() {
        // Check for EOC marker if we're in indefinite mode
        if is_indefinite && pos + 1 < data.len() && data[pos] == 0x00 && data[pos + 1] == 0x00 {
            break;
        }

        let tag = data[pos];
        pos += 1;

        if pos >= data.len() {
            break;
        }

        let (field_len, len_bytes) = read_ber_length(&data[pos..])?;
        let field_indefinite = field_len == 0 && data.get(pos) == Some(&0x80);
        pos += len_bytes;

        // Calculate actual field length for indefinite fields
        let actual_field_len = if field_indefinite {
            find_indefinite_end(&data[pos..]).unwrap_or(0)
        } else {
            field_len
        };

        if pos + actual_field_len > data.len() {
            break;
        }

        let field_data = &data[pos..pos + actual_field_len];

        // Decode based on tag (simplified - real implementation would handle all fields)
        match tag {
            0x80 => cdr.record_type = decode_integer(field_data) as i64,
            0x83 => cdr.served_imsi = decode_tbcd(field_data),
            0x84 => cdr.pgw_address = decode_ip_address(field_data),
            0x85 => cdr.charging_id = decode_integer(field_data) as u64,
            0x87 => cdr.apn = decode_string(field_data),
            0x88 => cdr.pdp_type = format!("{:02X}", field_data.first().unwrap_or(&0)),
            0x91 => cdr.cause_for_record_closing = decode_integer(field_data) as i64,
            0x92 => cdr.node_id = decode_string(field_data),
            0x94 => cdr.record_sequence_number = decode_integer(field_data) as u64,
            0x95 => cdr.local_sequence_number = decode_integer(field_data) as u64,
            0x96 => cdr.served_msisdn = decode_tbcd(field_data),
            0x97 => {
                // Data volumes (uplink/downlink)
                if field_len >= 2 {
                    cdr.data_volume_uplink = decode_integer(&field_data[..field_len/2]) as u64;
                    cdr.data_volume_downlink = decode_integer(&field_data[field_len/2..]) as u64;
                }
            }
            0x98 => cdr.rat_type = decode_integer(field_data) as i64,
            0x9B => cdr.mcc_mnc = decode_tbcd(field_data),
            0x9D => {
                // Served PDP Address
                cdr.served_pdp_address = decode_ip_address(field_data);
            }
            0xA9 | 0xAC => {
                // Record opening time / duration (constructed)
                if let Some(ts) = decode_timestamp_field(field_data) {
                    if tag == 0xA9 {
                        // This could be duration
                        cdr.duration = ts as u64;
                    }
                }
            }
            0x9F => {
                // Extended tag handling
                if field_len >= 2 {
                    match field_data[0] {
                        0x20 => {
                            // Record opening time
                            if let Some(ts) = parse_timestamp(&field_data[1..]) {
                                cdr.record_opening_time = ts;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                // Skip unknown tags
            }
        }

        pos += actual_field_len;
    }

    // Calculate total bytes consumed
    let total_consumed = if is_indefinite {
        end_pos // For indefinite, end_pos already accounts for EOC
    } else {
        2 + len_bytes + length
    };

    Ok((cdr, total_consumed.min(data.len())))
}

/// Read BER length encoding
/// Returns (content_length, bytes_consumed_for_length)
/// For indefinite length, returns (0, 1) - caller must find EOC marker
fn read_ber_length(data: &[u8]) -> Result<(usize, usize)> {
    if data.is_empty() {
        return Err(anyhow!("Empty data for length"));
    }

    let first = data[0];
    if first < 0x80 {
        // Short form: length is the byte value itself
        Ok((first as usize, 1))
    } else if first == 0x80 {
        // Indefinite length - content ends with EOC (0x00, 0x00)
        // Return 0 as length signal, caller must handle by finding EOC
        Ok((0, 1))
    } else {
        // Long form: first byte indicates number of following length octets
        let num_octets = (first & 0x7F) as usize;
        if num_octets > 4 || data.len() < 1 + num_octets {
            return Err(anyhow!("Invalid length encoding"));
        }

        let mut length: usize = 0;
        for i in 0..num_octets {
            length = (length << 8) | (data[1 + i] as usize);
        }

        Ok((length, 1 + num_octets))
    }
}

/// Find the end of an indefinite-length BER construct
/// Returns the position right after the EOC marker (0x00, 0x00)
fn find_indefinite_end(data: &[u8]) -> Option<usize> {
    let mut pos = 0;
    let mut depth = 1; // We start inside one indefinite construct

    while pos + 1 < data.len() {
        // Check for EOC marker
        if data[pos] == 0x00 && data[pos + 1] == 0x00 {
            depth -= 1;
            if depth == 0 {
                return Some(pos + 2); // Skip past the EOC
            }
            pos += 2;
            continue;
        }

        // Skip tag byte(s)
        let tag = data[pos];
        pos += 1;
        if pos >= data.len() {
            break;
        }

        // Handle multi-byte tags (high tag number form)
        if (tag & 0x1F) == 0x1F {
            while pos < data.len() && (data[pos] & 0x80) != 0 {
                pos += 1;
            }
            if pos < data.len() {
                pos += 1; // Final tag byte
            }
        }

        if pos >= data.len() {
            break;
        }

        // Read length
        match read_ber_length(&data[pos..]) {
            Ok((len, len_bytes)) => {
                pos += len_bytes;
                if len == 0 && data[pos - len_bytes] == 0x80 {
                    // Another indefinite length - increase depth
                    depth += 1;
                } else {
                    // Definite length - skip content
                    pos += len;
                }
            }
            Err(_) => break,
        }
    }

    None
}

fn decode_integer(data: &[u8]) -> i64 {
    let mut value: i64 = 0;
    for &byte in data {
        value = (value << 8) | (byte as i64);
    }
    value
}

fn decode_string(data: &[u8]) -> String {
    // Try to decode as UTF-8, fallback to lossy
    String::from_utf8(data.to_vec())
        .unwrap_or_else(|_| String::from_utf8_lossy(data).to_string())
}

fn decode_tbcd(data: &[u8]) -> String {
    // Telephony BCD encoding
    let mut result = String::new();
    for &byte in data {
        let low = byte & 0x0F;
        let high = (byte >> 4) & 0x0F;

        if low < 10 {
            result.push((b'0' + low) as char);
        } else if low == 0x0F {
            // Filler
        } else {
            result.push((b'A' + low - 10) as char);
        }

        if high < 10 {
            result.push((b'0' + high) as char);
        } else if high == 0x0F {
            // Filler
        } else {
            result.push((b'A' + high - 10) as char);
        }
    }
    result
}

fn decode_ip_address(data: &[u8]) -> String {
    if data.len() >= 4 {
        // IPv4
        format!("{}.{}.{}.{}", data[0], data[1], data[2], data[3])
    } else {
        // Return hex for other formats
        data.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(":")
    }
}

fn decode_timestamp_field(data: &[u8]) -> Option<i64> {
    // Try to extract timestamp from constructed field
    let mut pos = 0;
    while pos + 2 < data.len() {
        let _tag = data[pos];
        pos += 1;

        if let Ok((len, len_bytes)) = read_ber_length(&data[pos..]) {
            pos += len_bytes;
            if pos + len <= data.len() {
                // Check if this looks like a timestamp
                if let Some(ts) = parse_timestamp(&data[pos..pos + len]) {
                    return Some(ts);
                }
            }
            pos += len;
        } else {
            break;
        }
    }
    None
}

fn parse_timestamp(data: &[u8]) -> Option<i64> {
    // 3GPP timestamp format: YYMMDDHHMMSS with optional timezone
    if data.len() >= 6 {
        let year = 2000 + decode_bcd_byte(data[0]) as i32;
        let month = decode_bcd_byte(data[1]) as u32;
        let day = decode_bcd_byte(data[2]) as u32;
        let hour = decode_bcd_byte(data[3]) as u32;
        let minute = decode_bcd_byte(data[4]) as u32;
        let second = decode_bcd_byte(data[5]) as u32;

        if let Some(dt) = NaiveDateTime::from_timestamp_opt(0, 0)
            .and_then(|_| chrono::NaiveDate::from_ymd_opt(year, month, day))
            .and_then(|d| d.and_hms_opt(hour, minute, second))
        {
            return Some(Utc.from_utc_datetime(&dt).timestamp_millis());
        }
    }
    None
}

fn decode_bcd_byte(byte: u8) -> u8 {
    ((byte >> 4) & 0x0F) * 10 + (byte & 0x0F)
}

/// Decode a MSC-CDR file containing multiple voice call records
fn decode_msc_cdr_file(data: &[u8], source_file: &str) -> Result<Vec<MscCdr>> {
    let mut cdrs = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        // Look for SEQUENCE tag (0x30) indicating start of a CDR
        if data[offset] == 0x30 {
            match decode_msc_cdr(&data[offset..], source_file) {
                Ok((cdr, consumed)) => {
                    cdrs.push(cdr);
                    offset += consumed;
                }
                Err(_e) => {
                    // Skip to next byte and try again
                    offset += 1;
                }
            }
        } else {
            offset += 1;
        }
    }

    Ok(cdrs)
}

/// Decode a single MSC-CDR record (3GPP TS 32.205 format)
fn decode_msc_cdr(data: &[u8], source_file: &str) -> Result<(MscCdr, usize)> {
    let mut cdr = MscCdr::default();
    cdr.source_file = source_file.to_string();

    if data.is_empty() || data[0] != 0x30 {
        return Err(anyhow!("Not a valid MSC-CDR record"));
    }

    let mut pos = 1;  // Skip SEQUENCE tag

    // Read length (may be indefinite 0x83 or definite)
    let (length, len_bytes) = read_ber_length_msc(&data[pos..])?;
    pos += len_bytes;

    let end_pos = if length > 0 { pos + length } else { data.len() };

    // Parse fields within the record
    while pos < end_pos && pos + 2 < data.len() {
        let tag = data[pos];
        pos += 1;

        if pos >= data.len() {
            break;
        }

        let (field_len, len_bytes) = match read_ber_length_msc(&data[pos..]) {
            Ok(v) => v,
            Err(_) => break,
        };
        pos += len_bytes;

        if pos + field_len > data.len() {
            break;
        }

        let field_data = &data[pos..pos + field_len];

        // Decode based on tag (3GPP TS 32.205 MOCallRecord/MTCallRecord)
        match tag {
            0x80 => cdr.record_type = decode_integer(field_data) as i64,
            0x81 => cdr.served_imsi = decode_tbcd(field_data),
            0x82 => cdr.served_imei = decode_tbcd(field_data),
            0x83 => cdr.served_msisdn = decode_tbcd(field_data),
            0x84 => cdr.calling_number = decode_tbcd(field_data),
            0x85 => cdr.called_number = decode_tbcd(field_data),
            0x86 => cdr.recording_entity = decode_tbcd(field_data),
            0x87 => cdr.msc_address = decode_tbcd(field_data),
            0x88 => {
                // Call reference
                if let Some(ts) = parse_timestamp(field_data) {
                    cdr.seizure_time = ts;
                }
            }
            0x89 => {
                // Answer time
                if let Some(ts) = parse_timestamp(field_data) {
                    cdr.answer_time = ts;
                }
            }
            0x8A | 0x8B => {
                // Release time / call duration
                if tag == 0x8B {
                    cdr.call_duration = decode_integer(field_data) as u64;
                } else if let Some(ts) = parse_timestamp(field_data) {
                    cdr.release_time = ts;
                }
            }
            0x8C => cdr.cause_for_termination = decode_integer(field_data) as i64,
            0x8D | 0x8E => cdr.basic_service = format!("{:02X}", field_data.first().unwrap_or(&0)),
            0x8F => cdr.call_reference = decode_integer(field_data) as u64,
            0x90 | 0x91 => cdr.sequence_number = decode_integer(field_data) as u64,
            0xA0 | 0xA1 | 0xA2 => {
                // Constructed fields (location info, etc.)
                // Parse nested fields for location info
                if field_len > 2 {
                    let mut inner_pos = 0;
                    while inner_pos + 2 < field_len {
                        let inner_tag = field_data[inner_pos];
                        inner_pos += 1;
                        if let Ok((inner_len, inner_len_bytes)) = read_ber_length_msc(&field_data[inner_pos..]) {
                            inner_pos += inner_len_bytes;
                            if inner_pos + inner_len <= field_len {
                                let inner_data = &field_data[inner_pos..inner_pos + inner_len];
                                match inner_tag {
                                    0x80 => cdr.location_area_code = format!("{:04X}", decode_integer(inner_data)),
                                    0x81 => cdr.cell_id = format!("{:04X}", decode_integer(inner_data)),
                                    0x82 => cdr.mcc_mnc = decode_tbcd(inner_data),
                                    _ => {}
                                }
                            }
                            inner_pos += inner_len;
                        } else {
                            break;
                        }
                    }
                }
            }
            0x9F => {
                // Extended tag handling
                if field_len >= 2 {
                    match field_data[0] {
                        0x81 => {
                            // Various extended fields based on second byte
                            if field_len >= 3 {
                                match field_data[1] {
                                    0x01 | 0x02 | 0x03 => {
                                        // Duration or timestamps
                                        let val = decode_integer(&field_data[2..]);
                                        if field_data[1] == 0x01 {
                                            cdr.call_duration = val as u64;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            0xBF => {
                // Long-form context tag
                // Skip these for now
            }
            _ => {
                // Skip unknown tags
            }
        }

        pos += field_len;
    }

    // Calculate total consumed
    let total_consumed = 1 + len_bytes + length;
    Ok((cdr, total_consumed.max(1).min(data.len())))
}

/// Read BER length for MSC-CDR (handles indefinite length)
fn read_ber_length_msc(data: &[u8]) -> Result<(usize, usize)> {
    if data.is_empty() {
        return Err(anyhow!("Empty data for length"));
    }

    let first = data[0];
    if first < 0x80 {
        // Short form
        Ok((first as usize, 1))
    } else if first == 0x80 {
        // Indefinite length - return 0 and handle specially
        Ok((0, 1))
    } else {
        // Long form
        let num_octets = (first & 0x7F) as usize;
        if num_octets > 4 || data.len() < 1 + num_octets {
            return Err(anyhow!("Invalid length encoding"));
        }

        let mut length: usize = 0;
        for i in 0..num_octets {
            length = (length << 8) | (data[1 + i] as usize);
        }

        Ok((length, 1 + num_octets))
    }
}

/// Convert MSC-CDRs to Arrow RecordBatch
fn msc_cdrs_to_arrow_batch(cdrs: &[MscCdr]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_type", DataType::Int64, true),
        Field::new("calling_number", DataType::Utf8, true),
        Field::new("called_number", DataType::Utf8, true),
        Field::new("served_imsi", DataType::Utf8, true),
        Field::new("served_imei", DataType::Utf8, true),
        Field::new("served_msisdn", DataType::Utf8, true),
        Field::new("recording_entity", DataType::Utf8, true),
        Field::new("msc_address", DataType::Utf8, true),
        Field::new("call_reference", DataType::UInt64, true),
        Field::new("call_duration", DataType::UInt64, true),
        Field::new("answer_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("release_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("seizure_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("cause_for_termination", DataType::Int64, true),
        Field::new("basic_service", DataType::Utf8, true),
        Field::new("location_area_code", DataType::Utf8, true),
        Field::new("cell_id", DataType::Utf8, true),
        Field::new("sequence_number", DataType::UInt64, true),
        Field::new("mcc_mnc", DataType::Utf8, true),
        Field::new("source_file", DataType::Utf8, true),
    ]));

    let mut record_type = Int64Builder::new();
    let mut calling_number = StringBuilder::new();
    let mut called_number = StringBuilder::new();
    let mut served_imsi = StringBuilder::new();
    let mut served_imei = StringBuilder::new();
    let mut served_msisdn = StringBuilder::new();
    let mut recording_entity = StringBuilder::new();
    let mut msc_address = StringBuilder::new();
    let mut call_reference = UInt64Builder::new();
    let mut call_duration = UInt64Builder::new();
    let mut answer_time = TimestampMillisecondBuilder::new();
    let mut release_time = TimestampMillisecondBuilder::new();
    let mut seizure_time = TimestampMillisecondBuilder::new();
    let mut cause_for_termination = Int64Builder::new();
    let mut basic_service = StringBuilder::new();
    let mut location_area_code = StringBuilder::new();
    let mut cell_id = StringBuilder::new();
    let mut sequence_number = UInt64Builder::new();
    let mut mcc_mnc = StringBuilder::new();
    let mut source_file = StringBuilder::new();

    for cdr in cdrs {
        record_type.append_value(cdr.record_type);
        calling_number.append_value(&cdr.calling_number);
        called_number.append_value(&cdr.called_number);
        served_imsi.append_value(&cdr.served_imsi);
        served_imei.append_value(&cdr.served_imei);
        served_msisdn.append_value(&cdr.served_msisdn);
        recording_entity.append_value(&cdr.recording_entity);
        msc_address.append_value(&cdr.msc_address);
        call_reference.append_value(cdr.call_reference);
        call_duration.append_value(cdr.call_duration);

        if cdr.answer_time > 0 {
            answer_time.append_value(cdr.answer_time);
        } else {
            answer_time.append_null();
        }
        if cdr.release_time > 0 {
            release_time.append_value(cdr.release_time);
        } else {
            release_time.append_null();
        }
        if cdr.seizure_time > 0 {
            seizure_time.append_value(cdr.seizure_time);
        } else {
            seizure_time.append_null();
        }

        cause_for_termination.append_value(cdr.cause_for_termination);
        basic_service.append_value(&cdr.basic_service);
        location_area_code.append_value(&cdr.location_area_code);
        cell_id.append_value(&cdr.cell_id);
        sequence_number.append_value(cdr.sequence_number);
        mcc_mnc.append_value(&cdr.mcc_mnc);
        source_file.append_value(&cdr.source_file);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(record_type.finish()),
        Arc::new(calling_number.finish()),
        Arc::new(called_number.finish()),
        Arc::new(served_imsi.finish()),
        Arc::new(served_imei.finish()),
        Arc::new(served_msisdn.finish()),
        Arc::new(recording_entity.finish()),
        Arc::new(msc_address.finish()),
        Arc::new(call_reference.finish()),
        Arc::new(call_duration.finish()),
        Arc::new(answer_time.finish()),
        Arc::new(release_time.finish()),
        Arc::new(seizure_time.finish()),
        Arc::new(cause_for_termination.finish()),
        Arc::new(basic_service.finish()),
        Arc::new(location_area_code.finish()),
        Arc::new(cell_id.finish()),
        Arc::new(sequence_number.finish()),
        Arc::new(mcc_mnc.finish()),
        Arc::new(source_file.finish()),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to create MSC-CDR RecordBatch")
}

/// Decode an SMS-CDR file containing multiple SMS records
fn decode_sms_cdr_file(data: &[u8], source_file: &str) -> Result<Vec<SmsCdr>> {
    let mut cdrs = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        // Look for SEQUENCE tag (0x30) indicating start of a CDR
        if data[offset] == 0x30 {
            match decode_sms_cdr(&data[offset..], source_file) {
                Ok((cdr, consumed)) => {
                    cdrs.push(cdr);
                    offset += consumed;
                }
                Err(_e) => {
                    // Skip to next byte and try again
                    offset += 1;
                }
            }
        } else {
            offset += 1;
        }
    }

    Ok(cdrs)
}

/// Decode a single SMS-CDR record (3GPP TS 32.205 format)
fn decode_sms_cdr(data: &[u8], source_file: &str) -> Result<(SmsCdr, usize)> {
    let mut cdr = SmsCdr::default();
    cdr.source_file = source_file.to_string();

    if data.is_empty() || data[0] != 0x30 {
        return Err(anyhow!("Not a valid SMS-CDR record"));
    }

    let mut pos = 1;  // Skip SEQUENCE tag

    // Read length
    let (length, len_bytes) = read_ber_length_msc(&data[pos..])?;
    pos += len_bytes;

    let end_pos = if length > 0 { pos + length } else { data.len() };

    // Parse fields within the record
    while pos < end_pos && pos + 2 < data.len() {
        let tag = data[pos];
        pos += 1;

        if pos >= data.len() {
            break;
        }

        let (field_len, len_bytes) = match read_ber_length_msc(&data[pos..]) {
            Ok(v) => v,
            Err(_) => break,
        };
        pos += len_bytes;

        if pos + field_len > data.len() {
            break;
        }

        let field_data = &data[pos..pos + field_len];

        // Decode based on tag (3GPP TS 32.205 SMS record)
        match tag {
            0x80 => cdr.record_type = decode_integer(field_data) as i64,
            0x81 => cdr.served_imsi = decode_tbcd(field_data),
            0x82 => cdr.served_imei = decode_tbcd(field_data),
            0x83 => cdr.served_msisdn = decode_tbcd(field_data),
            0x84 => cdr.originating_number = decode_tbcd(field_data),
            0x85 => cdr.destination_number = decode_tbcd(field_data),
            0x86 => cdr.recording_entity = decode_tbcd(field_data),
            0x87 => cdr.smsc_address = decode_tbcd(field_data),
            0x88 => {
                // Submit time
                if let Some(ts) = parse_timestamp(field_data) {
                    cdr.submit_time = ts;
                }
            }
            0x89 => {
                // Delivery time
                if let Some(ts) = parse_timestamp(field_data) {
                    cdr.delivery_time = ts;
                }
            }
            0x8A => cdr.message_reference = decode_integer(field_data) as u64,
            0x8B => cdr.message_size = decode_integer(field_data) as u64,
            0x8C => cdr.delivery_status = decode_integer(field_data) as i64,
            0x8D => cdr.cause_for_termination = decode_integer(field_data) as i64,
            0x8E => cdr.message_class = format!("{:02X}", field_data.first().unwrap_or(&0)),
            0x8F => cdr.sequence_number = decode_integer(field_data) as u64,
            0xA0 | 0xA1 | 0xA2 => {
                // Constructed fields (location info, etc.)
                if field_len > 2 {
                    let mut inner_pos = 0;
                    while inner_pos + 2 < field_len {
                        let inner_tag = field_data[inner_pos];
                        inner_pos += 1;
                        if let Ok((inner_len, inner_len_bytes)) = read_ber_length_msc(&field_data[inner_pos..]) {
                            inner_pos += inner_len_bytes;
                            if inner_pos + inner_len <= field_len {
                                let inner_data = &field_data[inner_pos..inner_pos + inner_len];
                                match inner_tag {
                                    0x80 => cdr.location_area_code = format!("{:04X}", decode_integer(inner_data)),
                                    0x81 => cdr.cell_id = format!("{:04X}", decode_integer(inner_data)),
                                    0x82 => cdr.mcc_mnc = decode_tbcd(inner_data),
                                    _ => {}
                                }
                            }
                            inner_pos += inner_len;
                        } else {
                            break;
                        }
                    }
                }
            }
            _ => {
                // Skip unknown tags
            }
        }

        pos += field_len;
    }

    // Calculate total consumed
    let total_consumed = 1 + len_bytes + length;
    Ok((cdr, total_consumed.max(1).min(data.len())))
}

/// Convert SMS-CDRs to Arrow RecordBatch
fn sms_cdrs_to_arrow_batch(cdrs: &[SmsCdr]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_type", DataType::Int64, true),
        Field::new("originating_number", DataType::Utf8, true),
        Field::new("destination_number", DataType::Utf8, true),
        Field::new("served_imsi", DataType::Utf8, true),
        Field::new("served_imei", DataType::Utf8, true),
        Field::new("served_msisdn", DataType::Utf8, true),
        Field::new("recording_entity", DataType::Utf8, true),
        Field::new("smsc_address", DataType::Utf8, true),
        Field::new("message_reference", DataType::UInt64, true),
        Field::new("submit_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("delivery_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("message_size", DataType::UInt64, true),
        Field::new("message_class", DataType::Utf8, true),
        Field::new("delivery_status", DataType::Int64, true),
        Field::new("cause_for_termination", DataType::Int64, true),
        Field::new("location_area_code", DataType::Utf8, true),
        Field::new("cell_id", DataType::Utf8, true),
        Field::new("sequence_number", DataType::UInt64, true),
        Field::new("mcc_mnc", DataType::Utf8, true),
        Field::new("source_file", DataType::Utf8, true),
    ]));

    let mut record_type = Int64Builder::new();
    let mut originating_number = StringBuilder::new();
    let mut destination_number = StringBuilder::new();
    let mut served_imsi = StringBuilder::new();
    let mut served_imei = StringBuilder::new();
    let mut served_msisdn = StringBuilder::new();
    let mut recording_entity = StringBuilder::new();
    let mut smsc_address = StringBuilder::new();
    let mut message_reference = UInt64Builder::new();
    let mut submit_time = TimestampMillisecondBuilder::new();
    let mut delivery_time = TimestampMillisecondBuilder::new();
    let mut message_size = UInt64Builder::new();
    let mut message_class = StringBuilder::new();
    let mut delivery_status = Int64Builder::new();
    let mut cause_for_termination = Int64Builder::new();
    let mut location_area_code = StringBuilder::new();
    let mut cell_id = StringBuilder::new();
    let mut sequence_number = UInt64Builder::new();
    let mut mcc_mnc = StringBuilder::new();
    let mut source_file = StringBuilder::new();

    for cdr in cdrs {
        record_type.append_value(cdr.record_type);
        originating_number.append_value(&cdr.originating_number);
        destination_number.append_value(&cdr.destination_number);
        served_imsi.append_value(&cdr.served_imsi);
        served_imei.append_value(&cdr.served_imei);
        served_msisdn.append_value(&cdr.served_msisdn);
        recording_entity.append_value(&cdr.recording_entity);
        smsc_address.append_value(&cdr.smsc_address);
        message_reference.append_value(cdr.message_reference);

        if cdr.submit_time > 0 {
            submit_time.append_value(cdr.submit_time);
        } else {
            submit_time.append_null();
        }
        if cdr.delivery_time > 0 {
            delivery_time.append_value(cdr.delivery_time);
        } else {
            delivery_time.append_null();
        }

        message_size.append_value(cdr.message_size);
        message_class.append_value(&cdr.message_class);
        delivery_status.append_value(cdr.delivery_status);
        cause_for_termination.append_value(cdr.cause_for_termination);
        location_area_code.append_value(&cdr.location_area_code);
        cell_id.append_value(&cdr.cell_id);
        sequence_number.append_value(cdr.sequence_number);
        mcc_mnc.append_value(&cdr.mcc_mnc);
        source_file.append_value(&cdr.source_file);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(record_type.finish()),
        Arc::new(originating_number.finish()),
        Arc::new(destination_number.finish()),
        Arc::new(served_imsi.finish()),
        Arc::new(served_imei.finish()),
        Arc::new(served_msisdn.finish()),
        Arc::new(recording_entity.finish()),
        Arc::new(smsc_address.finish()),
        Arc::new(message_reference.finish()),
        Arc::new(submit_time.finish()),
        Arc::new(delivery_time.finish()),
        Arc::new(message_size.finish()),
        Arc::new(message_class.finish()),
        Arc::new(delivery_status.finish()),
        Arc::new(cause_for_termination.finish()),
        Arc::new(location_area_code.finish()),
        Arc::new(cell_id.finish()),
        Arc::new(sequence_number.finish()),
        Arc::new(mcc_mnc.finish()),
        Arc::new(source_file.finish()),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to create SMS-CDR RecordBatch")
}

/// Convert CDRs to Arrow RecordBatch
fn cdrs_to_arrow_batch(cdrs: &[PgwCdr]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_type", DataType::Int64, true),
        Field::new("served_imsi", DataType::Utf8, true),
        Field::new("served_imei", DataType::Utf8, true),
        Field::new("served_msisdn", DataType::Utf8, true),
        Field::new("pgw_address", DataType::Utf8, true),
        Field::new("charging_id", DataType::UInt64, true),
        Field::new("apn", DataType::Utf8, true),
        Field::new("pdp_type", DataType::Utf8, true),
        Field::new("served_pdp_address", DataType::Utf8, true),
        Field::new("record_opening_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("duration", DataType::UInt64, true),
        Field::new("cause_for_record_closing", DataType::Int64, true),
        Field::new("node_id", DataType::Utf8, true),
        Field::new("record_sequence_number", DataType::UInt64, true),
        Field::new("local_sequence_number", DataType::UInt64, true),
        Field::new("data_volume_uplink", DataType::UInt64, true),
        Field::new("data_volume_downlink", DataType::UInt64, true),
        Field::new("rat_type", DataType::Int64, true),
        Field::new("charging_characteristics", DataType::Utf8, true),
        Field::new("mcc_mnc", DataType::Utf8, true),
        Field::new("source_file", DataType::Utf8, true),
    ]));

    let mut record_type = Int64Builder::new();
    let mut served_imsi = StringBuilder::new();
    let mut served_imei = StringBuilder::new();
    let mut served_msisdn = StringBuilder::new();
    let mut pgw_address = StringBuilder::new();
    let mut charging_id = UInt64Builder::new();
    let mut apn = StringBuilder::new();
    let mut pdp_type = StringBuilder::new();
    let mut served_pdp_address = StringBuilder::new();
    let mut record_opening_time = TimestampMillisecondBuilder::new();
    let mut duration = UInt64Builder::new();
    let mut cause_for_record_closing = Int64Builder::new();
    let mut node_id = StringBuilder::new();
    let mut record_sequence_number = UInt64Builder::new();
    let mut local_sequence_number = UInt64Builder::new();
    let mut data_volume_uplink = UInt64Builder::new();
    let mut data_volume_downlink = UInt64Builder::new();
    let mut rat_type = Int64Builder::new();
    let mut charging_characteristics = StringBuilder::new();
    let mut mcc_mnc = StringBuilder::new();
    let mut source_file = StringBuilder::new();

    for cdr in cdrs {
        record_type.append_value(cdr.record_type);
        served_imsi.append_value(&cdr.served_imsi);
        served_imei.append_value(&cdr.served_imei);
        served_msisdn.append_value(&cdr.served_msisdn);
        pgw_address.append_value(&cdr.pgw_address);
        charging_id.append_value(cdr.charging_id);
        apn.append_value(&cdr.apn);
        pdp_type.append_value(&cdr.pdp_type);
        served_pdp_address.append_value(&cdr.served_pdp_address);
        if cdr.record_opening_time > 0 {
            record_opening_time.append_value(cdr.record_opening_time);
        } else {
            record_opening_time.append_null();
        }
        duration.append_value(cdr.duration);
        cause_for_record_closing.append_value(cdr.cause_for_record_closing);
        node_id.append_value(&cdr.node_id);
        record_sequence_number.append_value(cdr.record_sequence_number);
        local_sequence_number.append_value(cdr.local_sequence_number);
        data_volume_uplink.append_value(cdr.data_volume_uplink);
        data_volume_downlink.append_value(cdr.data_volume_downlink);
        rat_type.append_value(cdr.rat_type);
        charging_characteristics.append_value(&cdr.charging_characteristics);
        mcc_mnc.append_value(&cdr.mcc_mnc);
        source_file.append_value(&cdr.source_file);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(record_type.finish()),
        Arc::new(served_imsi.finish()),
        Arc::new(served_imei.finish()),
        Arc::new(served_msisdn.finish()),
        Arc::new(pgw_address.finish()),
        Arc::new(charging_id.finish()),
        Arc::new(apn.finish()),
        Arc::new(pdp_type.finish()),
        Arc::new(served_pdp_address.finish()),
        Arc::new(record_opening_time.finish()),
        Arc::new(duration.finish()),
        Arc::new(cause_for_record_closing.finish()),
        Arc::new(node_id.finish()),
        Arc::new(record_sequence_number.finish()),
        Arc::new(local_sequence_number.finish()),
        Arc::new(data_volume_uplink.finish()),
        Arc::new(data_volume_downlink.finish()),
        Arc::new(rat_type.finish()),
        Arc::new(charging_characteristics.finish()),
        Arc::new(mcc_mnc.finish()),
        Arc::new(source_file.finish()),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to create RecordBatch")
}

/// Get or create a thread-local connection to BOYODB
fn get_or_create_connection(host: &str) -> Result<TcpStream> {
    BOYODB_CONNECTION.with(|conn_cell| {
        let mut conn_ref = conn_cell.borrow_mut();

        // Check if we have an existing valid connection
        if let Some(ref stream) = *conn_ref {
            // Test if connection is still alive with a zero-length peek
            if stream.peek(&mut [0u8; 1]).is_ok() ||
               matches!(stream.peek(&mut [0u8; 1]), Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock) {
                // Connection looks valid, try to clone it for use
                if let Ok(cloned) = stream.try_clone() {
                    return Ok(cloned);
                }
            }
            // Connection is dead, clear it
            debug!("Existing connection is dead, reconnecting");
        }

        // Create new connection
        debug!("Creating new connection to BOYODB at {}", host);
        let stream = TcpStream::connect(host)
            .with_context(|| format!("Failed to connect to BOYODB at {}", host))?;
        stream.set_read_timeout(Some(Duration::from_secs(120)))?;
        stream.set_write_timeout(Some(Duration::from_secs(120)))?;
        stream.set_nodelay(true)?; // Disable Nagle's algorithm for lower latency

        // Store the connection and return a clone
        let cloned = stream.try_clone().context("Failed to clone stream")?;
        *conn_ref = Some(stream);
        Ok(cloned)
    })
}

/// Clear the thread-local connection (call after an error)
fn clear_connection() {
    BOYODB_CONNECTION.with(|conn_cell| {
        *conn_cell.borrow_mut() = None;
    });
}

/// Send a request and read response on a stream
fn send_and_receive(stream: &mut TcpStream, json_bytes: &[u8]) -> Result<serde_json::Value> {
    // Send length-prefixed frame (4-byte big-endian length + JSON)
    let len = json_bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(json_bytes)?;
    stream.flush()?;

    // Read response length (4 bytes big-endian)
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check response length
    if resp_len > 100 * 1024 * 1024 {
        return Err(anyhow!("Response too large: {} bytes", resp_len));
    }

    // Read response JSON
    let mut response_bytes = vec![0u8; resp_len];
    stream.read_exact(&mut response_bytes)?;

    // Parse JSON response
    serde_json::from_slice(&response_bytes)
        .with_context(|| format!("Failed to parse response: {}", String::from_utf8_lossy(&response_bytes)))
}

/// Ingest a RecordBatch into BOYODB using connection pooling
fn ingest_batch(host: &str, database: &str, table: &str, batch: RecordBatch) -> Result<()> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    // Serialize to Arrow IPC
    let mut ipc_data = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_data, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    // Build JSON request matching BOYODB's IngestIpc format
    let payload_base64 = STANDARD.encode(&ipc_data);
    let watermark_micros = chrono::Utc::now().timestamp_micros() as u64;

    let request = serde_json::json!({
        "op": "ingestipc",
        "payload_base64": payload_base64,
        "watermark_micros": watermark_micros,
        "database": database,
        "table": table
    });

    // Serialize to JSON bytes
    let json_bytes = serde_json::to_vec(&request)?;

    // Try to use pooled connection first
    let result = (|| -> Result<serde_json::Value> {
        let mut stream = get_or_create_connection(host)?;
        send_and_receive(&mut stream, &json_bytes)
    })();

    // If the pooled connection failed, try once more with a fresh connection
    let response = match result {
        Ok(resp) => resp,
        Err(e) => {
            debug!("Pooled connection failed: {}, retrying with fresh connection", e);
            clear_connection();
            let mut stream = get_or_create_connection(host)?;
            send_and_receive(&mut stream, &json_bytes)?
        }
    };

    // Check for success
    if let Some(status) = response.get("status").and_then(|v| v.as_str()) {
        if status != "ok" {
            let message = response.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow!("Ingest failed: {} - {}", status, message));
        }
    } else if response.get("error").is_some() {
        let error = response.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
        return Err(anyhow!("Ingest failed: {}", error));
    }

    Ok(())
}
