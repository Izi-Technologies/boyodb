use anyhow::{anyhow, Result};
use boyodb_core::types::{BoyodbStatus, OwnedBuffer};
use clap::{Args, Parser, Subcommand};
use serde_json;
use std::ffi::CString;
use std::io::Write;
use std::path::PathBuf;
use std::slice;

mod shell;

#[derive(Parser)]
#[command(name = "boyodb-cli")]
#[command(about = "CLI for boyodb via C ABI or TCP server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Clone, Debug)]
struct LocalEngineArgs {
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

#[derive(Subcommand)]
enum Commands {
    /// Connect to a boyodb-server and start an interactive SQL shell
    ///
    /// Settings can be configured in ~/.boyodbrc (TOML format).
    /// CLI arguments override config file settings.
    Shell {
        /// Server address (host:port). Default: localhost:8765 or from ~/.boyodbrc
        #[arg(short = 'H', long)]
        host: Option<String>,
        /// Authentication token (if server requires it)
        #[arg(short, long)]
        token: Option<String>,
        /// Username for authentication (use with --password for non-interactive login)
        #[arg(short, long)]
        user: Option<String>,
        /// Password for authentication (use with --user for non-interactive login)
        #[arg(short = 'P', long)]
        password: Option<String>,
        /// Default database to use
        #[arg(short, long)]
        database: Option<String>,
        /// Enable TLS connection
        #[arg(long)]
        tls: bool,
        /// Path to CA certificate file for TLS verification
        #[arg(long)]
        tls_ca: Option<String>,
        /// INSECURE: Skip TLS certificate verification (for testing only, vulnerable to MITM attacks)
        #[arg(long)]
        tls_skip_verify: bool,
        /// Execute SQL command and exit (non-interactive mode)
        #[arg(short = 'c', long)]
        command: Option<String>,
    },
    /// Fetch WAL stats from a running boyodb-server
    WalStats {
        /// Server address (host:port). Default: localhost:8765 or from ~/.boyodbrc
        #[arg(short = 'H', long)]
        host: Option<String>,
        /// Authentication token (if server requires it)
        #[arg(short, long)]
        token: Option<String>,
        /// Username for authentication (use with --password)
        #[arg(short, long)]
        user: Option<String>,
        /// Password for authentication (use with --user)
        #[arg(short = 'P', long)]
        password: Option<String>,
        /// Enable TLS connection
        #[arg(long)]
        tls: bool,
        /// Path to CA certificate file for TLS verification
        #[arg(long)]
        tls_ca: Option<String>,
        /// INSECURE: Skip TLS certificate verification (for testing only)
        #[arg(long)]
        tls_skip_verify: bool,
    },
    Ingest {
        data_dir: PathBuf,
        ipc_file: PathBuf,
        #[arg(long)]
        database: Option<String>,
        #[arg(long)]
        table: Option<String>,
        #[arg(long, default_value_t = 0)]
        shard_hint: u64,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Query {
        data_dir: PathBuf,
        sql: String,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Manifest {
        data_dir: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    ImportManifest {
        data_dir: PathBuf,
        json_file: PathBuf,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Health {
        data_dir: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Checkpoint {
        data_dir: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    CreateDb {
        data_dir: PathBuf,
        name: String,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    CreateTable {
        data_dir: PathBuf,
        database: String,
        table: String,
        #[arg(long)]
        schema_json_file: Option<PathBuf>,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    ListDbs {
        data_dir: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    ListTables {
        data_dir: PathBuf,
        #[arg(long)]
        database: Option<String>,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    DescribeTable {
        data_dir: PathBuf,
        database: String,
        table: String,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Backup {
        data_dir: PathBuf,
        output: PathBuf,
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long)]
        since_version: Option<u64>,
        #[arg(long, default_value_t = true)]
        prefer_hot: bool,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    Restore {
        data_dir: PathBuf,
        input: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Shell { host, token, user, password, database, tls, tls_ca, tls_skip_verify, command } => {
            let tls_config = if *tls {
                Some(shell::TlsConfig {
                    ca_path: tls_ca.clone(),
                    skip_verify: *tls_skip_verify,
                })
            } else {
                None
            };
            let auth = match (user, password) {
                (Some(u), Some(p)) => Some((u.clone(), p.clone())),
                (Some(u), None) => {
                    // Username provided without password - prompt for it
                    let pwd = rpassword::prompt_password("Password: ")
                        .unwrap_or_else(|_| String::new());
                    Some((u.clone(), pwd))
                },
                _ => None,
            };
            shell::run_shell(host.as_deref(), token.clone(), database.clone(), tls_config, auth, command.clone())?;
        }
        Commands::WalStats {
            host,
            token,
            user,
            password,
            tls,
            tls_ca,
            tls_skip_verify,
        } => {
            let tls_config = if *tls {
                Some(shell::TlsConfig {
                    ca_path: tls_ca.clone(),
                    skip_verify: *tls_skip_verify,
                })
            } else {
                None
            };
            let auth = match (user, password) {
                (Some(u), Some(p)) => Some((u.clone(), p.clone())),
                (Some(u), None) => {
                    let pwd = rpassword::prompt_password("Password: ")
                        .unwrap_or_else(|_| String::new());
                    Some((u.clone(), pwd))
                }
                _ => None,
            };
            let stats = shell::fetch_wal_stats(host.as_deref(), token.clone(), tls_config, auth)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
        Commands::Ingest {
            data_dir,
            ipc_file,
            database,
            table,
            shard_hint,
            engine,
        } => {
            let handle = open(data_dir, false, engine)?;
            let payload = std::fs::read(ipc_file)?;
            let db = database.clone().unwrap_or_else(|| "default".into());
            let tbl = table.clone().unwrap_or_else(|| "default".into());
            let status = boyodb_core::ffi::boyodb_ingest_ipc_v3(
                handle.as_ptr(),
                payload.as_ptr(),
                payload.len(),
                0,
                *shard_hint,
                true,
                CString::new(db)?.as_ptr(),
                true,
                CString::new(tbl)?.as_ptr(),
                true,
            );
            check_status(status)?;
            println!("ingest ok");
        }
        Commands::Query { data_dir, sql, engine } => {
            let handle = open(data_dir, false, engine)?;
            let c_sql = CString::new(sql.as_str())?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_query_ipc(
                handle.as_ptr(),
                &boyodb_core::ffi::BoyodbQueryRequest {
                    sql: c_sql.as_ptr(),
                    timeout_millis: 10_000,
                },
                buf.as_mut_ptr(),
            );
            check_status(status)?;
            std::io::stdout().write_all(buf.bytes())?;
        }
        Commands::Manifest { data_dir, engine } => {
            let handle = open(data_dir, false, engine)?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_manifest(handle.as_ptr(), buf.as_mut_ptr());
            check_status(status)?;
            std::io::stdout().write_all(buf.bytes())?;
        }
        Commands::ImportManifest {
            data_dir,
            json_file,
            overwrite,
            engine,
        } => {
            let handle = open(data_dir, true, engine)?;
            let payload = std::fs::read(json_file)?;
            let status = boyodb_core::ffi::boyodb_import_manifest(
                handle.as_ptr(),
                payload.as_ptr(),
                payload.len(),
                *overwrite,
            );
            check_status(status)?;
            println!("manifest imported");
        }
        Commands::Health { data_dir, engine } => {
            let handle = open(data_dir, false, engine)?;
            let status = boyodb_core::ffi::boyodb_healthcheck(handle.as_ptr());
            check_status(status)?;
            println!("healthy");
        }
        Commands::Checkpoint { data_dir, engine } => {
            let handle = open(data_dir, false, engine)?;
            let status = boyodb_core::ffi::boyodb_checkpoint(handle.as_ptr());
            check_status(status)?;
            println!("checkpoint ok");
        }
        Commands::CreateDb { data_dir, name, engine } => {
            let handle = open(data_dir, false, engine)?;
            let c_name = CString::new(name.as_str())?;
            let status = boyodb_core::ffi::boyodb_create_database(handle.as_ptr(), c_name.as_ptr());
            check_status(status)?;
            println!("database created: {}", name);
        }
        Commands::CreateTable {
            data_dir,
            database,
            table,
            schema_json_file,
            engine,
        } => {
            let handle = open(data_dir, false, engine)?;
            let c_db = CString::new(database.as_str())?;
            let c_tbl = CString::new(table.as_str())?;
            let _schema_c: Option<CString> = if let Some(path) = schema_json_file {
                let schema_owned = std::fs::read_to_string(path)?;
                Some(CString::new(schema_owned.as_str())?)
            } else {
                None
            };
            let schema_ptr: *const i8 = _schema_c
                .as_ref()
                .map(|c| c.as_ptr())
                .unwrap_or(std::ptr::null());
            let status = boyodb_core::ffi::boyodb_create_table(
                handle.as_ptr(),
                c_db.as_ptr(),
                c_tbl.as_ptr(),
                schema_ptr,
            );
            check_status(status)?;
            println!("table created: {}.{}", database, table);
        }
        Commands::ListDbs { data_dir, engine } => {
            let handle = open(data_dir, false, engine)?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_list_databases(handle.as_ptr(), buf.as_mut_ptr());
            check_status(status)?;
            println!("{}", String::from_utf8_lossy(buf.bytes()));
        }
        Commands::ListTables { data_dir, database, engine } => {
            let handle = open(data_dir, false, engine)?;
            let mut buf = BufferGuard::new();
            let _c_db: Option<CString> = match database {
                Some(db) => Some(CString::new(db.as_str())?),
                None => None,
            };
            let status = boyodb_core::ffi::boyodb_list_tables(
                handle.as_ptr(),
                _c_db
                    .as_ref()
                    .map(|c| c.as_ptr())
                    .unwrap_or(std::ptr::null()),
                _c_db.is_some(),
                buf.as_mut_ptr(),
            );
            check_status(status)?;
            println!("{}", String::from_utf8_lossy(buf.bytes()));
        }
        Commands::DescribeTable {
            data_dir,
            database,
            table,
            engine,
        } => {
            let handle = open(data_dir, false, engine)?;
            let c_db = CString::new(database.as_str())?;
            let c_tbl = CString::new(table.as_str())?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_describe_table(
                handle.as_ptr(),
                c_db.as_ptr(),
                c_tbl.as_ptr(),
                buf.as_mut_ptr(),
            );
            check_status(status)?;
            // Pretty-print the JSON
            let json: serde_json::Value = serde_json::from_slice(buf.bytes())?;
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        Commands::Backup {
            data_dir,
            output,
            max_bytes,
            since_version,
            prefer_hot,
            engine,
        } => {
            let handle = open(data_dir, false, engine)?;
            let mut buf = BufferGuard::new();
            let req = boyodb_core::ffi::BoyodbBundleRequest {
                max_bytes: max_bytes.unwrap_or_default(),
                has_max_bytes: max_bytes.is_some(),
                since_version: since_version.unwrap_or_default(),
                has_since_version: since_version.is_some(),
                prefer_hot: *prefer_hot,
                target_bytes_per_sec: 0,
                has_target_bytes_per_sec: false,
            };
            let status =
                boyodb_core::ffi::boyodb_export_bundle(handle.as_ptr(), &req, buf.as_mut_ptr());
            check_status(status)?;
            std::fs::write(output, buf.bytes())?;
            println!("backup written to {}", output.display());
        }
        Commands::Restore { data_dir, input, engine } => {
            let handle = open(data_dir, true, engine)?;
            let payload = std::fs::read(input)?;
            let status =
                boyodb_core::ffi::boyodb_apply_bundle(handle.as_ptr(), payload.as_ptr(), payload.len());
            check_status(status)?;
            println!("restore applied from {}", input.display());
        }
    }

    Ok(())
}

fn open(
    data_dir: &std::path::Path,
    allow_manifest_import: bool,
    engine: &LocalEngineArgs,
) -> Result<HandleGuard> {
    let data_dir_c = CString::new(data_dir.to_string_lossy().as_bytes())?;
    let tier_warm_c = engine
        .tier_warm_compression
        .as_ref()
        .map(|s| CString::new(s.as_bytes()))
        .transpose()?;
    let tier_cold_c = engine
        .tier_cold_compression
        .as_ref()
        .map(|s| CString::new(s.as_bytes()))
        .transpose()?;
    let mut handle: *mut boyodb_core::ffi::BoyodbHandle = std::ptr::null_mut();
    let status = boyodb_core::ffi::boyodb_open(
        &boyodb_core::ffi::BoyodbOpenOptions {
            data_dir: data_dir_c.as_ptr(),
            wal_dir: std::ptr::null(),
            shard_count: 1,
            cache_bytes: 0,
            wal_max_bytes: 0,
            wal_max_segments: 0,
            allow_manifest_import,
            tier_warm_compression: tier_warm_c
                .as_ref()
                .map_or(std::ptr::null(), |c| c.as_ptr()),
            tier_cold_compression: tier_cold_c
                .as_ref()
                .map_or(std::ptr::null(), |c| c.as_ptr()),
            cache_hot_segments: engine.cache_hot_segments,
            cache_warm_segments: engine.cache_warm_segments,
            cache_cold_segments: engine.cache_cold_segments,
        },
        &mut handle,
    );
    check_status(status)?;
    Ok(HandleGuard::new(handle))
}

fn check_status(status: BoyodbStatus) -> Result<()> {
    if status == BoyodbStatus::Ok {
        return Ok(());
    }
    let detail = last_error_message();
    let kind = match status {
        BoyodbStatus::Ok => "ok",
        BoyodbStatus::InvalidArgument => "invalid argument",
        BoyodbStatus::NotFound => "not found",
        BoyodbStatus::Internal => "internal error",
        BoyodbStatus::NotImplemented => "not implemented",
        BoyodbStatus::Io => "io error",
        BoyodbStatus::Timeout => "query timeout",
    };
    match detail {
        Some(msg) if !msg.is_empty() => Err(anyhow!("{kind}: {msg}")),
        _ => Err(anyhow!(kind)),
    }
}

fn last_error_message() -> Option<String> {
    let mut buf = BufferGuard::new();
    let status = boyodb_core::ffi::boyodb_last_error_message(buf.as_mut_ptr());
    if status != BoyodbStatus::Ok {
        return None;
    }
    Some(String::from_utf8_lossy(buf.bytes()).into_owned())
}

struct HandleGuard {
    handle: *mut boyodb_core::ffi::BoyodbHandle,
}

impl HandleGuard {
    fn new(handle: *mut boyodb_core::ffi::BoyodbHandle) -> Self {
        Self { handle }
    }

    fn as_ptr(&self) -> *mut boyodb_core::ffi::BoyodbHandle {
        self.handle
    }
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // SAFETY: handle is non-null and owned by this guard
            boyodb_core::ffi::boyodb_close(self.handle);
        }
    }
}

struct BufferGuard {
    buf: OwnedBuffer,
}

impl BufferGuard {
    fn new() -> Self {
        Self {
            buf: OwnedBuffer {
                data: std::ptr::null_mut(),
                len: 0,
                capacity: 0,
                destructor: None,
                destructor_state: std::ptr::null_mut(),
            },
        }
    }

    fn as_mut_ptr(&mut self) -> *mut OwnedBuffer {
        &mut self.buf
    }

    fn bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.buf.data, self.buf.len) }
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        if !self.buf.data.is_null() {
            // SAFETY: buffer was initialized by FFI and is owned by this guard
            boyodb_core::ffi::boyodb_free_buffer(&mut self.buf);
        }
    }
}
