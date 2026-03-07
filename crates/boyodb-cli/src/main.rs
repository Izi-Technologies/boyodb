use anyhow::{anyhow, Result};
use boyodb_core::types::{BoyodbStatus, OwnedBuffer};
use clap::{Args, Parser, Subcommand};
use std::ffi::CString;
use std::io::{BufRead, Write};
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

/// Common server connection arguments
#[derive(Args, Clone, Debug)]
struct ServerArgs {
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
    /// Get cluster status from a running boyodb-server
    ClusterStatus {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Create a point-in-time backup on the server
    CreateBackup {
        #[command(flatten)]
        server: ServerArgs,
        /// Optional backup label
        #[arg(long)]
        label: Option<String>,
    },
    /// List available backups on the server
    ListBackups {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Recover database to a specific point in time
    RecoverTo {
        #[command(flatten)]
        server: ServerArgs,
        /// Target timestamp (ISO 8601 format, e.g., 2024-01-15T14:30:00)
        #[arg(long, conflicts_with = "lsn")]
        timestamp: Option<String>,
        /// Target LSN (Log Sequence Number)
        #[arg(long, conflicts_with = "timestamp")]
        lsn: Option<u64>,
    },
    /// Show transaction status and active transactions
    TxnStatus {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Execute a SQL query against a running server
    #[command(name = "sql")]
    Sql {
        #[command(flatten)]
        server: ServerArgs,
        /// SQL query to execute
        query: String,
        /// Database to use
        #[arg(short, long)]
        database: Option<String>,
        /// Output format (table, csv, json)
        #[arg(short, long, default_value = "table")]
        format: String,
    },
    /// Create a database on a running server
    #[command(name = "server-create-db")]
    RemoteCreateDb {
        #[command(flatten)]
        server: ServerArgs,
        /// Database name
        name: String,
    },
    /// Drop a database on a running server
    #[command(name = "server-drop-db")]
    RemoteDropDb {
        #[command(flatten)]
        server: ServerArgs,
        /// Database name
        name: String,
        /// Skip confirmation prompt
        #[arg(long)]
        force: bool,
    },
    /// List databases on a running server
    #[command(name = "server-databases")]
    RemoteListDbs {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// List tables on a running server
    #[command(name = "server-tables")]
    RemoteListTables {
        #[command(flatten)]
        server: ServerArgs,
        /// Database to filter by
        #[arg(short, long)]
        database: Option<String>,
    },
    /// Describe a table schema on a running server
    #[command(name = "server-describe")]
    RemoteDescribe {
        #[command(flatten)]
        server: ServerArgs,
        /// Table name (database.table format)
        table: String,
    },
    /// Vacuum/compact a table on a running server
    #[command(name = "server-vacuum")]
    RemoteVacuum {
        #[command(flatten)]
        server: ServerArgs,
        /// Database name
        database: String,
        /// Table name
        table: String,
        /// Full vacuum (more thorough)
        #[arg(long)]
        full: bool,
    },
    /// Export data to a file
    Export {
        #[command(flatten)]
        server: ServerArgs,
        /// SQL query to export
        #[arg(short, long)]
        query: String,
        /// Output file path
        #[arg(short, long)]
        output: PathBuf,
        /// Output format (csv, json, parquet)
        #[arg(short, long, default_value = "csv")]
        format: String,
    },
    /// Import data from a file
    Import {
        #[command(flatten)]
        server: ServerArgs,
        /// Target table (database.table format)
        #[arg(short, long)]
        table: String,
        /// Input file path
        #[arg(short, long)]
        input: PathBuf,
        /// Input format (csv, json) - auto-detected from extension if not specified
        #[arg(short, long)]
        format: Option<String>,
        /// CSV has header row
        #[arg(long, default_value = "true")]
        header: bool,
        /// Batch size for inserts (default: 1000 rows per batch)
        #[arg(long, default_value = "1000")]
        batch_size: usize,
        /// Show progress during import
        #[arg(long)]
        progress: bool,
    },
    /// Show server metrics
    Metrics {
        #[command(flatten)]
        server: ServerArgs,
        /// Output format (table, json)
        #[arg(short, long, default_value = "table")]
        format: String,
    },
    /// Show server info and version
    Info {
        #[command(flatten)]
        server: ServerArgs,
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
    /// Compact (vacuum) a specific table to reduce segment count
    Vacuum {
        data_dir: PathBuf,
        /// Database name
        database: String,
        /// Table name
        table: String,
        /// Perform VACUUM FULL (merges all segments, slower but more thorough)
        #[arg(long, default_value_t = false)]
        full: bool,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    /// Compact all eligible tables in the database
    CompactAll {
        data_dir: PathBuf,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
    /// Repair and verify database integrity
    ///
    /// Scan manifest and verify all referenced segments exist,
    /// detect corrupted segments, and optionally repair issues.
    Repair {
        /// Data directory path
        data_dir: PathBuf,
        /// Just verify, don't make any changes
        #[arg(long)]
        verify_only: bool,
        /// Remove orphaned segment files not in manifest
        #[arg(long)]
        remove_orphaned: bool,
        /// Remove missing segment entries from manifest
        #[arg(long)]
        remove_missing: bool,
        /// Rebuild manifest from segments on disk
        #[arg(long)]
        rebuild_manifest: bool,
        /// Verify checksums of all segments
        #[arg(long)]
        verify_checksums: bool,
        #[command(flatten)]
        engine: LocalEngineArgs,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Shell {
            host,
            token,
            user,
            password,
            database,
            tls,
            tls_ca,
            tls_skip_verify,
            command,
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
                    // Username provided without password - prompt for it
                    let pwd =
                        rpassword::prompt_password("Password: ").unwrap_or_else(|_| String::new());
                    Some((u.clone(), pwd))
                }
                _ => None,
            };
            shell::run_shell(
                host.as_deref(),
                token.clone(),
                database.clone(),
                tls_config,
                auth,
                command.clone(),
            )?;
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
                    let pwd =
                        rpassword::prompt_password("Password: ").unwrap_or_else(|_| String::new());
                    Some((u.clone(), pwd))
                }
                _ => None,
            };
            let stats = shell::fetch_wal_stats(host.as_deref(), token.clone(), tls_config, auth)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
        Commands::ClusterStatus { server } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW CLUSTER STATUS",
            )?;
            println!("{}", result);
        }
        Commands::CreateBackup { server, label } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = match label {
                Some(l) => format!("CREATE BACKUP '{}'", l),
                None => "CREATE BACKUP".to_string(),
            };
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
            )?;
            println!("Backup created successfully");
            println!("{}", result);
        }
        Commands::ListBackups { server } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW BACKUPS",
            )?;
            println!("{}", result);
        }
        Commands::RecoverTo {
            server,
            timestamp,
            lsn,
        } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = if let Some(ts) = timestamp {
                format!("RECOVER TO TIMESTAMP '{}'", ts)
            } else if let Some(l) = lsn {
                format!("RECOVER TO LSN {}", l)
            } else {
                return Err(anyhow!("Either --timestamp or --lsn must be specified"));
            };
            println!("Starting recovery...");
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
            )?;
            println!("Recovery completed");
            println!("{}", result);
        }
        Commands::TxnStatus { server } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW TRANSACTIONS",
            )?;
            println!("{}", result);
        }
        Commands::Sql {
            server,
            query,
            database,
            format,
        } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                query,
                database.as_deref(),
                &format,
            )?;
            println!("{}", result);
        }
        Commands::RemoteCreateDb { server, name } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = format!("CREATE DATABASE {}", name);
            shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
            )?;
            println!("Database '{}' created successfully", name);
        }
        Commands::RemoteDropDb {
            server,
            name,
            force,
        } => {
            if !force {
                print!("Are you sure you want to drop database '{}'? [y/N] ", name);
                std::io::Write::flush(&mut std::io::stdout())?;
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Aborted");
                    return Ok(());
                }
            }
            let (tls_config, auth) = parse_server_args(server);
            let sql = format!("DROP DATABASE {}", name);
            shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
            )?;
            println!("Database '{}' dropped successfully", name);
        }
        Commands::RemoteListDbs { server } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW DATABASES",
                None,
                "table",
            )?;
            println!("{}", result);
        }
        Commands::RemoteListTables { server, database } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = match database {
                Some(db) => format!("SHOW TABLES IN {}", db),
                None => "SHOW TABLES".to_string(),
            };
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
                None,
                "table",
            )?;
            println!("{}", result);
        }
        Commands::RemoteDescribe { server, table } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = format!("DESCRIBE {}", table);
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
                None,
                "table",
            )?;
            println!("{}", result);
        }
        Commands::RemoteVacuum {
            server,
            database,
            table,
            full,
        } => {
            let (tls_config, auth) = parse_server_args(server);
            let sql = if *full {
                format!("VACUUM FULL {}.{}", database, table)
            } else {
                format!("VACUUM {}.{}", database, table)
            };
            println!("Vacuuming {}.{}...", database, table);
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                &sql,
            )?;
            println!("{}", result);
        }
        Commands::Export {
            server,
            query,
            output,
            format,
        } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                query,
                None,
                &format,
            )?;
            std::fs::write(output, &result)?;
            println!("Exported to {}", output.display());
        }
        Commands::Import {
            server,
            table,
            input,
            format,
            header,
            batch_size,
            progress,
        } => {
            let (tls_config, auth) = parse_server_args(server);

            // Detect format from extension if not specified
            let fmt = format.clone().unwrap_or_else(|| {
                input
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("csv")
                    .to_string()
            });

            let data = std::fs::read_to_string(&input)?;
            let parts: Vec<&str> = table.split('.').collect();
            if parts.len() != 2 {
                return Err(anyhow!("Table must be in database.table format"));
            }
            let (db, tbl) = (parts[0], parts[1]);

            let start_time = std::time::Instant::now();
            let mut total_rows = 0usize;
            let mut batch_count = 0usize;

            match fmt.as_str() {
                "csv" => {
                    // For CSV, we can batch by splitting the data into chunks
                    let lines: Vec<&str> = data.lines().collect();
                    let header_line = if *header && !lines.is_empty() {
                        Some(lines[0])
                    } else {
                        None
                    };
                    let data_start = if *header { 1 } else { 0 };
                    let data_lines = &lines[data_start..];
                    total_rows = data_lines.len();

                    // Process in batches
                    for chunk in data_lines.chunks(*batch_size) {
                        let mut batch_data = String::new();
                        if let Some(hdr) = header_line {
                            batch_data.push_str(hdr);
                            batch_data.push('\n');
                        }
                        for line in chunk {
                            batch_data.push_str(line);
                            batch_data.push('\n');
                        }
                        let sql = format!(
                            "INSERT INTO {}.{} FORMAT CSV {} DATA '{}'",
                            db,
                            tbl,
                            if *header { "HEADER" } else { "" },
                            batch_data.replace('\'', "''")
                        );
                        shell::execute_sql_command(
                            server.host.as_deref(),
                            server.token.clone(),
                            tls_config.clone(),
                            auth.clone(),
                            &sql,
                        )?;
                        batch_count += 1;
                        if *progress {
                            let processed = batch_count * batch_size.min(&chunk.len());
                            eprint!("\rImported {} / {} rows ({} batches)...",
                                processed.min(total_rows), total_rows, batch_count);
                        }
                    }
                }
                "json" => {
                    // Parse JSON and batch multiple rows into single INSERT
                    let rows: Vec<serde_json::Value> = serde_json::from_str(&data)?;
                    total_rows = rows.len();

                    if rows.is_empty() {
                        println!("No rows to import");
                        return Ok(());
                    }

                    // Get column names from first row
                    let first_obj = rows[0].as_object()
                        .ok_or_else(|| anyhow!("JSON must be an array of objects"))?;
                    let columns: Vec<&str> = first_obj.keys().map(|s| s.as_str()).collect();
                    let col_list = columns.join(", ");

                    // Process in batches
                    for chunk in rows.chunks(*batch_size) {
                        let values_list: Vec<String> = chunk.iter()
                            .filter_map(|row| {
                                row.as_object().map(|obj| {
                                    let vals: Vec<String> = columns.iter()
                                        .map(|col| {
                                            match obj.get(*col) {
                                                Some(serde_json::Value::String(s)) => format!("'{}'", s.replace('\'', "''")),
                                                Some(serde_json::Value::Null) | None => "NULL".to_string(),
                                                Some(other) => other.to_string(),
                                            }
                                        })
                                        .collect();
                                    format!("({})", vals.join(", "))
                                })
                            })
                            .collect();

                        if !values_list.is_empty() {
                            let sql = format!(
                                "INSERT INTO {}.{} ({}) VALUES {}",
                                db, tbl, col_list, values_list.join(", ")
                            );
                            shell::execute_sql_command(
                                server.host.as_deref(),
                                server.token.clone(),
                                tls_config.clone(),
                                auth.clone(),
                                &sql,
                            )?;
                            batch_count += 1;
                            if *progress {
                                let processed = batch_count * batch_size;
                                eprint!("\rImported {} / {} rows ({} batches)...",
                                    processed.min(total_rows), total_rows, batch_count);
                            }
                        }
                    }
                }
                _ => return Err(anyhow!("Unsupported format: {}", fmt)),
            }

            if *progress {
                eprintln!(); // New line after progress
            }
            let elapsed = start_time.elapsed();
            let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
                total_rows as f64 / elapsed.as_secs_f64()
            } else {
                total_rows as f64
            };
            println!(
                "Import completed: {} rows in {:.2}s ({:.0} rows/sec, {} batches)",
                total_rows, elapsed.as_secs_f64(), rows_per_sec, batch_count
            );
        }
        Commands::Metrics { server, format } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_query(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW METRICS",
                None,
                &format,
            )?;
            println!("{}", result);
        }
        Commands::Info { server } => {
            let (tls_config, auth) = parse_server_args(server);
            let result = shell::execute_sql_command(
                server.host.as_deref(),
                server.token.clone(),
                tls_config,
                auth,
                "SHOW SERVER INFO",
            )?;
            println!("{}", result);
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
            // Store CStrings in variables to prevent use-after-free
            // (temporary CStrings would be dropped immediately after as_ptr())
            let db_cstr = CString::new(db)?;
            let tbl_cstr = CString::new(tbl)?;
            let status = boyodb_core::ffi::boyodb_ingest_ipc_v3(
                handle.as_ptr(),
                payload.as_ptr(),
                payload.len(),
                0,
                *shard_hint,
                true,
                db_cstr.as_ptr(),
                true,
                tbl_cstr.as_ptr(),
                true,
            );
            check_status(status)?;
            println!("ingest ok");
        }
        Commands::Query {
            data_dir,
            sql,
            engine,
        } => {
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
        Commands::CreateDb {
            data_dir,
            name,
            engine,
        } => {
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
        Commands::ListTables {
            data_dir,
            database,
            engine,
        } => {
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
        Commands::Restore {
            data_dir,
            input,
            engine,
        } => {
            let handle = open(data_dir, true, engine)?;
            let payload = std::fs::read(input)?;
            let status = boyodb_core::ffi::boyodb_apply_bundle(
                handle.as_ptr(),
                payload.as_ptr(),
                payload.len(),
            );
            check_status(status)?;
            println!("restore applied from {}", input.display());
        }
        Commands::Vacuum {
            data_dir,
            database,
            table,
            full,
            engine,
        } => {
            let handle = open(data_dir, false, engine)?;
            let c_db = CString::new(database.as_str())?;
            let c_tbl = CString::new(table.as_str())?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_vacuum(
                handle.as_ptr(),
                c_db.as_ptr(),
                c_tbl.as_ptr(),
                *full,
                buf.as_mut_ptr(),
            );
            check_status(status)?;
            let json: serde_json::Value = serde_json::from_slice(buf.bytes())?;
            println!("Vacuum completed for {}.{}:", database, table);
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        Commands::CompactAll { data_dir, engine } => {
            let handle = open(data_dir, false, engine)?;
            let mut buf = BufferGuard::new();
            let status = boyodb_core::ffi::boyodb_compact_all(handle.as_ptr(), buf.as_mut_ptr());
            check_status(status)?;
            let json: serde_json::Value = serde_json::from_slice(buf.bytes())?;
            println!("Compaction completed:");
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        Commands::Repair {
            data_dir,
            verify_only,
            remove_orphaned,
            remove_missing,
            rebuild_manifest,
            verify_checksums,
            engine,
        } => {
            run_repair(
                data_dir,
                *verify_only,
                *remove_orphaned,
                *remove_missing,
                *rebuild_manifest,
                *verify_checksums,
                engine,
            )?;
        }
    }

    Ok(())
}

fn parse_server_args(server: &ServerArgs) -> (Option<shell::TlsConfig>, Option<(String, String)>) {
    let tls_config = if server.tls {
        Some(shell::TlsConfig {
            ca_path: server.tls_ca.clone(),
            skip_verify: server.tls_skip_verify,
        })
    } else {
        None
    };
    let auth = match (&server.user, &server.password) {
        (Some(u), Some(p)) => Some((u.clone(), p.clone())),
        (Some(u), None) => {
            let pwd = rpassword::prompt_password("Password: ").unwrap_or_else(|_| String::new());
            Some((u.clone(), pwd))
        }
        _ => None,
    };
    (tls_config, auth)
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

/// Run repair operations on a database
fn run_repair(
    data_dir: &PathBuf,
    verify_only: bool,
    remove_orphaned: bool,
    remove_missing: bool,
    rebuild_manifest: bool,
    verify_checksums: bool,
    engine: &LocalEngineArgs,
) -> Result<()> {
    use boyodb_core::{Db, EngineConfig};
    use std::collections::HashSet;

    println!("BoyoDB Repair Tool");
    println!("==================");
    println!("Data directory: {}", data_dir.display());
    println!();

    // Build engine config
    let mut cfg = EngineConfig::new(data_dir.clone(), 1);
    cfg.skip_damaged_segments = true; // Don't fail on damaged segments during repair
    if let Some(ref warm) = engine.tier_warm_compression {
        cfg = cfg.with_tier_warm_compression(Some(warm.clone()));
    }
    if let Some(ref cold) = engine.tier_cold_compression {
        cfg = cfg.with_tier_cold_compression(Some(cold.clone()));
    }
    cfg = cfg
        .with_cache_hot_segments(engine.cache_hot_segments)
        .with_cache_warm_segments(engine.cache_warm_segments)
        .with_cache_cold_segments(engine.cache_cold_segments);

    // Open database
    let db = Db::open(cfg).map_err(|e| anyhow!("Failed to open database: {}", e))?;

    // Run manifest check
    println!("Checking manifest integrity...");
    let check_result = db
        .check_manifest()
        .map_err(|e| anyhow!("Failed to check manifest: {}", e))?;

    println!("  Total segments: {}", check_result.total_segments);
    println!("  Total databases: {}", check_result.total_databases);
    println!("  Total tables: {}", check_result.total_tables);
    println!("  Manifest version: {}", check_result.manifest_version);
    println!();

    // Report missing segments
    if !check_result.missing_segments.is_empty() {
        println!(
            "Missing segments ({}):",
            check_result.missing_segments.len()
        );
        for seg in &check_result.missing_segments {
            println!(
                "  - {} ({}.{}) - {} bytes",
                seg.segment_id, seg.database, seg.table, seg.size_bytes
            );
        }
        println!();
    } else {
        println!("No missing segments found.");
    }

    // Report corrupted segments (if checksum verification requested)
    if verify_checksums && !check_result.corrupted_segments.is_empty() {
        println!(
            "Corrupted segments ({}):",
            check_result.corrupted_segments.len()
        );
        for seg in &check_result.corrupted_segments {
            println!(
                "  - {} ({}.{}) - expected checksum {:016x}, got {:016x}",
                seg.segment_id, seg.database, seg.table, seg.expected_checksum, seg.actual_checksum
            );
        }
        println!();
    } else if verify_checksums {
        println!("No corrupted segments found (checksums verified).");
    }

    // Report orphaned files
    if !check_result.orphaned_files.is_empty() {
        println!("Orphaned files ({}):", check_result.orphaned_files.len());
        for file in &check_result.orphaned_files {
            println!("  - {} - {} bytes", file.path, file.size_bytes);
        }
        println!();
    } else {
        println!("No orphaned files found.");
    }

    // If verify only, stop here
    if verify_only {
        println!();
        println!("Verification complete (no changes made).");
        return Ok(());
    }

    // Perform repairs
    let mut repairs_made = 0;

    // Remove missing segment entries from manifest
    if remove_missing && !check_result.missing_segments.is_empty() {
        println!();
        println!("Removing missing segment entries from manifest...");
        let tables: HashSet<(String, String)> = check_result
            .missing_segments
            .iter()
            .map(|s| (s.database.clone(), s.table.clone()))
            .collect();

        for (database, table) in tables {
            match db.repair_segments(Some(database.as_str()), Some(table.as_str())) {
                Ok(removed) => {
                    if !removed.is_empty() {
                        println!("  Removed {} segments from {}.{}", removed.len(), database, table);
                        repairs_made += removed.len();
                    }
                }
                Err(e) => {
                    eprintln!("  Failed to repair {}.{}: {}", database, table, e);
                }
            }
        }
    }

    // Remove orphaned files
    if remove_orphaned && !check_result.orphaned_files.is_empty() {
        println!();
        println!("Removing orphaned files...");
        for file in &check_result.orphaned_files {
            let path = std::path::Path::new(&file.path);
            match std::fs::remove_file(path) {
                Ok(()) => {
                    println!("  Removed: {}", file.path);
                    repairs_made += 1;
                }
                Err(e) => {
                    eprintln!("  Failed to remove {}: {}", file.path, e);
                }
            }
        }
    }

    // Rebuild manifest (not implemented yet - complex operation)
    if rebuild_manifest {
        println!();
        println!("Manifest rebuild is not yet implemented.");
        println!("This would scan all .ipc files and rebuild the manifest from scratch.");
    }

    println!();
    if repairs_made > 0 {
        println!("Repair complete. {} items repaired.", repairs_made);
    } else {
        println!("Repair complete. No changes were necessary.");
    }

    Ok(())
}
