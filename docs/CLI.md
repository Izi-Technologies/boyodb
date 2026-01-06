# CLI Reference

Complete reference for the BoyoDB command-line interface.

## Installation

```bash
# From source
cargo install --path crates/boyodb-cli

# Or build manually
cargo build -p boyodb-cli --release
./target/release/boyodb-cli --help
```

## Commands

### Server Connection

#### shell

Start an interactive SQL shell.

```bash
boyodb-cli shell [OPTIONS]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--host <HOST>` | Server address | `localhost:8765` |
| `--token <TOKEN>` | Authentication token | None |
| `--database <DB>` | Default database | None |
| `--tls` | Enable TLS | false |
| `--ca-cert <PATH>` | CA certificate path | System CA |
| `--insecure` | Skip TLS verification | false |
| `--user <USER>` | Auto-login username | None |

**Examples:**

```bash
# Basic connection
boyodb-cli shell --host localhost:8765

# With authentication
boyodb-cli shell --host db.example.com:8765 --token secret123

# With TLS
boyodb-cli shell --host secure.example.com:8765 --tls --ca-cert /path/to/ca.pem

# Auto-login
boyodb-cli shell --host localhost:8765 --user admin
```

### Direct Operations

Local engine options (applies to commands that use `--data-dir`):

| Option | Description | Required |
|--------|-------------|----------|
| `--tier-warm-compression <ALGO>` | Recompress Warm segments (e.g. `zstd`) | No |
| `--tier-cold-compression <ALGO>` | Recompress Cold segments (e.g. `zstd`) | No |
| `--cache-hot-segments <BOOL>` | Cache Hot segments in memory | No (true) |
| `--cache-warm-segments <BOOL>` | Cache Warm segments in memory | No (true) |
| `--cache-cold-segments <BOOL>` | Cache Cold segments in memory | No (false) |

Server flags (boyodb-server startup):

| Option | Description | Required |
|--------|-------------|----------|
| `--plan-cache-size <N>` | Query plan cache entries | No |
| `--plan-cache-ttl-secs <N>` | Query plan cache TTL (seconds) | No |
| `--prepared-cache-size <N>` | Prepared statement cache entries | No |
| `--prepared-cache-ttl-secs <N>` | Prepared statement cache TTL (seconds) | No |
| `--index-granularity-rows <N>` | Row count per sorted batch when writing hot segments | No |
| `--tier-warm-compression <ALGO>` | Recompress Warm segments (e.g. `zstd`) | No |
| `--tier-cold-compression <ALGO>` | Recompress Cold segments (e.g. `zstd`) | No |
| `--cache-hot-segments <BOOL>` | Cache Hot segments in memory | No |
| `--cache-warm-segments <BOOL>` | Cache Warm segments in memory | No |
| `--cache-cold-segments <BOOL>` | Cache Cold segments in memory | No |
| `--wal-sync-bytes <BYTES>` | Fsync WAL after N bytes (0 = strict mode) | No |
| `--wal-sync-interval-ms <MS>` | Fsync WAL every N ms (0 = immediate) | No |

#### ingest

Ingest Arrow IPC data file.

```bash
boyodb-cli ingest --data-dir <DIR> --ipc-file <FILE> [OPTIONS]
```

**Options:**

| Option | Description | Required |
|--------|-------------|----------|
| `--data-dir <DIR>` | Data directory | Yes |
| `--ipc-file <FILE>` | Arrow IPC file path | Yes |
| `--database <DB>` | Target database | No |
| `--table <TABLE>` | Target table | No |

**Example:**

```bash
boyodb-cli ingest --data-dir /var/lib/boyodb --ipc-file batch.ipc --database analytics --table events
```

#### query

Execute a SQL query.

```bash
boyodb-cli query --data-dir <DIR> --sql <SQL> [OPTIONS]
```

**Options:**

| Option | Description | Required |
|--------|-------------|----------|
| `--data-dir <DIR>` | Data directory | Yes |
| `--sql <SQL>` | SQL query | Yes |
| `--timeout <MS>` | Query timeout | No (10000) |

**Example:**

```bash
boyodb-cli query --data-dir /var/lib/boyodb --sql "SELECT COUNT(*) FROM analytics.events"
```

#### create-db

Create a database.

```bash
boyodb-cli create-db --data-dir <DIR> --name <NAME>
```

**Example:**

```bash
boyodb-cli create-db --data-dir /var/lib/boyodb --name analytics
```

#### create-table

Create a table.

```bash
boyodb-cli create-table --data-dir <DIR> --database <DB> --table <TABLE> [--schema-json-file <FILE>]
```

**Example:**

```bash
boyodb-cli create-table --data-dir /var/lib/boyodb --database analytics --table events --schema-json-file schema.json
```

`schema.json` fields can include optional `encoding`: `dictionary`, `delta`, or `rle` (delta supports `int64` and `timestamp`).

#### list-dbs

List all databases.

```bash
boyodb-cli list-dbs --data-dir <DIR>
```

#### list-tables

List tables in a database.

```bash
boyodb-cli list-tables --data-dir <DIR> --database <DB>
```

#### manifest

Show the manifest.

```bash
boyodb-cli manifest --data-dir <DIR>
```

#### import-manifest

Import a manifest from JSON.

```bash
boyodb-cli import-manifest --data-dir <DIR> --json-file <FILE> [--overwrite]
```

#### health

Check database health.

```bash
boyodb-cli health --data-dir <DIR>
```

#### checkpoint

Flush memtables + manifest and truncate WAL segments.

```bash
boyodb-cli checkpoint --data-dir <DIR>
```

#### wal-stats

Fetch WAL stats from a running server (superuser only).

```bash
boyodb-cli wal-stats --host <HOST:PORT> [--token <TOKEN>] [--user <USER> --password <PASS>] [--tls] [--tls-ca <FILE>] [--tls-skip-verify]
```

---

## Interactive Shell

For a detailed guide on using the interactive shell, including key bindings and meta-commands, see [SHELL.md](SHELL.md).

### Starting the Shell

```bash
boyodb-cli shell --host localhost:8765
```

### Shell Prompt

The prompt shows connection status:

```
boyodb>                          # Not logged in
admin@boyodb>                    # Logged in as admin
admin@boyodb[TLS]>               # TLS connection
admin@boyodb[TLS][analytics]>    # Current database set
```

### Meta-Commands

| Command | Description |
|---------|-------------|
| `\l` | List databases |
| `\dt [database]` | List tables |
| `\d <table>` | Describe table schema |
| `\du` | List users and roles |
| `\di` | List indexes |
| `\dv`, `\views` | List client-side views |
| `\c <database>` | Switch database |
| `\x` | Toggle expanded output |
| `\o [file]` | Redirect output to file |
| `\i <file>` | Execute SQL from file |
| `\ps` | List prepared statements |
| `\format [fmt]` | Set output format |
| `\pager [on\|off]` | Toggle pager |
| `\history [N]` | Show command history |
| `\history search X` | Search history |
| `\copy` | Import CSV file |
| `\q`, `quit`, `exit` | Exit shell |
| `\?`, `help`, `\h` | Show help |
| `clear` | Clear screen |
| `status` | Show connection status |

### Authentication Commands

```sql
login [username]     -- Log in (prompts for password)
logout               -- Log out
whoami               -- Show current user
```

### Output Formats

```bash
\format table      # ASCII table (default)
\format csv        # Comma-separated values
\format json       # Pretty JSON
\format vertical   # One column per line (like \x)
```

**Table Format:**
```
┌────┬──────────┐
│ id │ name     │
├────┼──────────┤
│ 1  │ Alice    │
│ 2  │ Bob      │
└────┴──────────┘
```

**CSV Format:**
```
id,name
1,Alice
2,Bob
```

**JSON Format:**
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

**Vertical Format:**
```
-[ RECORD 1 ]---------------
id   | 1
name | Alice
-[ RECORD 2 ]---------------
id   | 2
name | Bob
```

### File Operations

#### Execute SQL from File

```bash
\i /path/to/script.sql
```

The file should contain SQL statements separated by semicolons. Comments (`--`) are skipped.

#### Redirect Output

```bash
\o /path/to/output.txt    # Start writing to file
SELECT * FROM large_table;
\o -                      # Stop writing, reset to stdout
\o                        # Show current output destination
```

### Pager Support

```bash
\pager on     # Enable pager for large output
\pager off    # Disable pager
\pager        # Toggle pager
```

The pager uses `less` or `more` and activates when output exceeds terminal height. Set `PAGER` environment variable to use a custom pager.

### Tab Completion

The shell provides tab completion for:

- SQL keywords (`SELECT`, `FROM`, `WHERE`, etc.)
- Meta-commands (`\l`, `\dt`, `\du`, etc.)
- Database names (after `USE` or `\c`)
- Table names (after `FROM`, `JOIN`, `INTO`, etc.)

Press `Tab` to complete or show available options.

### Command History

History is saved to `~/.boyodb_history`.

**Keyboard Shortcuts:**

| Key | Action |
|-----|--------|
| `Up/Down` | Navigate history |
| `Ctrl+R` | Reverse search |
| `Ctrl+S` | Forward search |
| `Ctrl+C` | Cancel current line |
| `Ctrl+D` | Exit shell |

**History Commands:**

```bash
\history           # Show last 20 entries
\history 50        # Show last 50 entries
\history search X  # Search for pattern X
```

### Prepared Statements

```sql
-- Create prepared statement
PREPARE get_user AS SELECT * FROM users WHERE id = $1;

-- Execute with parameters
EXECUTE get_user (42);

-- List prepared statements
\ps

-- Remove prepared statement
DEALLOCATE get_user;
DEALLOCATE ALL;
```

### CSV Import

```bash
\copy users FROM '/path/to/data.csv' WITH (HEADER true, DELIMITER ',')
```

Options:
- `HEADER true|false` - File has header row
- `DELIMITER 'x'` - Field delimiter character

---

## Configuration File

The shell reads `~/.boyodbrc` for default settings.

```ini
# Server connection
host = "production-db.example.com:8765"
token = "my-secret-token"
database = "analytics"

# TLS settings
tls = true
ca_cert = "/etc/ssl/certs/ca-bundle.crt"

# Output preferences
format = "table"
timeout_ms = 60000

# Auto-login
user = "admin"
```

**Supported Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Server address | `localhost:8765` |
| `token` | Auth token | None |
| `database` | Default database | None |
| `tls` | Enable TLS | `false` |
| `ca_cert` | CA certificate path | System CA |
| `format` | Output format | `table` |
| `timeout_ms` | Query timeout | `30000` |
| `user` | Auto-login username | None |

**Security:** Set file permissions to `600`:
```bash
chmod 600 ~/.boyodbrc
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BoyoDB_HOST` | Default server address |
| `BoyoDB_TOKEN` | Authentication token |
| `PAGER` | Pager command for large output |
| `NO_COLOR` | Disable colored output |

---

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Connection error |
| 3 | Authentication error |
| 4 | Query error |

---

## Examples

### Basic Session

```bash
$ boyodb-cli shell --host localhost:8765

boyodb> login admin
Password: ********
Logged in as admin

admin@boyodb> CREATE DATABASE mydb;
Database created

admin@boyodb> \c mydb
Switched to database: mydb

admin@boyodb[mydb]> CREATE TABLE users (id INT64, name STRING);
Table created

admin@boyodb[mydb]> INSERT INTO users (id, name) VALUES (1, 'Alice');
1 row inserted

admin@boyodb[mydb]> SELECT * FROM users;
┌────┬───────┐
│ id │ name  │
├────┼───────┤
│ 1  │ Alice │
└────┴───────┘
1 row(s)

admin@boyodb[mydb]> \q
Bye!
```

### Scripted Usage

```bash
# Execute query and save to file
boyodb-cli shell --host localhost:8765 --token secret <<EOF
\o /tmp/report.csv
\format csv
SELECT * FROM analytics.events WHERE date > '2024-01-01';
\o -
EOF

# Execute SQL file
echo "SELECT COUNT(*) FROM users;" > query.sql
boyodb-cli shell --host localhost:8765 -i query.sql
```

### Batch Import

```bash
# Import CSV data
boyodb-cli shell --host localhost:8765 <<EOF
\copy users FROM '/data/users.csv' WITH (HEADER true)
EOF

# Or use the ingest command
boyodb-cli ingest --data-dir /var/lib/boyodb --ipc-file batch.ipc --database mydb --table events
```
