# BoyoDB Interactive Shell

BoyoDB provides a powerful interactive SQL shell (`bsql` or `boyodb-cli shell`) designed to be familiar to users of `psql` (PostgreSQL) and `mysql`.

## Quick Start

The easiest way to start the shell is using the `bsql` wrapper script:

```bash
# Connect to default localhost:8765
./scripts/bsql

# Connect to specific host
./scripts/bsql -H prod-db.example.com:8765

# Connect with authentication
./scripts/bsql -H prod-db.example.com:8765 --user admin
```

## Features

### Command History & Completion

- **History**: Use `Up`/`Down` arrows to navigate previous commands. History is persisted to `~/.boyodb_history`.
- **Search**: Press `Ctrl+R` to search history backward.
- **Tab Completion**: Press `Tab` to autocomplete keywords (`SELECT`, `FROM`), table names, and meta-commands.

### Meta-Commands

BoyoDB supports `psql`-style meta-commands (starting with `\`).

| Command | Description |
|---------|-------------|
| `\l` | List all databases |
| `\c <db>` | Connect/switch to a database |
| `\dt` | List tables in current database |
| `\d <table>` | Describe table schema (columns, types) |
| `\du` | List users and roles |
| `\x` | Toggle "Extended" (vertical) display mode |
| `\timing` | Toggle query execution timing |
| `\q` | Quit the shell |
| `\?` or `\h` | Show help for all commands |

### Output Formats

You can change how query results are displayed using `\format` or command-line args.

- **Table** (default): ASCII table with borders.
- **CSV**: Comma-separated values (useful for export).
- **JSON**: Pretty-printed JSON objects.
- **Vertical**: One row per block (toggled via `\x`).

Example of Vertical Output:
```text
boyodb> \x
Expanded display is on.

boyodb> SELECT * FROM users WHERE id=1;
-[ RECORD 1 ]---
id    | 1
name  | Alice
email | alice@example.com
```

### Scripting & Automation

You can run SQL scripts or single commands non-interactively:

```bash
# Run a single command
./scripts/bsql -c "SELECT count(*) FROM events"

# Run a SQL script file
./scripts/bsql -i migration.sql

# Output to a file
./scripts/bsql -c "SELECT * FROM large_table" -o output.csv --format csv
```

## Configuration

The shell looks for a configuration file at `~/.boyodbrc`. You can store connection details there to avoid typing them every time.

```toml
# ~/.boyodbrc
host = "localhost:8765"
user = "admin"
tls = true
format = "table"
# database = "analytics"
```
