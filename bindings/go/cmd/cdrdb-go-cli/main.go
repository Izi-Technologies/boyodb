package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"boyodb/bindings/go/boyodb"
)

func usage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  boyodb-go-cli ingest <dataDir> <ipcFile> [db.table] [shardKey]
  boyodb-go-cli query <dataDir> "<SQL>"
  boyodb-go-cli create-db <dataDir> <name>
  boyodb-go-cli create-table <dataDir> <db.table> [schema.json]
  boyodb-go-cli list-dbs <dataDir>
  boyodb-go-cli list-tables <dataDir> [db]
  boyodb-go-cli manifest <dataDir>
  boyodb-go-cli import-manifest <dataDir> <jsonFile> [--overwrite]
  boyodb-go-cli health <dataDir>`)
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "ingest":
		if len(args) < 2 {
			usage(os.Stderr)
			os.Exit(1)
		}
		dataDir := args[0]
		ipcFile := args[1]
		var database, table string
		if len(args) >= 3 && strings.Contains(args[2], ".") {
			parts := strings.SplitN(args[2], ".", 2)
			database, table = parts[0], parts[1]
		}
		var shardKey uint64
		var hasShard bool
		if len(args) >= 4 {
			if v, err := strconv.ParseUint(args[3], 10, 64); err == nil {
				shardKey = v
				hasShard = true
			}
		}
		if err := cmdIngest(dataDir, ipcFile, database, table, shardKey, hasShard); err != nil {
			fail(err)
		}
	case "query":
		if len(args) < 2 {
			usage(os.Stderr)
			os.Exit(1)
		}
		dataDir := args[0]
		sql := args[1]
		if err := cmdQuery(dataDir, sql); err != nil {
			fail(err)
		}
	case "create-db":
		if len(args) < 2 {
			usage(os.Stderr)
			os.Exit(1)
		}
		if err := cmdCreateDB(args[0], args[1]); err != nil {
			fail(err)
		}
	case "create-table":
		if len(args) < 2 || !strings.Contains(args[1], ".") {
			usage(os.Stderr)
			os.Exit(1)
		}
		dataDir := args[0]
		parts := strings.SplitN(args[1], ".", 2)
		database, table := parts[0], parts[1]
		var schema string
		if len(args) >= 3 {
			b, err := os.ReadFile(args[2])
			if err != nil {
				fail(err)
			}
			schema = string(b)
		}
		if err := cmdCreateTable(dataDir, database, table, schema); err != nil {
			fail(err)
		}
	case "list-dbs":
		if len(args) < 1 {
			usage(os.Stderr)
			os.Exit(1)
		}
		if err := cmdListDBs(args[0]); err != nil {
			fail(err)
		}
	case "list-tables":
		if len(args) < 1 {
			usage(os.Stderr)
			os.Exit(1)
		}
		dataDir := args[0]
		var database string
		if len(args) >= 2 {
			database = args[1]
		}
		if err := cmdListTables(dataDir, database); err != nil {
			fail(err)
		}
	case "manifest":
		if len(args) < 1 {
			usage(os.Stderr)
			os.Exit(1)
		}
		if err := cmdManifest(args[0]); err != nil {
			fail(err)
		}
	case "import-manifest":
		fs := flag.NewFlagSet("import-manifest", flag.ExitOnError)
		overwrite := fs.Bool("overwrite", false, "overwrite manifest")
		_ = fs.Parse(args)
		parsed := fs.Args()
		if len(parsed) < 2 {
			usage(os.Stderr)
			os.Exit(1)
		}
		dataDir := parsed[0]
		jsonFile := parsed[1]
		if err := cmdImportManifest(dataDir, jsonFile, *overwrite); err != nil {
			fail(err)
		}
	case "health":
		if len(args) < 1 {
			usage(os.Stderr)
			os.Exit(1)
		}
		if err := cmdHealth(args[0]); err != nil {
			fail(err)
		}
	default:
		usage(os.Stderr)
		os.Exit(1)
	}
}

func openHandle(dataDir string, allowManifestImport bool) (*boyodb.Handle, error) {
	opts := boyodb.OpenOptions{
		DataDir:             filepath.Clean(dataDir),
		ShardCount:          1,
		WalMaxBytes:         64 * 1024 * 1024,
		WalMaxSegments:      4,
		AllowManifestImport: allowManifestImport,
	}
	return boyodb.Open(opts)
}

func cmdIngest(dataDir, ipcFile, database, table string, shardKey uint64, hasShard bool) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	ipc, err := os.ReadFile(ipcFile)
	if err != nil {
		return err
	}
	if database != "" || table != "" {
		return h.IngestInto(ipc, uint64(time.Now().UnixMicro()), shardKey, hasShard, database, table)
	}
	if hasShard {
		return h.IngestWithShard(ipc, uint64(time.Now().UnixMicro()), shardKey)
	}
	return h.Ingest(ipc, uint64(time.Now().UnixMicro()))
}

func cmdQuery(dataDir, sql string) error {
	if err := validateSelect(sql); err != nil {
		return err
	}
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	buf, err := h.Query(sql, 10_000)
	if err != nil {
		return err
	}
	rows, err := boyodb.DecodeRows(buf)
	if err != nil {
		// Fallback: print raw IPC bytes if decode fails (e.g., MIN/MAX).
		fmt.Println(string(buf))
		return nil
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

// validateSelect ensures we only run a single SELECT statement via the CLI to
// avoid accidental multi-statement/injection-style usage. The underlying engine
// only supports SELECTs, but we guard here defensively for CLI inputs.
func validateSelect(sql string) error {
	s := strings.TrimSpace(strings.ToLower(sql))
	if !strings.HasPrefix(s, "select") {
		return fmt.Errorf("only SELECT statements are allowed")
	}
	if strings.ContainsAny(s, ";") {
		return fmt.Errorf("multiple statements are not allowed")
	}
	return nil
}

func cmdCreateDB(dataDir, name string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	if err := h.CreateDatabase(name); err != nil {
		return err
	}
	fmt.Printf("database created: %s\n", name)
	return nil
}

func cmdCreateTable(dataDir, database, table, schema string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	if err := h.CreateTable(database, table, schema); err != nil {
		return err
	}
	fmt.Printf("table created: %s.%s\n", database, table)
	return nil
}

func cmdListDBs(dataDir string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	dbs, err := h.ListDatabases()
	if err != nil {
		return err
	}
	for _, db := range dbs {
		fmt.Println(db)
	}
	return nil
}

func cmdListTables(dataDir, database string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	tables, err := h.ListTables(database)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if t.SchemaJSON != nil {
			fmt.Printf("%s.%s (schema: %s)\n", t.Database, t.Name, *t.SchemaJSON)
		} else {
			fmt.Printf("%s.%s\n", t.Database, t.Name)
		}
	}
	return nil
}

func cmdManifest(dataDir string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	payload, err := h.Manifest()
	if err != nil {
		return err
	}
	fmt.Println(string(payload))
	return nil
}

func cmdImportManifest(dataDir, jsonFile string, overwrite bool) error {
	h, err := openHandle(dataDir, true)
	if err != nil {
		return err
	}
	defer h.Close()
	payload, err := os.ReadFile(jsonFile)
	if err != nil {
		return err
	}
	if err := h.ImportManifest(payload, overwrite); err != nil {
		return err
	}
	fmt.Println("manifest imported")
	return nil
}

func cmdHealth(dataDir string) error {
	h, err := openHandle(dataDir, false)
	if err != nil {
		return err
	}
	defer h.Close()
	if err := h.HealthCheck(); err != nil {
		return err
	}
	fmt.Println("healthy")
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
