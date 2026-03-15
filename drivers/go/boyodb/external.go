package boyodb

import (
	"fmt"
	"strings"
)

// ExternalTables provides helpers for querying external data sources.
type ExternalTables struct {
	client *Client
}

// External returns an ExternalTables instance.
func (c *Client) External() *ExternalTables {
	return &ExternalTables{client: c}
}

// QueryS3Options holds options for S3 queries.
type QueryS3Options struct {
	Format string
	Limit  int
}

// QueryS3 queries data from S3.
func (et *ExternalTables) QueryS3(path, sql string, opts *QueryS3Options) (*Result, error) {
	if sql == "" {
		sql = "SELECT *"
	}

	formatArg := ""
	limitClause := ""

	if opts != nil {
		if opts.Format != "" {
			formatArg = fmt.Sprintf(", '%s'", opts.Format)
		}
		if opts.Limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", opts.Limit)
		}
	}

	var fullSQL string
	if strings.TrimSpace(strings.ToUpper(sql)) == "SELECT *" {
		fullSQL = fmt.Sprintf("SELECT * FROM s3('%s'%s)%s", path, formatArg, limitClause)
	} else {
		fullSQL = fmt.Sprintf(`
			SELECT * FROM (
				%s
			) AS subq
			%s
		`, strings.ReplaceAll(sql, "FROM data", fmt.Sprintf("FROM s3('%s'%s)", path, formatArg)), limitClause)
	}

	return et.client.Query(fullSQL)
}

// QueryURL queries data from HTTP URL.
func (et *ExternalTables) QueryURL(url, sql string, opts *QueryS3Options) (*Result, error) {
	if sql == "" {
		sql = "SELECT *"
	}

	formatArg := ""
	limitClause := ""

	if opts != nil {
		if opts.Format != "" {
			formatArg = fmt.Sprintf(", '%s'", opts.Format)
		}
		if opts.Limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", opts.Limit)
		}
	}

	var fullSQL string
	if strings.TrimSpace(strings.ToUpper(sql)) == "SELECT *" {
		fullSQL = fmt.Sprintf("SELECT * FROM url('%s'%s)%s", url, formatArg, limitClause)
	} else {
		fullSQL = fmt.Sprintf(`
			SELECT * FROM (
				%s
			) AS subq
			%s
		`, strings.ReplaceAll(sql, "FROM data", fmt.Sprintf("FROM url('%s'%s)", url, formatArg)), limitClause)
	}

	return et.client.Query(fullSQL)
}

// QueryFile queries data from local file.
func (et *ExternalTables) QueryFile(path, sql string, opts *QueryS3Options) (*Result, error) {
	if sql == "" {
		sql = "SELECT *"
	}

	formatArg := ""
	limitClause := ""

	if opts != nil {
		if opts.Format != "" {
			formatArg = fmt.Sprintf(", '%s'", opts.Format)
		}
		if opts.Limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", opts.Limit)
		}
	}

	var fullSQL string
	if strings.TrimSpace(strings.ToUpper(sql)) == "SELECT *" {
		fullSQL = fmt.Sprintf("SELECT * FROM file('%s'%s)%s", path, formatArg, limitClause)
	} else {
		fullSQL = fmt.Sprintf(`
			SELECT * FROM (
				%s
			) AS subq
			%s
		`, strings.ReplaceAll(sql, "FROM data", fmt.Sprintf("FROM file('%s'%s)", path, formatArg)), limitClause)
	}

	return et.client.Query(fullSQL)
}

// QueryHDFS queries data from HDFS.
func (et *ExternalTables) QueryHDFS(path, sql string, opts *QueryS3Options) (*Result, error) {
	if sql == "" {
		sql = "SELECT *"
	}

	formatArg := ""
	limitClause := ""

	if opts != nil {
		if opts.Format != "" {
			formatArg = fmt.Sprintf(", '%s'", opts.Format)
		}
		if opts.Limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", opts.Limit)
		}
	}

	var fullSQL string
	if strings.TrimSpace(strings.ToUpper(sql)) == "SELECT *" {
		fullSQL = fmt.Sprintf("SELECT * FROM hdfs('%s'%s)%s", path, formatArg, limitClause)
	} else {
		fullSQL = fmt.Sprintf(`
			SELECT * FROM (
				%s
			) AS subq
			%s
		`, strings.ReplaceAll(sql, "FROM data", fmt.Sprintf("FROM hdfs('%s'%s)", path, formatArg)), limitClause)
	}

	return et.client.Query(fullSQL)
}

// ExternalTableColumn defines a column for an external table.
type ExternalTableColumn struct {
	Name string
	Type string
}

// CreateExternalTableOptions holds options for creating an external table.
type CreateExternalTableOptions struct {
	SourceType       string
	Location         string
	Format           string
	Columns          []ExternalTableColumn
	PartitionColumns []string
}

// CreateExternalTable creates a named external table.
func (et *ExternalTables) CreateExternalTable(name string, opts CreateExternalTableOptions) error {
	colDefs := make([]string, 0, len(opts.Columns))
	for _, c := range opts.Columns {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", c.Name, c.Type))
	}

	partitionClause := ""
	if len(opts.PartitionColumns) > 0 {
		partitionClause = fmt.Sprintf(" PARTITION BY (%s)", strings.Join(opts.PartitionColumns, ", "))
	}

	// Normalize location
	location := opts.Location
	prefix := opts.SourceType + "://"
	if strings.HasPrefix(location, prefix) {
		location = strings.TrimPrefix(location, prefix)
	}

	sql := fmt.Sprintf(`
		CREATE EXTERNAL TABLE %s (%s)
		STORED AS %s
		LOCATION '%s://%s'
		%s
	`, name, strings.Join(colDefs, ", "), strings.ToUpper(opts.Format), opts.SourceType, location, partitionClause)

	return et.client.Exec(sql)
}

// DropExternalTable drops an external table.
func (et *ExternalTables) DropExternalTable(name string) error {
	return et.client.Exec(fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s", name))
}

// VectorSearch provides vector similarity search helpers.
type VectorSearch struct {
	client *Client
}

// Vector returns a VectorSearch instance.
func (c *Client) Vector() *VectorSearch {
	return &VectorSearch{client: c}
}

// VectorIndexOptions holds options for creating a vector index.
type VectorIndexOptions struct {
	IndexName      string
	IndexType      string
	DistanceMetric string
	M              int
	EfConstruction int
}

// CreateIndex creates a vector index.
func (vs *VectorSearch) CreateIndex(table, column string, opts *VectorIndexOptions) error {
	indexType := "hnsw"
	distanceMetric := "cosine"
	m := 16
	efConstruction := 200
	indexName := fmt.Sprintf("idx_%s_%s_vec", strings.ReplaceAll(table, ".", "_"), column)

	if opts != nil {
		if opts.IndexType != "" {
			indexType = opts.IndexType
		}
		if opts.DistanceMetric != "" {
			distanceMetric = opts.DistanceMetric
		}
		if opts.M > 0 {
			m = opts.M
		}
		if opts.EfConstruction > 0 {
			efConstruction = opts.EfConstruction
		}
		if opts.IndexName != "" {
			indexName = opts.IndexName
		}
	}

	sql := fmt.Sprintf(`
		CREATE INDEX %s ON %s
		USING %s (%s %s)
		WITH (m = %d, ef_construction = %d)
	`, indexName, table, indexType, column, distanceMetric, m, efConstruction)

	return vs.client.Exec(sql)
}

// VectorSearchOptions holds options for vector search.
type VectorSearchOptions struct {
	K             int
	Where         string
	EfSearch      int
	ReturnColumns []string
}

// VectorSearchResult represents a vector search result.
type VectorSearchResult struct {
	Distance float64
	Data     map[string]interface{}
}

// Search finds k nearest neighbors.
func (vs *VectorSearch) Search(table, column string, queryVector []float64, opts *VectorSearchOptions) (*Result, error) {
	k := 10
	whereClause := ""
	cols := "*"

	if opts != nil {
		if opts.K > 0 {
			k = opts.K
		}
		if opts.Where != "" {
			whereClause = fmt.Sprintf("WHERE %s", opts.Where)
		}
		if len(opts.ReturnColumns) > 0 {
			cols = strings.Join(opts.ReturnColumns, ", ")
		}
	}

	// Format vector
	vecParts := make([]string, len(queryVector))
	for i, v := range queryVector {
		vecParts[i] = fmt.Sprintf("%f", v)
	}
	vecStr := "[" + strings.Join(vecParts, ",") + "]"

	sql := fmt.Sprintf(`
		SELECT %s, %s <=> '%s'::vector AS distance
		FROM %s
		%s
		ORDER BY %s <=> '%s'::vector
		LIMIT %d
	`, cols, column, vecStr, table, whereClause, column, vecStr, k)

	return vs.client.Query(sql)
}

// HybridSearchOptions holds options for hybrid search.
type HybridSearchOptions struct {
	K            int
	VectorWeight float64
	TextWeight   float64
}

// HybridSearch performs hybrid vector + full-text search.
func (vs *VectorSearch) HybridSearch(table, vectorColumn, textColumn string, queryVector []float64, queryText string, opts *HybridSearchOptions) (*Result, error) {
	k := 10
	vectorWeight := 0.7
	textWeight := 0.3

	if opts != nil {
		if opts.K > 0 {
			k = opts.K
		}
		if opts.VectorWeight > 0 {
			vectorWeight = opts.VectorWeight
		}
		if opts.TextWeight > 0 {
			textWeight = opts.TextWeight
		}
	}

	// Format vector
	vecParts := make([]string, len(queryVector))
	for i, v := range queryVector {
		vecParts[i] = fmt.Sprintf("%f", v)
	}
	vecStr := "[" + strings.Join(vecParts, ",") + "]"

	escapedText := strings.ReplaceAll(queryText, "'", "''")

	sql := fmt.Sprintf(`
		SELECT *,
			(%f * (1.0 - (%s <=> '%s'::vector))) +
			(%f * ts_rank(%s_tsvector, plainto_tsquery('%s')))
			AS hybrid_score
		FROM %s
		WHERE %s_tsvector @@ plainto_tsquery('%s')
		   OR %s <=> '%s'::vector < 0.5
		ORDER BY hybrid_score DESC
		LIMIT %d
	`, vectorWeight, vectorColumn, vecStr, textWeight, textColumn, escapedText, table, textColumn, escapedText, vectorColumn, vecStr, k)

	return vs.client.Query(sql)
}

// DropIndex drops a vector index.
func (vs *VectorSearch) DropIndex(indexName string) error {
	return vs.client.Exec(fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName))
}
