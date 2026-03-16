// Package boyodb provides async insert buffer support for high-throughput data ingestion.
//
// The AsyncInsertBuffer batches inserts and flushes them periodically for optimal performance.
//
// Example usage:
//
//	buffer := boyodb.NewAsyncInsertBuffer(client, "mydb.events", &boyodb.AsyncInsertConfig{
//	    MaxRows:     10000,
//	    MaxWaitTime: 200 * time.Millisecond,
//	    Deduplicate: true,
//	    DedupColumns: []string{"event_id"},
//	})
//	buffer.Start()
//
//	for _, event := range events {
//	    buffer.Insert(event)
//	}
//
//	buffer.Stop()
//	fmt.Printf("Inserted %d rows in %d flushes\n", buffer.Stats().TotalRows, buffer.Stats().TotalFlushes)
package boyodb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncInsertConfig holds configuration for async insert buffer.
type AsyncInsertConfig struct {
	// MaxRows is the maximum number of rows to buffer before flushing.
	// Default: 10000
	MaxRows int
	// MaxBytes is the maximum buffer size in bytes before flushing.
	// Default: 10MB (10485760)
	MaxBytes int
	// MaxWaitTime is the maximum time to wait before flushing.
	// Default: 200ms
	MaxWaitTime time.Duration
	// Deduplicate enables deduplication within the buffer.
	Deduplicate bool
	// DedupColumns specifies which columns to use for deduplication.
	// If empty and Deduplicate is true, all columns are used.
	DedupColumns []string
	// MaxRetries is the number of retries on flush failure.
	// Default: 3
	MaxRetries int
	// RetryDelay is the delay between retries.
	// Default: 100ms
	RetryDelay time.Duration
	// OnFlush is called after each successful flush with row count and any error.
	OnFlush func(rowCount int, err error)
	// OnError is called when an error occurs.
	OnError func(error)
}

// AsyncInsertStats holds statistics for async insert operations.
type AsyncInsertStats struct {
	TotalRows         int64
	TotalFlushes      int64
	TotalBytes        int64
	DuplicatesRemoved int64
	TotalErrors       int64
}

// AsyncInsertBuffer buffers inserts and flushes them in batches.
type AsyncInsertBuffer struct {
	client *Client
	table  string
	config AsyncInsertConfig

	buffer      []map[string]interface{}
	bufferBytes int
	bufferMu    sync.Mutex

	dedupSet map[string]struct{}

	stats AsyncInsertStats

	running   bool
	stopChan  chan struct{}
	doneChan  chan struct{}
	flushChan chan struct{}
}

// NewAsyncInsertBuffer creates a new async insert buffer.
//
// If config is nil, default configuration is used.
func NewAsyncInsertBuffer(client *Client, table string, config *AsyncInsertConfig) *AsyncInsertBuffer {
	cfg := AsyncInsertConfig{
		MaxRows:     10000,
		MaxBytes:    10 * 1024 * 1024, // 10MB
		MaxWaitTime: 200 * time.Millisecond,
		MaxRetries:  3,
		RetryDelay:  100 * time.Millisecond,
	}
	if config != nil {
		if config.MaxRows > 0 {
			cfg.MaxRows = config.MaxRows
		}
		if config.MaxBytes > 0 {
			cfg.MaxBytes = config.MaxBytes
		}
		if config.MaxWaitTime > 0 {
			cfg.MaxWaitTime = config.MaxWaitTime
		}
		if config.MaxRetries > 0 {
			cfg.MaxRetries = config.MaxRetries
		}
		if config.RetryDelay > 0 {
			cfg.RetryDelay = config.RetryDelay
		}
		cfg.Deduplicate = config.Deduplicate
		cfg.DedupColumns = config.DedupColumns
		cfg.OnFlush = config.OnFlush
		cfg.OnError = config.OnError
	}

	b := &AsyncInsertBuffer{
		client: client,
		table:  table,
		config: cfg,
		buffer: make([]map[string]interface{}, 0, cfg.MaxRows),
	}

	if cfg.Deduplicate {
		b.dedupSet = make(map[string]struct{})
	}

	return b
}

// Start begins the background flush goroutine.
func (b *AsyncInsertBuffer) Start() {
	b.bufferMu.Lock()
	if b.running {
		b.bufferMu.Unlock()
		return
	}
	b.running = true
	b.stopChan = make(chan struct{})
	b.doneChan = make(chan struct{})
	b.flushChan = make(chan struct{}, 1)
	b.bufferMu.Unlock()

	go b.flushLoop()
}

// Stop stops the buffer and flushes any remaining rows.
func (b *AsyncInsertBuffer) Stop() error {
	b.bufferMu.Lock()
	if !b.running {
		b.bufferMu.Unlock()
		return nil
	}
	b.running = false
	close(b.stopChan)
	b.bufferMu.Unlock()

	// Wait for flush loop to finish
	<-b.doneChan

	// Final flush
	return b.Flush()
}

// Insert adds a row to the buffer.
//
// Returns true if the row was added, false if it was deduplicated.
func (b *AsyncInsertBuffer) Insert(row map[string]interface{}) (bool, error) {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	if !b.running {
		return false, fmt.Errorf("buffer not started")
	}

	// Check deduplication
	if b.dedupSet != nil {
		key := b.makeDedupKey(row)
		if _, exists := b.dedupSet[key]; exists {
			atomic.AddInt64(&b.stats.DuplicatesRemoved, 1)
			return false, nil
		}
		b.dedupSet[key] = struct{}{}
	}

	// Estimate row size
	rowSize := b.estimateRowSize(row)

	b.buffer = append(b.buffer, row)
	b.bufferBytes += rowSize

	// Check if immediate flush is needed
	if len(b.buffer) >= b.config.MaxRows || b.bufferBytes >= b.config.MaxBytes {
		b.signalFlush()
	}

	return true, nil
}

// InsertMany adds multiple rows to the buffer.
//
// Returns the number of rows added (excluding duplicates).
func (b *AsyncInsertBuffer) InsertMany(rows []map[string]interface{}) (int, error) {
	count := 0
	for _, row := range rows {
		added, err := b.Insert(row)
		if err != nil {
			return count, err
		}
		if added {
			count++
		}
	}
	return count, nil
}

// Flush immediately flushes the buffer.
func (b *AsyncInsertBuffer) Flush() error {
	b.bufferMu.Lock()
	if len(b.buffer) == 0 {
		b.bufferMu.Unlock()
		return nil
	}

	// Swap buffer
	rows := b.buffer
	bytes := b.bufferBytes
	b.buffer = make([]map[string]interface{}, 0, b.config.MaxRows)
	b.bufferBytes = 0
	if b.dedupSet != nil {
		b.dedupSet = make(map[string]struct{})
	}
	b.bufferMu.Unlock()

	// Build and execute INSERT
	sql := b.buildInsertSQL(rows)
	var lastErr error

	for attempt := 0; attempt < b.config.MaxRetries; attempt++ {
		err := b.client.Exec(sql)
		if err == nil {
			atomic.AddInt64(&b.stats.TotalRows, int64(len(rows)))
			atomic.AddInt64(&b.stats.TotalBytes, int64(bytes))
			atomic.AddInt64(&b.stats.TotalFlushes, 1)

			if b.config.OnFlush != nil {
				b.config.OnFlush(len(rows), nil)
			}
			return nil
		}

		lastErr = err
		if attempt < b.config.MaxRetries-1 {
			time.Sleep(b.config.RetryDelay * time.Duration(attempt+1))
		}
	}

	atomic.AddInt64(&b.stats.TotalErrors, 1)
	if b.config.OnFlush != nil {
		b.config.OnFlush(0, lastErr)
	}
	if b.config.OnError != nil {
		b.config.OnError(lastErr)
	}
	return lastErr
}

// Stats returns current buffer statistics.
func (b *AsyncInsertBuffer) Stats() AsyncInsertStats {
	return AsyncInsertStats{
		TotalRows:         atomic.LoadInt64(&b.stats.TotalRows),
		TotalFlushes:      atomic.LoadInt64(&b.stats.TotalFlushes),
		TotalBytes:        atomic.LoadInt64(&b.stats.TotalBytes),
		DuplicatesRemoved: atomic.LoadInt64(&b.stats.DuplicatesRemoved),
		TotalErrors:       atomic.LoadInt64(&b.stats.TotalErrors),
	}
}

// PendingRows returns the number of rows pending flush.
func (b *AsyncInsertBuffer) PendingRows() int {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	return len(b.buffer)
}

// IsRunning returns whether the buffer is currently running.
func (b *AsyncInsertBuffer) IsRunning() bool {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	return b.running
}

func (b *AsyncInsertBuffer) flushLoop() {
	ticker := time.NewTicker(b.config.MaxWaitTime)
	defer ticker.Stop()
	defer close(b.doneChan)

	for {
		select {
		case <-b.stopChan:
			return
		case <-b.flushChan:
			if err := b.Flush(); err != nil {
				// Error already handled in Flush
			}
		case <-ticker.C:
			if b.PendingRows() > 0 {
				if err := b.Flush(); err != nil {
					// Error already handled in Flush
				}
			}
		}
	}
}

func (b *AsyncInsertBuffer) signalFlush() {
	select {
	case b.flushChan <- struct{}{}:
	default:
		// Flush already signaled
	}
}

func (b *AsyncInsertBuffer) makeDedupKey(row map[string]interface{}) string {
	if len(b.config.DedupColumns) > 0 {
		parts := make([]string, len(b.config.DedupColumns))
		for i, col := range b.config.DedupColumns {
			parts[i] = fmt.Sprintf("%v", row[col])
		}
		return strings.Join(parts, "|")
	}

	// Use all columns sorted by key
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k, row[k])
	}
	return strings.Join(parts, "|")
}

func (b *AsyncInsertBuffer) estimateRowSize(row map[string]interface{}) int {
	// Rough estimate based on JSON encoding
	data, err := json.Marshal(row)
	if err != nil {
		return 100 // Default estimate
	}
	return len(data)
}

func (b *AsyncInsertBuffer) buildInsertSQL(rows []map[string]interface{}) string {
	if len(rows) == 0 {
		return ""
	}

	// Get column names from first row
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build column list
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(b.table)
	sb.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(") VALUES ")

	// Build values
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, col := range columns {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(b.formatValue(row[col]))
		}
		sb.WriteString(")")
	}

	return sb.String()
}

func (b *AsyncInsertBuffer) formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		// Escape single quotes
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%v", val)
	case time.Time:
		return "'" + val.Format(time.RFC3339) + "'"
	case []interface{}, map[string]interface{}:
		// JSON encode arrays and objects
		data, err := json.Marshal(val)
		if err != nil {
			return "NULL"
		}
		escaped := strings.ReplaceAll(string(data), "'", "''")
		return "'" + escaped + "'"
	default:
		// Try to convert to string
		return fmt.Sprintf("'%v'", v)
	}
}
