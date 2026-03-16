// Package boyodb provides CDC (Change Data Capture) subscription support.
//
// CDC allows subscribing to real-time changes on database tables.
//
// Example usage:
//
//	subscriber := boyodb.NewCDCSubscriber(client, "mydb.users", func(event *boyodb.ChangeEvent) {
//	    fmt.Printf("Change: %s on %s\n", event.ChangeType, event.Table)
//	}, nil)
//	subscriber.Start()
//	defer subscriber.Stop()
package boyodb

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ChangeType represents the type of change in a CDC event.
type ChangeType string

const (
	// ChangeTypeInsert indicates a new row was inserted.
	ChangeTypeInsert ChangeType = "INSERT"
	// ChangeTypeUpdate indicates an existing row was updated.
	ChangeTypeUpdate ChangeType = "UPDATE"
	// ChangeTypeDelete indicates a row was deleted.
	ChangeTypeDelete ChangeType = "DELETE"
)

// ChangeEvent represents a single CDC change event.
type ChangeEvent struct {
	// Table is the name of the table that changed.
	Table string `json:"table"`
	// ChangeType is the type of change (INSERT, UPDATE, DELETE).
	ChangeType ChangeType `json:"change_type"`
	// Before contains the row data before the change (for UPDATE/DELETE).
	Before map[string]interface{} `json:"before,omitempty"`
	// After contains the row data after the change (for INSERT/UPDATE).
	After map[string]interface{} `json:"after,omitempty"`
	// LSN is the log sequence number for this change.
	LSN uint64 `json:"lsn"`
	// Timestamp is when the change occurred.
	Timestamp time.Time `json:"timestamp"`
	// TxID is the transaction ID, if available.
	TxID *uint64 `json:"transaction_id,omitempty"`
}

// CDCConfig holds configuration for a CDC subscriber.
type CDCConfig struct {
	// IncludeBefore includes the before image in UPDATE/DELETE events.
	IncludeBefore bool
	// ChangeTypes filters which change types to receive.
	// If nil or empty, all change types are received.
	ChangeTypes []ChangeType
	// StartLSN specifies the LSN to start reading from.
	// If 0, starts from the current position.
	StartLSN uint64
	// PollInterval is how often to poll for changes.
	// Default: 100ms
	PollInterval time.Duration
	// OnError is called when an error occurs during polling.
	OnError func(error)
}

// CDCSubscriber subscribes to CDC events for a table.
type CDCSubscriber struct {
	client   *Client
	table    string
	onChange func(*ChangeEvent)
	config   CDCConfig

	mu         sync.Mutex
	running    bool
	currentLSN uint64
	stopChan   chan struct{}
	doneChan   chan struct{}
}

// NewCDCSubscriber creates a new CDC subscriber.
//
// The onChange callback is called for each change event. It should not block
// for long periods as it runs in the polling goroutine.
//
// If config is nil, default configuration is used.
func NewCDCSubscriber(client *Client, table string, onChange func(*ChangeEvent), config *CDCConfig) *CDCSubscriber {
	cfg := CDCConfig{
		PollInterval: 100 * time.Millisecond,
	}
	if config != nil {
		cfg = *config
		if cfg.PollInterval == 0 {
			cfg.PollInterval = 100 * time.Millisecond
		}
	}

	return &CDCSubscriber{
		client:     client,
		table:      table,
		onChange:   onChange,
		config:     cfg,
		currentLSN: cfg.StartLSN,
	}
}

// Start begins receiving CDC events.
//
// This method starts a background goroutine that polls for changes.
// Call Stop to stop receiving events.
func (s *CDCSubscriber) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.stopChan = make(chan struct{})
	s.doneChan = make(chan struct{})
	s.mu.Unlock()

	// Send START CDC command
	if err := s.startCDC(); err != nil {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		return fmt.Errorf("failed to start CDC: %w", err)
	}

	// Start polling goroutine
	go s.pollLoop()

	return nil
}

// Stop stops receiving CDC events.
//
// This method signals the polling goroutine to stop, waits for it to finish,
// and sends the STOP CDC command to the server.
func (s *CDCSubscriber) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopChan)
	s.mu.Unlock()

	// Wait for polling goroutine to finish
	<-s.doneChan

	// Send STOP CDC command
	return s.stopCDC()
}

// CurrentLSN returns the current log sequence number.
func (s *CDCSubscriber) CurrentLSN() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentLSN
}

// IsRunning returns whether the subscriber is currently running.
func (s *CDCSubscriber) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *CDCSubscriber) startCDC() error {
	opts := []string{}
	if s.config.IncludeBefore {
		opts = append(opts, "include_before = true")
	}
	if s.config.StartLSN > 0 {
		opts = append(opts, fmt.Sprintf("start_lsn = %d", s.config.StartLSN))
	}

	sql := fmt.Sprintf("START CDC ON %s", s.table)
	if len(opts) > 0 {
		sql += " WITH ("
		for i, opt := range opts {
			if i > 0 {
				sql += ", "
			}
			sql += opt
		}
		sql += ")"
	}

	return s.client.Exec(sql)
}

func (s *CDCSubscriber) stopCDC() error {
	sql := fmt.Sprintf("STOP CDC ON %s", s.table)
	return s.client.Exec(sql)
}

func (s *CDCSubscriber) pollLoop() {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	defer close(s.doneChan)

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.pollOnce()
		}
	}
}

func (s *CDCSubscriber) pollOnce() {
	s.mu.Lock()
	lsn := s.currentLSN
	s.mu.Unlock()

	sql := fmt.Sprintf("POLL CDC %s FROM %d", s.table, lsn)
	result, err := s.client.Query(sql)
	if err != nil {
		if s.config.OnError != nil {
			s.config.OnError(err)
		}
		return
	}
	defer result.Close()

	for result.Next() {
		event, err := s.parseChangeEvent(result)
		if err != nil {
			if s.config.OnError != nil {
				s.config.OnError(err)
			}
			continue
		}

		// Filter by change type
		if !s.shouldProcess(event.ChangeType) {
			continue
		}

		// Call callback
		s.onChange(event)

		// Update LSN
		s.mu.Lock()
		if event.LSN >= s.currentLSN {
			s.currentLSN = event.LSN + 1
		}
		s.mu.Unlock()
	}
}

func (s *CDCSubscriber) shouldProcess(changeType ChangeType) bool {
	if len(s.config.ChangeTypes) == 0 {
		return true
	}
	for _, ct := range s.config.ChangeTypes {
		if ct == changeType {
			return true
		}
	}
	return false
}

func (s *CDCSubscriber) parseChangeEvent(result *Result) (*ChangeEvent, error) {
	row, err := result.ScanMap()
	if err != nil {
		return nil, err
	}

	event := &ChangeEvent{
		Table: s.table,
	}

	// Parse change type
	if ct, ok := row["change_type"].(string); ok {
		event.ChangeType = ChangeType(ct)
	} else if ct, ok := row["changeType"].(string); ok {
		event.ChangeType = ChangeType(ct)
	}

	// Parse LSN
	if lsn, ok := row["lsn"].(float64); ok {
		event.LSN = uint64(lsn)
	} else if lsn, ok := row["lsn"].(int64); ok {
		event.LSN = uint64(lsn)
	}

	// Parse timestamp
	if ts, ok := row["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			event.Timestamp = t
		}
	} else if ts, ok := row["timestamp"].(float64); ok {
		event.Timestamp = time.UnixMilli(int64(ts))
	}

	// Parse transaction ID
	if txid, ok := row["transaction_id"].(float64); ok {
		tid := uint64(txid)
		event.TxID = &tid
	} else if txid, ok := row["txid"].(float64); ok {
		tid := uint64(txid)
		event.TxID = &tid
	}

	// Parse before/after
	if before, ok := row["before"]; ok {
		event.Before = s.parseRowData(before)
	}
	if after, ok := row["after"]; ok {
		event.After = s.parseRowData(after)
	}

	return event, nil
}

func (s *CDCSubscriber) parseRowData(data interface{}) map[string]interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		return v
	case string:
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(v), &result); err == nil {
			return result
		}
	}
	return nil
}
