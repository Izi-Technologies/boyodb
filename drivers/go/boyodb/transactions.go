package boyodb

import (
	"fmt"
)

// IsolationLevel represents transaction isolation levels.
type IsolationLevel string

const (
	ReadUncommitted IsolationLevel = "READ UNCOMMITTED"
	ReadCommitted   IsolationLevel = "READ COMMITTED"
	RepeatableRead  IsolationLevel = "REPEATABLE READ"
	Serializable    IsolationLevel = "SERIALIZABLE"
	Snapshot        IsolationLevel = "SNAPSHOT"
)

// TxOptions holds transaction configuration options.
type TxOptions struct {
	IsolationLevel IsolationLevel
	ReadOnly       bool
}

// Transaction represents an ACID transaction.
type Transaction struct {
	client     *Client
	options    TxOptions
	active     bool
	savepoints []string
}

// NewTransaction creates a new transaction wrapper.
func NewTransaction(client *Client, options *TxOptions) *Transaction {
	opts := TxOptions{}
	if options != nil {
		opts = *options
	}
	return &Transaction{
		client:     client,
		options:    opts,
		active:     false,
		savepoints: make([]string, 0),
	}
}

// Begin starts the transaction.
func (tx *Transaction) Begin() error {
	sql := "BEGIN"
	if tx.options.IsolationLevel != "" {
		sql += fmt.Sprintf(" ISOLATION LEVEL %s", tx.options.IsolationLevel)
	}
	if tx.options.ReadOnly {
		sql += " READ ONLY"
	}

	if err := tx.client.Exec(sql); err != nil {
		return err
	}
	tx.active = true
	return nil
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	if !tx.active {
		return nil
	}
	if err := tx.client.Exec("COMMIT"); err != nil {
		return err
	}
	tx.active = false
	tx.savepoints = nil
	return nil
}

// Rollback rolls back the transaction.
func (tx *Transaction) Rollback() error {
	if !tx.active {
		return nil
	}
	if err := tx.client.Exec("ROLLBACK"); err != nil {
		return err
	}
	tx.active = false
	tx.savepoints = nil
	return nil
}

// Savepoint creates a savepoint.
func (tx *Transaction) Savepoint(name string) error {
	if !tx.active {
		return fmt.Errorf("no active transaction")
	}
	if err := tx.client.Exec(fmt.Sprintf("SAVEPOINT %s", name)); err != nil {
		return err
	}
	tx.savepoints = append(tx.savepoints, name)
	return nil
}

// RollbackTo rolls back to a savepoint.
func (tx *Transaction) RollbackTo(name string) error {
	if !tx.active {
		return fmt.Errorf("no active transaction")
	}
	if err := tx.client.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)); err != nil {
		return err
	}
	// Trim savepoints list
	for i, sp := range tx.savepoints {
		if sp == name {
			tx.savepoints = tx.savepoints[:i+1]
			break
		}
	}
	return nil
}

// Release releases a savepoint.
func (tx *Transaction) Release(name string) error {
	if !tx.active {
		return fmt.Errorf("no active transaction")
	}
	if err := tx.client.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", name)); err != nil {
		return err
	}
	// Remove from savepoints list
	for i, sp := range tx.savepoints {
		if sp == name {
			tx.savepoints = append(tx.savepoints[:i], tx.savepoints[i+1:]...)
			break
		}
	}
	return nil
}

// Execute runs SQL within the transaction.
func (tx *Transaction) Execute(sql string) (*Result, error) {
	if !tx.active {
		return nil, fmt.Errorf("no active transaction")
	}
	return tx.client.Query(sql)
}

// Exec runs SQL without returning results.
func (tx *Transaction) Exec(sql string) error {
	if !tx.active {
		return fmt.Errorf("no active transaction")
	}
	return tx.client.Exec(sql)
}

// IsActive returns true if the transaction is active.
func (tx *Transaction) IsActive() bool {
	return tx.active
}

// WithTransaction executes a function within an auto-managed transaction.
// The transaction is committed on success and rolled back on error.
func (c *Client) WithTransaction(fn func(*Transaction) error, opts *TxOptions) error {
	tx := NewTransaction(c, opts)
	if err := tx.Begin(); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Transaction creates a new transaction wrapper.
func (c *Client) Transaction(opts *TxOptions) *Transaction {
	return NewTransaction(c, opts)
}
