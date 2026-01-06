package boyodb

import "errors"

// Common errors returned by the client.
var (
	// ErrNotConnected is returned when trying to use a closed or uninitialized client.
	ErrNotConnected = errors.New("boyodb: not connected")

	// ErrAuthRequired is returned when authentication is required but not provided.
	ErrAuthRequired = errors.New("boyodb: authentication required")

	// ErrQueryFailed is returned when a query fails on the server.
	ErrQueryFailed = errors.New("boyodb: query failed")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("boyodb: operation timed out")

	// ErrInvalidResponse is returned when the server returns an invalid response.
	ErrInvalidResponse = errors.New("boyodb: invalid response from server")

	// ErrResultClosed is returned when trying to use a closed result.
	ErrResultClosed = errors.New("boyodb: result is closed")
)

// Error represents a boyodb error with additional context.
type Error struct {
	Op      string // Operation that failed
	Message string // Error message from server
	Err     error  // Underlying error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return "boyodb: " + e.Op + ": " + e.Message + ": " + e.Err.Error()
	}
	return "boyodb: " + e.Op + ": " + e.Message
}

func (e *Error) Unwrap() error {
	return e.Err
}
