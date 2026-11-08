package telemetry

import "context"

// Cursor streams a snapshot's rows. Callers must Close it, including on disconnect.
type Cursor interface {
	Next() bool
	Record() (Record, error)
	Err() error
	Close()
}
type Reader interface {
	ListGPUs(context.Context) ([]string, error)
	Query(context.Context, string, Bounds) (Cursor, error)
}
