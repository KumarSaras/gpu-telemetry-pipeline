package platform

import (
	"context"
	"errors"
	"time"
)

// ManagedRun propagates management-listener failures and bounds worker shutdown.
func ManagedRun(cancel context.CancelFunc, work func() error, httpErr <-chan error, grace time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- work() }()
	select {
	case err := <-done:
		return err
	case err := <-httpErr:
		if err == nil {
			err = errors.New("management listener stopped")
		}
		cancel()
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
			return err
		case <-timer.C:
			return errors.Join(err, errors.New("worker shutdown deadline exceeded"))
		}
	}
}
