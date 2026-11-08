package platform

import (
	"context"
	"math/rand/v2"
	"time"
)

// Pause applies capped exponential backoff with jitter and cancellation.
func Pause(ctx context.Context, attempt int) error {
	d := min(100*time.Millisecond*time.Duration(1<<min(attempt, 6)), 5*time.Second)
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
