package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type reader struct{ errs []error }

func (r *reader) err() error { return errors.Join(r.errs...) }

func (r *reader) str(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func (r *reader) required(key string) string {
	value := r.str(key, "")
	if value == "" {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}
	return value
}

func (r *reader) integer(key string, fallback, low, high int64) int64 {
	value := fallback
	if raw, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("invalid %s", key))
		} else {
			value = parsed
		}
	}
	if value < low || value > high {
		r.errs = append(r.errs, fmt.Errorf("%s must be in [%d,%d]", key, low, high))
	}
	return value
}

func (r *reader) duration(key string, fallback time.Duration) time.Duration {
	value := fallback
	if raw, ok := os.LookupEnv(key); ok {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("invalid %s", key))
		} else {
			value = parsed
		}
	}
	if value <= 0 || value > 24*time.Hour {
		r.errs = append(r.errs, fmt.Errorf("%s must be positive and at most 24h", key))
	}
	return value
}

func (r *reader) boolean(key string, fallback bool) bool {
	if raw, ok := os.LookupEnv(key); ok {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("invalid %s", key))
		}
		return value
	}
	return fallback
}

// Broker and producer/subscriber clients use the same default and override.
func (r *reader) messageBytes() int { return int(r.integer("MAX_MESSAGE_BYTES", 64<<10, 1024, 16<<20)) }
