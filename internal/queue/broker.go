// Package queue implements the custom, bounded, in-memory work queue.
package queue

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"time"

	"gpu-telemetry-pipeline/internal/telemetry"
)

var (
	ErrFull     = errors.New("queue capacity reached")
	ErrDraining = errors.New("queue is draining")
	ErrClosed   = errors.New("queue closed")
	ErrStale    = errors.New("stale delivery")
	ErrSlots    = errors.New("invalid subscriber slots or subscriber limit reached")
)

type Config struct {
	MaxMessages              int
	MaxBytes                 int64
	MaxMessageBytes          int
	MaxSubscribers, MaxSlots int
	AckTimeout               time.Duration
}

func (c Config) Validate() error {
	if c.MaxMessages < 1 || c.MaxBytes < 1 || c.MaxMessageBytes < 1 || int64(c.MaxMessageBytes) > c.MaxBytes || c.MaxSubscribers < 1 || c.MaxSlots < 1 || c.MaxSlots > 4096 || c.AckTimeout < time.Millisecond || c.AckTimeout > time.Duration(1<<63-1)/time.Duration(c.MaxSlots) {
		return errors.New("invalid queue bounds or acknowledgement timeout")
	}
	return nil
}

type Delivery struct {
	Envelope telemetry.Envelope
	Tag      int64
}
type Subscription struct {
	ID         int64
	Slots      int
	Deliveries <-chan Delivery
	Context    context.Context
}
type Stats struct {
	Messages, Ready, InFlight, Subscribers, Slots                int
	Bytes                                                        int64
	Accepted, Duplicates, Completed, Rejected, Redelivered, Full uint64
	Draining                                                     bool
}
type entry struct {
	envelope   telemetry.Envelope
	size       int64
	owner, tag int64
	deadline   time.Time
}
type subscriber struct {
	Subscription
	out         chan Delivery
	cancel      context.CancelCauseFunc
	assignments map[telemetry.Key]*entry
}
type state struct {
	cfg         Config
	ready       *list.List
	active      map[telemetry.Key]*entry
	subscribers []*subscriber
	cursor      int
	tags, ids   telemetry.Counter
	stats       Stats
	fatal       chan error
	failed      bool
}
type command struct {
	ctx   context.Context
	apply func(*state)
}
type Broker struct {
	commands chan command
	done     chan struct{}
	cancel   context.CancelFunc
	failure  chan error
	cfg      Config
}

func New(cfg Config) (*Broker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{make(chan command, 256), make(chan struct{}), cancel, make(chan error, 1), cfg}
	s := &state{cfg: cfg, ready: list.New(), active: make(map[telemetry.Key]*entry), fatal: b.failure}
	go b.run(ctx, s)
	return b, nil
}
func (b *Broker) run(ctx context.Context, s *state) {
	defer close(b.done)
	period := min(b.cfg.AckTimeout/4, time.Second)
	tick := time.NewTicker(period)
	defer tick.Stop()
	defer func() {
		for _, sub := range s.subscribers {
			sub.cancel(ErrClosed)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-b.commands:
			if cmd.ctx.Err() == nil {
				cmd.apply(s)
			}
		case now := <-tick.C:
			s.expire(now)
		}
		s.dispatch(time.Now())
	}
}
func request[T any](ctx context.Context, b *Broker, fn func(*state) T) (T, error) {
	var zero T
	result := make(chan T, 1)
	select {
	case b.commands <- command{ctx, func(s *state) { result <- fn(s) }}:
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-b.done:
		return zero, ErrClosed
	}
	select {
	case v := <-result:
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-b.done:
		return zero, ErrClosed
	}
}
func (b *Broker) Enqueue(ctx context.Context, e telemetry.Envelope) (bool, error) {
	if err := e.Key.Validate(); err != nil {
		return false, err
	}
	size := int64(e.Size())
	if size > int64(b.cfg.MaxMessageBytes) {
		return false, fmt.Errorf("message exceeds %d bytes", b.cfg.MaxMessageBytes)
	}
	type result struct {
		duplicate bool
		err       error
	}
	r, err := request(ctx, b, func(s *state) result {
		if _, ok := s.active[e.Key]; ok {
			s.stats.Duplicates++
			return result{true, nil}
		}
		if s.stats.Draining {
			return result{false, ErrDraining}
		}
		if len(s.active) >= s.cfg.MaxMessages || size > s.cfg.MaxBytes-s.stats.Bytes {
			s.stats.Full++
			return result{false, ErrFull}
		}
		v := &entry{envelope: e, size: size}
		s.active[e.Key] = v
		s.ready.PushBack(v)
		s.stats.Bytes += size
		s.stats.Accepted++
		return result{}
	})
	if err != nil {
		return false, err
	}
	return r.duplicate, r.err
}
func (b *Broker) Subscribe(ctx context.Context, slots int) (Subscription, error) {
	type result struct {
		sub Subscription
		err error
	}
	r, err := request(ctx, b, func(s *state) result {
		if slots < 1 || slots > s.cfg.MaxSlots || len(s.subscribers) >= s.cfg.MaxSubscribers {
			return result{err: ErrSlots}
		}
		id, err := s.ids.Next()
		if err != nil {
			s.fail(err)
			return result{err: err}
		}
		subctx, cancel := context.WithCancelCause(ctx)
		out := make(chan Delivery, slots)
		sub := &subscriber{Subscription: Subscription{ID: id, Slots: slots, Deliveries: out, Context: subctx}, out: out, cancel: cancel, assignments: make(map[telemetry.Key]*entry)}
		s.subscribers = append(s.subscribers, sub)
		return result{sub: sub.Subscription}
	})
	if err != nil {
		return Subscription{}, err
	}
	return r.sub, r.err
}
func (b *Broker) Disconnect(ctx context.Context, id int64) error {
	_, err := request(ctx, b, func(s *state) bool { s.remove(id, errors.New("subscriber disconnected")); return true })
	return err
}

type Disposition int

const (
	Ack Disposition = iota + 1
	Retry
	Reject
)

func (b *Broker) Complete(ctx context.Context, id int64, key telemetry.Key, tag int64, action Disposition) error {
	if id <= 0 || tag <= 0 {
		return ErrStale
	}
	r, err := request(ctx, b, func(s *state) error {
		e := s.active[key]
		if e == nil || e.owner != id || e.tag != tag {
			return ErrStale
		}
		if action == Retry {
			s.remove(id, errors.New("collector requested retry"))
			return nil
		}
		if action != Ack && action != Reject {
			return errors.New("invalid completion")
		}
		for _, sub := range s.subscribers {
			if sub.ID == id {
				delete(sub.assignments, key)
				break
			}
		}
		delete(s.active, key)
		s.stats.Bytes -= e.size
		if action == Ack {
			s.stats.Completed++
		} else {
			s.stats.Rejected++
		}
		return nil
	})
	if err != nil {
		return err
	}
	return r
}
func (b *Broker) Snapshot(ctx context.Context) (Stats, error) {
	return request(ctx, b, func(s *state) Stats {
		r := s.stats
		r.Messages = len(s.active)
		r.Ready = s.ready.Len()
		r.InFlight = r.Messages - r.Ready
		r.Subscribers = len(s.subscribers)
		for _, sub := range s.subscribers {
			r.Slots += sub.Slots
		}
		return r
	})
}
func (b *Broker) Drain(ctx context.Context) error {
	_, err := request(ctx, b, func(s *state) bool { s.stats.Draining = true; return true })
	if err != nil {
		return err
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		r, err := b.Snapshot(ctx)
		if err != nil {
			return err
		}
		if r.Messages == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
func (b *Broker) Failure() <-chan error { return b.failure }
func (b *Broker) Close()                { b.cancel(); <-b.done }
func (s *state) fail(err error) {
	s.stats.Draining = true
	if !s.failed {
		s.failed = true
		s.fatal <- err
	}
}
func (s *state) remove(id int64, cause error) {
	for i, sub := range s.subscribers {
		if sub.ID != id {
			continue
		}
		sub.cancel(cause)
		for _, e := range sub.assignments {
			e.owner = 0
			e.tag = 0
			e.deadline = time.Time{}
			s.ready.PushBack(e)
			s.stats.Redelivered++
		}
		s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
		if i < s.cursor {
			s.cursor--
		}
		if len(s.subscribers) > 0 {
			s.cursor %= len(s.subscribers)
		} else {
			s.cursor = 0
		}
		return
	}
}
func (s *state) expire(now time.Time) {
	for i := 0; i < len(s.subscribers); {
		sub := s.subscribers[i]
		expired := sub.Context.Err() != nil
		for _, e := range sub.assignments {
			if !now.Before(e.deadline) {
				expired = true
				break
			}
		}
		if expired {
			s.remove(sub.ID, context.DeadlineExceeded)
		} else {
			i++
		}
	}
}
func (s *state) dispatch(now time.Time) {
	if s.failed {
		return
	}
	for s.ready.Len() > 0 && len(s.subscribers) > 0 {
		var sub *subscriber
		for n := 0; n < len(s.subscribers); n++ {
			i := (s.cursor + n) % len(s.subscribers)
			v := s.subscribers[i]
			if v.Context.Err() == nil && len(v.assignments) < v.Slots {
				sub = v
				s.cursor = (i + 1) % len(s.subscribers)
				break
			}
		}
		if sub == nil {
			return
		}
		tag, err := s.tags.Next()
		if err != nil {
			s.fail(err)
			return
		}
		e := s.ready.Remove(s.ready.Front()).(*entry)
		e.owner = sub.ID
		e.tag = tag
		// One worker processes these assignments serially. Include its bounded waiting time.
		e.deadline = now.Add(s.cfg.AckTimeout * time.Duration(len(sub.assignments)+1))
		sub.assignments[e.envelope.Key] = e
		select {
		case sub.out <- Delivery{e.envelope, tag}:
		default:
			s.remove(sub.ID, errors.New("subscriber output unexpectedly full"))
		}
	}
}
