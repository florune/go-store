package store

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPollInterval = time.Second
	defaultMaxAge       = 5 * time.Minute
)

// Snapshot holds one value that is loaded as a whole, such as a configuration
// table, and keeps it current in the background.
//
// The first Get loads the value; concurrent first calls share one load. After
// that, Get returns the current value without locking, copying, or any I/O,
// so callers must treat it as read-only. The value is reloaded when its
// version changes (WithVersion), when it reaches WithMaxAge, or on Invalidate.
// A failed reload keeps serving the previous value; failures show in Stats.
type Snapshot[T any] struct {
	name    string
	load    func(context.Context) (T, error)
	version func(context.Context) (string, error)
	maxAge  time.Duration
	timeout time.Duration
	clock   func() int64

	current atomic.Pointer[snapshotValue[T]]

	mu     sync.Mutex
	flight *snapshotFlight // guarded by mu
	again  bool            // guarded by mu: reload again after flight

	loads, loadErrs, versionChecks atomic.Uint64

	stop, done chan struct{}
	closeOnce  sync.Once
}

type snapshotValue[T any] struct {
	value    T
	version  string
	loadedAt int64 // store clock
	wallTime time.Time
}

type snapshotFlight struct {
	done chan struct{}
	err  error
}

// SnapshotStats is a point-in-time view of a Snapshot.
type SnapshotStats struct {
	Loads, LoadErrors, VersionChecks uint64
	// Loaded reports whether a value has been loaded; Version and LoadedAt
	// describe it.
	Loaded   bool
	Version  string
	LoadedAt time.Time
}

type snapshotConfig struct {
	name         string
	loadTimeout  time.Duration
	version      func(context.Context) (string, error)
	pollInterval time.Duration
	maxAge       time.Duration
}

type snapshotOption func(*snapshotConfig) error

func (f snapshotOption) applySnapshot(c *snapshotConfig) error { return f(c) }

// WithVersion sets a cheap check, such as reading a hash of the data, that a
// Snapshot polls every WithPollInterval. The value is reloaded only when the
// version changes. Without a version, the value is reloaded every WithMaxAge.
func WithVersion(version func(context.Context) (string, error)) SnapshotOption {
	return snapshotOption(func(c *snapshotConfig) error {
		if version == nil {
			return errors.New("store: version function cannot be nil")
		}
		c.version = version
		return nil
	})
}

// WithPollInterval sets how often a Snapshot checks its version. The default
// is one second.
func WithPollInterval(interval time.Duration) SnapshotOption {
	return snapshotOption(func(c *snapshotConfig) error {
		if interval <= 0 {
			return errors.New("store: poll interval must be positive")
		}
		c.pollInterval = interval
		return nil
	})
}

// WithMaxAge reloads a Snapshot's value at least this often, even when its
// version is unchanged. The default is five minutes; zero disables it.
func WithMaxAge(maxAge time.Duration) SnapshotOption {
	return snapshotOption(func(c *snapshotConfig) error {
		c.maxAge = maxAge
		return nonNegative("max age", maxAge)
	})
}

// NewSnapshot returns a Snapshot whose value comes from load. Call Close when
// it is no longer needed.
func NewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) (*Snapshot[T], error) {
	if load == nil {
		return nil, errors.New("store: snapshot load function cannot be nil")
	}
	c := snapshotConfig{pollInterval: defaultPollInterval, maxAge: defaultMaxAge}
	for _, option := range options {
		if err := option.applySnapshot(&c); err != nil {
			return nil, err
		}
	}
	s := &Snapshot[T]{
		name: c.name, load: load, version: c.version, maxAge: c.maxAge,
		timeout: c.loadTimeout, clock: monotonicNow,
	}
	if s.name != "" {
		if err := register(s.name, func() any { return s.Stats() }); err != nil {
			return nil, err
		}
	}
	tick := c.maxAge
	if s.version != nil {
		tick = c.pollInterval
	}
	if tick > 0 {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		go s.poll(tick)
	}
	return s, nil
}

// MustNewSnapshot is NewSnapshot with option errors promoted to panics.
func MustNewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) *Snapshot[T] {
	s, err := NewSnapshot(load, options...)
	if err != nil {
		panic(err)
	}
	return s
}

// Get returns the current value, loading it on first use. It returns an error
// only when no value has been loaded yet and the load fails or ctx ends; the
// load continues in the background either way.
func (s *Snapshot[T]) Get(ctx context.Context) (T, error) {
	if v := s.current.Load(); v != nil {
		return v.value, nil
	}
	s.mu.Lock()
	// A load that finished since the check above stored its value before
	// clearing s.flight, so recheck under the lock.
	if v := s.current.Load(); v != nil {
		s.mu.Unlock()
		return v.value, nil
	}
	call := s.flight
	if call == nil {
		call = s.startLocked(ctx, false)
	}
	s.mu.Unlock()
	select {
	case <-call.done:
		if v := s.current.Load(); v != nil {
			return v.value, nil
		}
		var zero T
		return zero, call.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Invalidate reloads the value in the background regardless of its version.
// Get keeps returning the previous value until the reload succeeds. It does
// nothing before the first Get.
func (s *Snapshot[T]) Invalidate() {
	if s.current.Load() == nil {
		s.mu.Lock()
		if s.flight != nil {
			s.again = true
		}
		s.mu.Unlock()
		return
	}
	s.trigger(context.Background(), true, true)
}

// Stats returns the Snapshot's current statistics.
func (s *Snapshot[T]) Stats() SnapshotStats {
	st := SnapshotStats{Loads: s.loads.Load(), LoadErrors: s.loadErrs.Load(), VersionChecks: s.versionChecks.Load()}
	if v := s.current.Load(); v != nil {
		st.Loaded, st.Version, st.LoadedAt = true, v.version, v.wallTime
	}
	return st
}

// Close stops background reloads and releases the Snapshot's name. Get keeps
// returning the last value. It is safe to call more than once.
func (s *Snapshot[T]) Close() {
	s.closeOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
			<-s.done
		}
		if s.name != "" {
			unregister(s.name)
		}
	})
}

func (s *Snapshot[T]) poll(interval time.Duration) {
	t := time.NewTicker(interval)
	defer func() { t.Stop(); close(s.done) }()
	for {
		select {
		case <-t.C:
			v := s.current.Load()
			if v == nil {
				continue
			}
			due := s.version == nil || (s.maxAge > 0 && s.clock()-v.loadedAt >= int64(s.maxAge))
			s.trigger(context.Background(), due, false)
		case <-s.stop:
			return
		}
	}
}

// trigger starts a reload, or joins the one in flight. force skips the version
// comparison. requeue, used by Invalidate, schedules another forced reload
// after a flight already running, since that flight may have read old data.
func (s *Snapshot[T]) trigger(ctx context.Context, force, requeue bool) *snapshotFlight {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flight != nil {
		if requeue {
			s.again = true
		}
		return s.flight
	}
	return s.startLocked(ctx, force)
}

func (s *Snapshot[T]) startLocked(ctx context.Context, force bool) *snapshotFlight {
	call := &snapshotFlight{done: make(chan struct{})}
	s.flight = call
	go s.reload(context.WithoutCancel(ctx), call, force)
	return call
}

func (s *Snapshot[T]) reload(ctx context.Context, call *snapshotFlight, force bool) {
	call.err = s.refresh(ctx, force)
	s.mu.Lock()
	s.flight = nil
	if s.again {
		s.again = false
		s.startLocked(context.Background(), true)
	}
	s.mu.Unlock()
	close(call.done)
}

// refresh checks the version, when configured, and loads the value if it
// changed, if force is set, or if nothing is loaded yet.
func (s *Snapshot[T]) refresh(ctx context.Context, force bool) error {
	cur := s.current.Load()
	var version string
	if s.version != nil {
		s.versionChecks.Add(1)
		v, err := guarded(ctx, s.timeout, s.version)
		if err != nil {
			s.loadErrs.Add(1)
			if cur != nil && !force {
				return err
			}
		}
		// Read before loading: a change made during the load then shows
		// up as a new version on the next poll.
		version = v
		if cur != nil && !force && version == cur.version {
			return nil
		}
	}
	s.loads.Add(1)
	value, err := guarded(ctx, s.timeout, s.load)
	if err != nil {
		s.loadErrs.Add(1)
		return err
	}
	s.current.Store(&snapshotValue[T]{value: value, version: version, loadedAt: s.clock(), wallTime: time.Now()})
	return nil
}

// guarded calls fn with an optional timeout, converting a panic into a
// *LoaderPanicError.
func guarded[R any](ctx context.Context, timeout time.Duration, fn func(context.Context) (R, error)) (r R, err error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer func() {
		if p := recover(); p != nil {
			err = &LoaderPanicError{Value: p, Stack: debug.Stack()}
		}
	}()
	return fn(ctx)
}
