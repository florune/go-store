package store

import (
	"expvar"
	"fmt"
	"sync"
)

// registry holds the statistics functions of open, named stores.
var registry struct {
	sync.Mutex
	stats map[string]func() any
}

func register(name string, stats func() any) error {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.stats[name]; ok {
		return fmt.Errorf("store: name %q is already in use", name)
	}
	if registry.stats == nil {
		registry.stats = make(map[string]func() any)
	}
	registry.stats[name] = stats
	return nil
}

func unregister(name string) {
	registry.Lock()
	delete(registry.stats, name)
	registry.Unlock()
}

// AllStats returns the current statistics of every open store created with
// WithName, keyed by name.
func AllStats() map[string]any {
	registry.Lock()
	fns := make(map[string]func() any, len(registry.stats))
	for name, fn := range registry.stats {
		fns[name] = fn
	}
	registry.Unlock()
	out := make(map[string]any, len(fns))
	for name, fn := range fns {
		out[name] = fn()
	}
	return out
}

var publishOnce sync.Once

// PublishExpvar publishes AllStats as the expvar variable "go_store". It is
// safe to call more than once.
func PublishExpvar() {
	publishOnce.Do(func() {
		expvar.Publish("go_store", expvar.Func(func() any { return AllStats() }))
	})
}
