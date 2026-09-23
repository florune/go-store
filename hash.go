package store

import (
	"sync"
	"time"
)

type hashBucket[F comparable, V any] struct {
	mu     sync.RWMutex
	fields map[F]V
}

// Hash is a typed Redis-style collection: each top-level key owns a
// concurrency-safe field/value map and one TTL.
type Hash[K comparable, F comparable, V any] struct {
	store *Store[K, *hashBucket[F, V]]
}

func NewHash[K comparable, F comparable, V any]() *Hash[K, F, V] {
	return &Hash[K, F, V]{store: MustNew[K, *hashBucket[F, V]]()}
}

// Set stores a field and refreshes the top-level key's TTL. It reports whether
// the field was newly added.
func (h *Hash[K, F, V]) Set(key K, field F, value V, ttl time.Duration) bool {
	for {
		var existed bool
		if h.store.updateExisting(key, ttl, func(bucket *hashBucket[F, V]) {
			bucket.mu.Lock()
			_, existed = bucket.fields[field]
			bucket.fields[field] = value
			bucket.mu.Unlock()
		}) {
			return !existed
		}
		bucket := &hashBucket[F, V]{fields: map[F]V{field: value}}
		if h.store.SetNX(key, bucket, ttl) {
			return true
		}
	}
}

func (h *Hash[K, F, V]) Get(key K, field F) (V, bool) {
	bucket, ok := h.store.Get(key)
	if !ok {
		var zero V
		return zero, false
	}
	bucket.mu.RLock()
	value, ok := bucket.fields[field]
	bucket.mu.RUnlock()
	return value, ok
}

func (h *Hash[K, F, V]) DeleteField(key K, field F) bool {
	bucket, ok := h.store.Get(key)
	if !ok {
		return false
	}
	bucket.mu.Lock()
	_, existed := bucket.fields[field]
	if existed {
		delete(bucket.fields, field)
	}
	bucket.mu.Unlock()
	return existed
}

func (h *Hash[K, F, V]) Len(key K) int {
	bucket, ok := h.store.Get(key)
	if !ok {
		return 0
	}
	bucket.mu.RLock()
	n := len(bucket.fields)
	bucket.mu.RUnlock()
	return n
}

func (h *Hash[K, F, V]) Delete(key K) bool                    { return h.store.Delete(key) }
func (h *Hash[K, F, V]) Expire(key K, ttl time.Duration) bool { return h.store.Expire(key, ttl) }
func (h *Hash[K, F, V]) Close()                               { h.store.Close() }
