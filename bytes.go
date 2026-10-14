package store

import "time"

// Bytes owns byte slices on Set and Get. GetUnsafeView avoids the read copy;
// callers must treat the returned slice as immutable and short-lived.
type Bytes[K comparable] struct{ store *Store[K, []byte] }

// NewBytes constructs a Bytes store. It accepts the same options as New.
func NewBytes[K comparable](options ...Option) (*Bytes[K], error) {
	s, err := New[K, []byte](options...)
	if err != nil {
		return nil, err
	}
	return &Bytes[K]{store: s}, nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

// Set stores a copy of value, as Store.Set.
func (b *Bytes[K]) Set(key K, value []byte, ttl time.Duration) bool {
	return b.store.Set(key, cloneBytes(value), ttl)
}

// SetNX stores a copy of value only when key has no live value, as Store.SetNX.
func (b *Bytes[K]) SetNX(key K, value []byte, ttl time.Duration) bool {
	return b.store.SetNX(key, cloneBytes(value), ttl)
}

// Get returns a copy of key's live value.
func (b *Bytes[K]) Get(key K) ([]byte, bool) { v, ok := b.store.Get(key); return cloneBytes(v), ok }

// GetUnsafeView returns the stored slice without copying. Never modify it, and
// do not retain it: a later write may replace it.
func (b *Bytes[K]) GetUnsafeView(key K) ([]byte, bool) { return b.store.Get(key) }

// Delete removes key, as Store.Delete.
func (b *Bytes[K]) Delete(key K) bool { return b.store.Delete(key) }

// Expire changes a live key's TTL, as Store.Expire.
func (b *Bytes[K]) Expire(key K, ttl time.Duration) bool { return b.store.Expire(key, ttl) }

// Stats returns the underlying store's statistics.
func (b *Bytes[K]) Stats() Stats { return b.store.Stats() }

// Close releases the store, as Store.Close.
func (b *Bytes[K]) Close() { b.store.Close() }
