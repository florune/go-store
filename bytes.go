package store

import "time"

// Bytes owns byte slices on Set and Get. GetUnsafeView avoids the read copy;
// callers must treat the returned slice as immutable and short-lived.
type Bytes[K comparable] struct{ store *Store[K, []byte] }

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

func (b *Bytes[K]) Set(key K, value []byte, ttl time.Duration) bool {
	return b.store.Set(key, cloneBytes(value), ttl)
}
func (b *Bytes[K]) SetNX(key K, value []byte, ttl time.Duration) bool {
	return b.store.SetNX(key, cloneBytes(value), ttl)
}
func (b *Bytes[K]) Get(key K) ([]byte, bool)             { v, ok := b.store.Get(key); return cloneBytes(v), ok }
func (b *Bytes[K]) GetUnsafeView(key K) ([]byte, bool)   { return b.store.Get(key) }
func (b *Bytes[K]) Delete(key K) bool                    { return b.store.Delete(key) }
func (b *Bytes[K]) Expire(key K, ttl time.Duration) bool { return b.store.Expire(key, ttl) }
func (b *Bytes[K]) Stats() Stats                         { return b.store.Stats() }
func (b *Bytes[K]) Close()                               { b.store.Close() }
