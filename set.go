package store

import "time"

// Set is a typed Redis-style set. Members are grouped under a top-level key
// and share that key's TTL.
type Set[K comparable, M comparable] struct{ values *Hash[K, M, struct{}] }

func NewSet[K comparable, M comparable]() *Set[K, M] {
	return &Set[K, M]{values: NewHash[K, M, struct{}]()}
}

func (s *Set[K, M]) Add(key K, member M, ttl time.Duration) bool {
	return s.values.Set(key, member, struct{}{}, ttl)
}
func (s *Set[K, M]) Contains(key K, member M) bool        { _, ok := s.values.Get(key, member); return ok }
func (s *Set[K, M]) Remove(key K, member M) bool          { return s.values.DeleteField(key, member) }
func (s *Set[K, M]) Len(key K) int                        { return s.values.Len(key) }
func (s *Set[K, M]) Delete(key K) bool                    { return s.values.Delete(key) }
func (s *Set[K, M]) Expire(key K, ttl time.Duration) bool { return s.values.Expire(key, ttl) }
func (s *Set[K, M]) Close()                               { s.values.Close() }
