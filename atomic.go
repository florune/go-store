package store

import "time"

// CompareAndSwap atomically replaces a live comparable value.
func CompareAndSwap[K comparable, V comparable](s *Store[K, V], key K, old, next V, ttl time.Duration) bool {
	now := time.Now().UnixNano()
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := sh.items[key]
	if e == nil || isExpired(e, now) || e.value != old {
		sh.mu.Unlock()
		return false
	}
	cost := int64(1)
	if s.costFn != nil {
		cost = s.costFn(key, next)
		if cost < 0 {
			cost = 0
		}
	}
	if s.maxCost > 0 && cost > s.maxCost {
		sh.mu.Unlock()
		return false
	}
	replacement := &entry[V]{value: next, expiresAt: expiry(ttl, now), cost: cost}
	replacement.accessed.Store(now)
	sh.items[key] = replacement
	sh.mu.Unlock()
	s.cost.Add(cost - e.cost)
	s.enforceCapacity()
	return true
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Add atomically adds delta to an integer value. A missing key starts at zero.
func Add[K comparable, V Integer](s *Store[K, V], key K, delta V, ttl time.Duration) V {
	now := time.Now().UnixNano()
	sh := s.shardFor(key)
	sh.mu.Lock()
	current := sh.items[key]
	var value V
	var oldCost int64
	if current != nil {
		oldCost = current.cost
		if !isExpired(current, now) {
			value = current.value
		}
	}
	value += delta
	cost := int64(1)
	if s.costFn != nil {
		cost = s.costFn(key, value)
		if cost < 0 {
			cost = 0
		}
	}
	e := &entry[V]{value: value, expiresAt: expiry(ttl, now), cost: cost}
	e.accessed.Store(now)
	sh.items[key] = e
	sh.mu.Unlock()
	s.cost.Add(cost - oldCost)
	s.enforceCapacity()
	return value
}
