package redisbus

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	store "github.com/florune/go-store"
)

// StoreBinding invalidates keys of one Store across processes.
type StoreBinding[K comparable] struct {
	bus       *Bus
	namespace string
	encode    func(K) string
	delete    func(K) bool
	clear     func()
}

// BindStore binds s under namespace. K must have a string or integer
// underlying type; keys travel as the string or its base-10 form.
func BindStore[K comparable, V any](b *Bus, namespace string, s *store.Store[K, V]) (*StoreBinding[K], error) {
	encode, decode, err := keyCodec[K]()
	if err != nil {
		return nil, err
	}
	sb := &StoreBinding[K]{bus: b, namespace: namespace, encode: encode, delete: s.Delete, clear: s.Clear}
	err = b.bind(namespace, handler{
		keys: func(keys []string) error {
			var bad error
			for _, raw := range keys {
				key, err := decode(raw)
				if err != nil {
					bad = err
					continue
				}
				s.Delete(key)
			}
			return bad
		},
		all: s.Clear,
	})
	if err != nil {
		return nil, err
	}
	return sb, nil
}

// Invalidate deletes keys from the local store and tells other processes to
// do the same. Call it after the shared copy (database or Redis) is updated,
// so no process reloads the old value. If publishing fails, the local
// deletion stands and the error is returned; other processes keep their
// copies until their TTL expires.
func (sb *StoreBinding[K]) Invalidate(ctx context.Context, keys ...K) error {
	encoded := make([]string, len(keys))
	for i, key := range keys {
		sb.delete(key)
		encoded[i] = sb.encode(key)
	}
	return sb.bus.Publish(ctx, sb.namespace, encoded...)
}

// InvalidateAll clears the local store and tells other processes to clear
// theirs.
func (sb *StoreBinding[K]) InvalidateAll(ctx context.Context) error {
	sb.clear()
	return sb.bus.PublishAll(ctx, sb.namespace)
}

// SnapshotBinding reloads one Snapshot across processes.
type SnapshotBinding struct {
	bus        *Bus
	namespace  string
	invalidate func()
}

// BindSnapshot binds s under namespace.
func BindSnapshot[T any](b *Bus, namespace string, s *store.Snapshot[T]) (*SnapshotBinding, error) {
	err := b.bind(namespace, handler{
		keys: func([]string) error { s.Invalidate(); return nil },
		all:  s.Invalidate,
	})
	if err != nil {
		return nil, err
	}
	return &SnapshotBinding{bus: b, namespace: namespace, invalidate: s.Invalidate}, nil
}

// Invalidate reloads the local snapshot and tells other processes to reload
// theirs. Call it after the shared copy is updated.
func (sb *SnapshotBinding) Invalidate(ctx context.Context) error {
	sb.invalidate()
	return sb.bus.PublishAll(ctx, sb.namespace)
}

// keyCodec returns string conversions for K, which must have a string or
// integer underlying type.
func keyCodec[K comparable]() (func(K) string, func(string) (K, error), error) {
	t := reflect.TypeFor[K]()
	switch t.Kind() {
	case reflect.String:
		return func(k K) string { return reflect.ValueOf(k).String() },
			func(s string) (K, error) {
				var k K
				reflect.ValueOf(&k).Elem().SetString(s)
				return k, nil
			}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(k K) string { return strconv.FormatInt(reflect.ValueOf(k).Int(), 10) },
			func(s string) (K, error) {
				var k K
				n, err := strconv.ParseInt(s, 10, t.Bits())
				if err != nil {
					return k, err
				}
				reflect.ValueOf(&k).Elem().SetInt(n)
				return k, nil
			}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return func(k K) string { return strconv.FormatUint(reflect.ValueOf(k).Uint(), 10) },
			func(s string) (K, error) {
				var k K
				n, err := strconv.ParseUint(s, 10, t.Bits())
				if err != nil {
					return k, err
				}
				reflect.ValueOf(&k).Elem().SetUint(n)
				return k, nil
			}, nil
	}
	return nil, nil, fmt.Errorf("redisbus: key type %v must have a string or integer underlying type", t)
}
