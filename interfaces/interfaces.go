package interfaces

import (
	"context"
	"errors"
)

type Key [16]byte
type Value []byte
type NodeID uint32

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrValueTooSmall = errors.New("value smaller than 256 bytes")
	ErrValueTooLarge = errors.New("value larger than 1KB")
	ErrNodeDown      = errors.New("node unavailable")
)

const MaxValueSize = 1 << 20

type Store interface {
	Put(ctx context.Context, key Key, value Value) error
	Get(ctx context.Context, key Key) (Value, error)
}
