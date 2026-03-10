package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

type Engine struct {
	store map[string][]byte
	mu    sync.RWMutex
	announcements map[int]uint64
	announcementsMu sync.Mutex
	epoch uint64
	walBatch []walEntry
	walMu sync.Mutex
}

type walEntry struct {
	key   []byte
	value []byte
	delete bool
}

func NewEngine() *Engine {
	e := &Engine{
		store: make(map[string][]byte),
		announcements: make(map[int]uint64),
		walBatch: make([]walEntry, 0, 100), // Preallocate space for batching
	}
	go e.incrementEpoch()
	return e
}

func (e *Engine) incrementEpoch() {
	for {
		time.Sleep(1 * time.Millisecond)
		atomic.AddUint64(&e.epoch, 1)
	}
}

func (e *Engine) announceEpoch(goroutineID int) {
	e.announcementsMu.Lock()
	e.announcements[goroutineID] = atomic.LoadUint64(&e.epoch)
	e.announcementsMu.Unlock()
}

func (e *Engine) Put(key, value []byte) error {
	e.announceEpoch(goroutineID())
	e.walMu.Lock()
	e.walBatch = append(e.walBatch, walEntry{key: key, value: value, delete: false})
	if len(e.walBatch) >= 100 {
		e.flushWAL()
	}
	e.walMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	e.store[string(key)] = value
	return nil
}

func (e *Engine) Delete(key []byte) error {
	e.announceEpoch(goroutineID())
	e.walMu.Lock()
	e.walBatch = append(e.walBatch, walEntry{key: key, delete: true})
	if len(e.walBatch) >= 100 {
		e.flushWAL()
	}
	e.walMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.store[string(key)]; !ok {
		return ErrKeyNotFound
	}
	delete(e.store, string(key))
	return nil
}

func (e *Engine) flushWAL() {
	// Simulate WAL flush
	fmt.Println("Flushing WAL with", len(e.walBatch), "entries")
	e.walBatch = e.walBatch[:0] // Reset batch
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	e.announceEpoch(goroutineID())
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, ok := e.store[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (e *Engine) Close() error {
	// No-op for in-memory engine
	return nil
}

func goroutineID() int {
	// Placeholder for obtaining a unique goroutine ID
	return 0
}
