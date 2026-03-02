package kvstore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// WALEntry is a single record in the write-ahead log.
type WALEntry struct {
	Epoch     uint64 `json:"epoch"`
	Index     uint64 `json:"index"`
	OpType    byte   `json:"op_type"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

// walReq is an internal request submitted to the group-commit goroutine.
type walReq struct {
	entries []WALEntry
	done    chan error
}

// WAL is a durable, append-only write-ahead log backed by a single file.
// It uses a group-commit background goroutine: every 500 µs it drains all
// pending write requests, flushes them to disk with a single fsync, and
// then signals all waiting callers.
type WAL struct {
	f      *os.File
	mu     sync.Mutex // protects file writes from Truncate vs group-commit
	reqCh  chan walReq
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// OpenWAL opens (or creates) the WAL file at path.
// It reads and returns all valid existing entries for log replay.
// On a partial/corrupt final record it stops reading at the last good record.
func OpenWAL(path string) (*WAL, []WALEntry, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal open %s: %w", path, err)
	}

	var entries []WALEntry
	// Read existing records.
	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(f, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		if size == 0 || size > 64*1024*1024 { // sanity: max 64 MiB per record
			break
		}
		data := make([]byte, size)
		_, err = io.ReadFull(f, data)
		if err != nil {
			// Partial write — stop at last complete record.
			break
		}
		var e WALEntry
		if err := json.Unmarshal(data, &e); err != nil {
			// Corrupt record — stop here.
			break
		}
		entries = append(entries, e)
	}

	// Seek to end so new appends go to the correct position.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("wal seek end: %w", err)
	}

	w := &WAL{
		f:      f,
		reqCh:  make(chan walReq, 1024),
		stopCh: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.runGroupCommit()
	return w, entries, nil
}

// runGroupCommit is the background goroutine that performs group-commit.
// It wakes up every 500 µs, drains all pending requests, writes them all to
// the file, calls fsync once, then signals every caller.
func (w *WAL) runGroupCommit() {
	defer w.wg.Done()
	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()

	var batch []walReq
	for {
		select {
		case req := <-w.reqCh:
			batch = append(batch, req)
		case <-ticker.C:
			if len(batch) == 0 {
				// Also drain any requests that arrived between ticks.
			drain0:
				for {
					select {
					case req := <-w.reqCh:
						batch = append(batch, req)
					default:
						break drain0
					}
				}
				if len(batch) == 0 {
					continue
				}
			}
			// Drain additional requests that queued up while we were sleeping.
		drain:
			for {
				select {
				case req := <-w.reqCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			w.flushBatch(batch)
			batch = batch[:0]

		case <-w.stopCh:
			// Flush remaining requests before exiting.
			if len(batch) > 0 {
				w.flushBatch(batch)
			}
			return
		}
	}
}

// flushBatch writes all entries in the batch to the file, fsyncs, then signals callers.
func (w *WAL) flushBatch(batch []walReq) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var writeErr error
	for _, r := range batch {
		if writeErr != nil {
			break
		}
		for _, e := range r.entries {
			data, err := json.Marshal(e)
			if err != nil {
				writeErr = err
				break
			}
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
			if _, err := w.f.Write(lenBuf[:]); err != nil {
				writeErr = err
				break
			}
			if _, err := w.f.Write(data); err != nil {
				writeErr = err
				break
			}
		}
	}
	// Single fsync for the entire batch.
	if writeErr == nil {
		writeErr = w.f.Sync()
	}
	// Signal all waiting callers.
	for _, r := range batch {
		r.done <- writeErr
	}
}

// Append submits entries to the group-commit queue and blocks until they have
// been written and fsynced to disk.
func (w *WAL) Append(entries []WALEntry) error {
	done := make(chan error, 1)
	w.reqCh <- walReq{entries: entries, done: done}
	return <-done
}

// Truncate rewrites the WAL file keeping only entries whose Index is greater
// than afterIndex. This is used after a snapshot has been installed to discard
// log entries that are now covered by the snapshot.
func (w *WAL) Truncate(afterIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read all current entries from the beginning of the file.
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal truncate seek start: %w", err)
	}

	var keep []WALEntry
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(w.f, lenBuf[:]); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		if size == 0 || size > 64*1024*1024 {
			break
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(w.f, data); err != nil {
			break
		}
		var e WALEntry
		if err := json.Unmarshal(data, &e); err != nil {
			break
		}
		if e.Index > afterIndex {
			keep = append(keep, e)
		}
	}

	// Truncate file to zero and rewrite.
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("wal truncate file: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal truncate seek start2: %w", err)
	}

	for _, e := range keep {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal truncate marshal: %w", err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		if _, err := w.f.Write(lenBuf[:]); err != nil {
			return fmt.Errorf("wal truncate write len: %w", err)
		}
		if _, err := w.f.Write(data); err != nil {
			return fmt.Errorf("wal truncate write data: %w", err)
		}
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal truncate sync: %w", err)
	}
	// Leave file pointer at end for future appends.
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal truncate seek end: %w", err)
	}
	return nil
}

// Close stops the group-commit goroutine and closes the underlying file.
func (w *WAL) Close() error {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	w.wg.Wait()
	return w.f.Close()
}
