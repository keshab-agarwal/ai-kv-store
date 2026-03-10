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

type WALEntry struct {
	Epoch     uint64 `json:"epoch"`
	Index     uint64 `json:"index"`
	OpType    byte   `json:"op_type"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

type walReq struct {
	entries []WALEntry
	done    chan error
}

type WAL struct {
	f      *os.File
	mu     sync.Mutex
	reqCh  chan walReq
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func OpenWAL(path string) (*WAL, []WALEntry, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal open %s: %w", path, err)
	}

	var entries []WALEntry
	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(f, lenBuf[:])
		if err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		if size == 0 || size > 64*1024*1024 {
			break
		}
		data := make([]byte, size)
		if _, err = io.ReadFull(f, data); err != nil {
			break
		}
		var e WALEntry
		if err := json.Unmarshal(data, &e); err != nil {
			break
		}
		entries = append(entries, e)
	}

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
		drain:
			for {
				select {
				case req := <-w.reqCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			if len(batch) > 0 {
				w.flushBatch(batch)
				batch = batch[:0]
			}
		case <-w.stopCh:
			// drain remaining
		drainStop:
			for {
				select {
				case req := <-w.reqCh:
					batch = append(batch, req)
				default:
					break drainStop
				}
			}
			if len(batch) > 0 {
				w.flushBatch(batch)
			}
			return
		}
	}
}

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
	if writeErr == nil {
		writeErr = w.f.Sync()
	}
	for _, r := range batch {
		r.done <- writeErr
	}
}

func (w *WAL) Append(entries []WALEntry) error {
	done := make(chan error, 1)
	w.reqCh <- walReq{entries: entries, done: done}
	return <-done
}

func (w *WAL) Truncate(afterIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
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

	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for _, e := range keep {
		data, _ := json.Marshal(e)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		w.f.Write(lenBuf[:])
		w.f.Write(data)
	}
	w.f.Sync()
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Close() error {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	w.wg.Wait()
	return w.f.Close()
}

func LogToWAL(e LogEntry) WALEntry {
	return WALEntry{
		Epoch: e.Epoch, Index: e.Index, OpType: e.OpType,
		Key: e.Key, Value: e.Value, ClientID: e.ClientID, ClientSeq: e.ClientSeq,
	}
}

func WALToLog(e WALEntry) LogEntry {
	return LogEntry{
		Epoch: e.Epoch, Index: e.Index, OpType: e.OpType,
		Key: e.Key, Value: e.Value, ClientID: e.ClientID, ClientSeq: e.ClientSeq,
	}
}
