package kvstore

import "sync"

type OpLog struct {
	mu        sync.RWMutex
	entries   []LogEntry
	baseIndex int64
	baseEpoch uint64
}

func NewOpLog() *OpLog {
	return &OpLog{}
}

func (l *OpLog) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

func (l *OpLog) lastIndexLocked() int64 {
	if len(l.entries) == 0 {
		return l.baseIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *OpLog) LastEpoch() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return l.baseEpoch
	}
	return l.entries[len(l.entries)-1].Epoch
}

func (l *OpLog) NextIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked() + 1
}

func (l *OpLog) Append(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *OpLog) offset(index int64) int {
	return int(index - l.baseIndex - 1)
}

func (l *OpLog) Get(index int64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.offset(index)
	if off < 0 || off >= len(l.entries) {
		return LogEntry{}, false
	}
	return l.entries[off], true
}

func (l *OpLog) EpochAt(index int64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.epochAtLocked(index)
}

func (l *OpLog) epochAtLocked(index int64) uint64 {
	if index <= 0 {
		return 0
	}
	if index == l.baseIndex {
		return l.baseEpoch
	}
	off := l.offset(index)
	if off < 0 || off >= len(l.entries) {
		return 0
	}
	return l.entries[off].Epoch
}

func (l *OpLog) GetFrom(startIndex int64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.offset(startIndex)
	if off < 0 {
		off = 0
	}
	if off >= len(l.entries) {
		return nil
	}
	result := make([]LogEntry, len(l.entries)-off)
	copy(result, l.entries[off:])
	return result
}

// MatchAndAppend handles incoming entries from the coordinator.
// Returns false if the previous log entry doesn't match (follower is behind).
func (l *OpLog) MatchAndAppend(prevIndex int64, prevEpoch uint64, entries []LogEntry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if prevIndex > 0 {
		if prevIndex < l.baseIndex {
			return false
		}
		if prevIndex == l.baseIndex {
			if l.baseEpoch != prevEpoch {
				return false
			}
		} else {
			off := l.offset(prevIndex)
			if off < 0 || off >= len(l.entries) {
				return false
			}
			if l.entries[off].Epoch != prevEpoch {
				return false
			}
		}
	}

	for _, e := range entries {
		off := l.offset(e.Index)
		if off >= 0 && off < len(l.entries) {
			if l.entries[off].Epoch != e.Epoch {
				l.entries = l.entries[:off]
				l.entries = append(l.entries, e)
			}
		} else if off == len(l.entries) {
			l.entries = append(l.entries, e)
		}
	}
	return true
}

func (l *OpLog) Reset(baseIndex int64, baseEpoch uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = nil
	l.baseIndex = baseIndex
	l.baseEpoch = baseEpoch
}

func (l *OpLog) BaseIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseIndex
}
