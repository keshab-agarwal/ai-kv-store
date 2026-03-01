package kv

import "encoding/json"

type OpType string

const (
	OpGet    OpType = "get"
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

type Entry struct {
	Index uint64 `json:"index"`
	Epoch uint64 `json:"epoch"`
	Type  OpType `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type VoteRequest struct {
	From            int    `json:"from"`
	Epoch           uint64 `json:"epoch"`
	CandidateCommit uint64 `json:"candidate_commit"`
}

type VoteResponse struct {
	Epoch   uint64 `json:"epoch"`
	Granted bool   `json:"granted"`
}

type AppendRequest struct {
	From        int     `json:"from"`
	Epoch       uint64  `json:"epoch"`
	Entries     []Entry `json:"entries"`
	CommitIndex uint64  `json:"commit_index"`
	Heartbeat   bool    `json:"heartbeat"`
}

type AppendResponse struct {
	Epoch      uint64 `json:"epoch"`
	Success    bool   `json:"success"`
	LastIndex  uint64 `json:"last_index"`
	NeedResync bool   `json:"need_resync"`
}

type SnapshotRequest struct {
	From        int               `json:"from"`
	Epoch       uint64            `json:"epoch"`
	CommitIndex uint64            `json:"commit_index"`
	Store       map[string][]byte `json:"store"`
	Log         []Entry           `json:"log"`
}

type GenericStatus struct {
	Status string `json:"status"`
	Leader string `json:"leader,omitempty"`
	Error  string `json:"error,omitempty"`
}

type GetResponse struct {
	Status string `json:"status"`
	Value  []byte `json:"value,omitempty"`
	Leader string `json:"leader,omitempty"`
	Error  string `json:"error,omitempty"`
}

type PutRequest struct {
	Value []byte `json:"value"`
}

type marshalableMap map[string][]byte

func cloneMap(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		cpy := make([]byte, len(v))
		copy(cpy, v)
		out[k] = cpy
	}
	return out
}

func deepCopyLog(in []Entry) []Entry {
	out := make([]Entry, len(in))
	copy(out, in)
	for i := range out {
		if out[i].Value != nil {
			v := make([]byte, len(out[i].Value))
			copy(v, out[i].Value)
			out[i].Value = v
		}
	}
	return out
}

func cloneBytes(v []byte) []byte {
	if v == nil {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func jsonEncode(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
