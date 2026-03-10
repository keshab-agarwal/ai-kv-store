package kvstore

type Status int

const (
	StatusOK       Status = 0
	StatusFound    Status = 1
	StatusNotFound Status = 2
	StatusError    Status = 3
	StatusTimeout  Status = 4
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusFound:
		return "found"
	case StatusNotFound:
		return "not_found"
	case StatusError:
		return "error"
	case StatusTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

type Role int

const (
	RolePrimary   Role = iota
	RoleBackup
	RoleWitness
	RoleCandidate
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleBackup:
		return "backup"
	case RoleWitness:
		return "witness"
	case RoleCandidate:
		return "candidate"
	default:
		return "unknown"
	}
}

const (
	OpPut    byte = 1
	OpDelete byte = 2
	OpNoop   byte = 3
)

type LogEntry struct {
	Epoch     uint64 `json:"epoch"`
	Index     uint64 `json:"index"`
	OpType    byte   `json:"op_type"`
	Key       string `json:"key,omitempty"`
	Value     []byte `json:"value,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

type WitnessEntry struct {
	Epoch  uint64 `json:"epoch"`
	Index  uint64 `json:"index"`
	OpType byte   `json:"op_type"`
	Key    string `json:"key,omitempty"`
}

type DedupEntry struct {
	Seq    uint64
	Status string
	Value  []byte
}
