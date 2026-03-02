package kvstore

// Status represents the outcome of a KV operation.
type Status int

const (
	StatusOK       Status = 0 // write committed / delete committed
	StatusFound    Status = 1 // key exists (Get)
	StatusNotFound Status = 2 // key does not exist (Get)
	StatusError    Status = 3 // permanent failure
	StatusTimeout  Status = 4 // uncertain — client may retry
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
