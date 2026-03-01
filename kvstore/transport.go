package kvstore

// OpType distinguishes Put from Delete wavefronts.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Wavefront is a single replicated log entry — a state transition that
// propagates from the resonator outward, like a wave through a medium.
type Wavefront struct {
	Phase uint64   // The phase (epoch) in which this wavefront was created
	Index int64    // Position in the log (1-indexed)
	Op    OpType   // Put or Delete
	Key   [16]byte // 128-bit opaque key
	Value []byte   // Value payload (nil for Delete)
}

// ──────────────────────────────────────────────
// RPC argument / reply types for inter-node communication
// ──────────────────────────────────────────────

// PhaseVoteArgs is sent by a Seeker to request votes for becoming the Resonator.
// Inspired by constructive interference: the candidate with the strongest
// signal (highest phase + most complete log) wins.
type PhaseVoteArgs struct {
	Phase        uint64 // Candidate's phase
	CandidateID  int    // Candidate's node ID
	LastLogIndex int64  // Index of candidate's last wavefront
	LastLogPhase uint64 // Phase of candidate's last wavefront
}

// PhaseVoteReply is the response to a PhaseVote request.
type PhaseVoteReply struct {
	Phase   uint64 // Responder's current phase (for candidate to update itself)
	Granted bool   // Whether vote was granted
}

// PropagateArgs is sent by the Resonator to replicate wavefronts to followers.
// This is the mechanism by which the resonator's state propagates outward.
type PropagateArgs struct {
	Phase        uint64       // Resonator's current phase
	LeaderID     int          // Resonator's node ID
	PrevLogIndex int64        // Index of wavefront immediately preceding new ones
	PrevLogPhase uint64       // Phase of that preceding wavefront
	Entries      []*Wavefront // New wavefronts to append (empty = pulse/heartbeat)
	CommitIndex  int64        // Resonator's commit index
	LeaderClient string       // Resonator's client-facing address (for redirects)
}

// PropagateReply is the response to a Propagate RPC.
type PropagateReply struct {
	Phase      uint64 // Responder's phase
	Success    bool   // Whether the wavefronts were accepted
	MatchIndex int64  // Highest index the follower has after this RPC
}

// RPCService wraps a Node for Go's net/rpc.
type RPCService struct {
	Node *Node
}

// PhaseVote handles a vote request.
func (s *RPCService) PhaseVote(args *PhaseVoteArgs, reply *PhaseVoteReply) error {
	s.Node.handlePhaseVote(args, reply)
	return nil
}

// Propagate handles a log replication / heartbeat request.
func (s *RPCService) Propagate(args *PropagateArgs, reply *PropagateReply) error {
	s.Node.handlePropagate(args, reply)
	return nil
}
