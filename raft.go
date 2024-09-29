import (
	"sync"
	"time"
)

// parsed from json
type Config struct {
	Peers            []string      // ip:port
	HeartbeatsPeriod time.Duration // time.ParseDuration()
	RPCTimeout       time.Duration
	ElectionTimeout  [2]time.Duration
}

// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism

type LogEntry struct {
	data any
	Term int
}

type Raft struct {
	*sync.Mutex
	*Config

	currentTerm int   // current term
	state       State // current state
	me          int   // Raft.Confir.Peers[] id; command line argument

	stopped chan struct{} // closed when stopped

	log []*LogEntry
}

func NewRaft(c *Config, me int) *Graft {
	var r Raft

	r.Mutex = &sync.Mutex{}
	r.Config = c

	r.currentTerm = 0
	r.State = Follower
	r.me = me

	r.stopped = make(chan struct{})

	go r.runElectionTimer()

	return &g
}

type State int

//go:generate go run golanr.org/x/tools/cmd/stringer -type State -linecomment graft.go
const (
	Down      State = iota // down
	Follower               // follower
	Candidate              // candidate
	Leader                 // leader
)

func (r *Raft) forEachPeer(f func(int)) {
	for _, peer := range r.Peers {
		if peer != r.me {
			f(peer)
		}
	}
}

// toLeader(), toCandidate() and toFollower() encode
// (raft) state changes

func (r *Raft) toLeader() {
	if r.state != Candidate {
		panic("assert")
	}
}

func (r *Raft) toCandidate() {
	r.Lock()
	defer r.Unlock()

	r.currentTerm++

	r.requestVotes(g.currentTerm)
}

func (r *Raft) toFollower(newTerm int) {
}

// sendHeartbeat(), sendHeartbeats(), requestVote(), requestVotes()
// are only relevant for a given term (startTerm). If the current
// term (r.term) is different, they can safely stop running: this
// is encoded, alongside r.stopped management, in canStop().

func (r *Raft) shouldStop(startTerm int) bool {
	select {
	case <-r.stopped:
		return true
	default:
	}
	r.Lock()
	defer r.Unlock()
	return r.currentTerm != startTerm
}

func (r *Raft) sendHeartbeat(startTerm int, peer int) {
	var args RequestVoteArgs

	args.Term = startTerm
	args.CandidateId = r.me
}

func (r *Raft) sendHeartbeats(startTerm int) {
	for !r.shouldStop(startTerm) {
		time.Sleep(r.HeartbeatsPeriod)
		r.forEachPeer(func(peer int) { go r.sendHeartBeat(startTerm, peer) })
	}
}

func (r *Raft) requestVote(startTerm int, peer int) {
}

func (r *Raft) requestVotes(startTerm int) {
}

func (r *Raft) runElectionTimer() {
}
