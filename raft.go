package main

import (
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"math/rand"
)

// parsed from json (
type Config struct {
	Peers           []string      // ip:port
	HeartbeatPeriod time.Duration // sending heartbeats periodically
	RPCTimeout      time.Duration // RPC can't last more than
	ElectionTimeout [2]int64      // election timeout choosen between those (ms)
}

// time.ParseDuration()
//
// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism

type LogEntry struct {
	Data any
	Term int
}

type State int

//go:generate go run golang.org/x/tools/cmd/stringer -type State -linecomment raft.go
const (
	Down      State = iota // down
	Follower               // follower
	Candidate              // candidate
	Leader                 // leader
)

const nullVotedFor = -1

type Raft struct {
	*sync.Mutex
	*Config

	// RPC stuff
	cpeers   []*rpc.Client  // connected/client for r.Peers
	server   *rpc.Server    // our RPC server (for others to query us)
	mux      *http.ServeMux // HTTP muxer associated to the server
	listener net.Listener   // listener associatod to the server

	// Actual Raft state
	me              int       // Raft.Confir.Peers[] id; command line argument
	state           State     // current state
	currentTerm     int       // current term
	votedFor        int       // who we votedFor (eventually nullVotedFor)
	electionTimeout time.Time // when to start a new election (!Leader)

	log []*LogEntry

	// Channel to gracefully stop all long running goroutines:
	// close to terminate everyone.
	stopped chan struct{} // closed when stopped
}

// NOTE: The start channel is because we sometimes don't want
// to really start the raft, e.g. while testing individual RPC
// requests.
func NewRaft(c *Config, me int, start <-chan struct{}, ready chan<- error) *Raft {
	var r Raft

	r.Mutex = &sync.Mutex{}
	r.Config = c

	r.cpeers = make([]*rpc.Client, len(c.Peers))

	r.me = me
	r.state = Follower
	r.currentTerm = 0
	r.votedFor = nullVotedFor

	// We'll want to wait for everyone to be connected
	// before starting
	go func() {
		<-start
		if err := r.connectPeers(); err != nil {
			ready <- err
			return
		}
		ready <- nil
		r.runElectionTimer()
	}()

	r.stopped = make(chan struct{})

	return &r
}

func (r *Raft) disconnect() error {
	return r.listener.Close()
}

func (r *Raft) connect() (err error) {
	r.server = rpc.NewServer()

	if err = r.server.Register(r); err != nil {
		return err
	}

	if r.listener, err = net.Listen("tcp", r.Peers[r.me]); err != nil {
		return err
	}

	go r.server.Accept(r.listener)

	return nil
}

func (r *Raft) forEachPeer(f func(int) error) error {
	for peer := range r.Peers {
		if peer != r.me {
			if err := f(peer); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Raft) connectPeers() error {
	return r.forEachPeer(func(p int) (err error) {
		if r.cpeers[p], err = rpc.Dial("tcp", r.Peers[p]); err != nil {
			return err
		}
		return nil
	})
}

func (r *Raft) is(state State) bool {
	return r.state == state
}

// assumes we're locked (so term is actually r.currentTerm)
func (r *Raft) toLeader(term int) {
	r.state = Leader

	// NOTE: we'll soon have more to do here

	go r.sendHeartbeats(term)
}

// assumes we're locked
func (r *Raft) toFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = nullVotedFor
}

// sendAppendEntries(), sendHeartbeats(), requestVote(), requestVotes()
// are only relevant for a given term (term). If the current
// term (r.currentTerm) is different, they can safely stop running: this
// is encoded, alongside r.stopped management, in canStop().

func (r *Raft) shouldStop() bool {
	select {
	case <-r.stopped:
		return true
	default:
	}

	return false
}

func (r *Raft) at(term int) bool {
	return r.currentTerm == term
}

func (r *Raft) lAt(term int) bool {
	r.Lock()
	defer r.Unlock()
	return r.at(term)
}

// assumes we're locked
func (r *Raft) shouldStartElection() bool {
	if r.is(Leader) {
		return false
	}

	// we haven't received a heartbeat for too long
	return time.Now().After(r.electionTimeout)
}

// assumes we're locked
func (r *Raft) rstElectionTimeout() int64 {
	d := r.ElectionTimeout[0] + rand.Int63n(r.ElectionTimeout[1]-r.ElectionTimeout[0])
	r.electionTimeout = time.Now().Add(time.Duration(d) * time.Millisecond)
	return d
}

func (r *Raft) callAppendEntries(term, peer int) *AppendEntriesReply {
	var reply AppendEntriesReply
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: r.me,
	}
	if err := r.cpeers[peer].Call("Raft.AppendEntries", &args, &reply); err != nil {
		panic(err)
	}
	return &reply
}

func (r *Raft) sendHeartbeats(term int) {
	for !r.shouldStop() && r.lAt(term) {
		time.Sleep(r.HeartbeatPeriod)
		r.forEachPeer(func(peer int) error {
			go r.callAppendEntries(term, peer)
			return nil
		})
	}
}

func (r *Raft) callRequestVote(term, peer int) *RequestVoteReply {
	var reply RequestVoteReply
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: r.me,
	}

	if err := r.cpeers[peer].Call("Raft.RequestVote", &args, &reply); err != nil {
		panic(err)
	}

	return &reply
}

func (r *Raft) hasMajority(count int) bool {
	return count >= (len(r.Peers)/2)+1
}

type voteCounter struct {
	*sync.Mutex
	count   int
	elected bool
}

func (r *Raft) requestVote(term, peer int, vc *voteCounter) {
	// TODO: timeouts
	reply := r.callRequestVote(term, peer)

	if reply.VoteGranted {
		vc.Lock()
		vc.count++
		if !vc.elected && r.hasMajority(vc.count) {
			r.Lock()
			if r.at(term) && r.is(Candidate) {
				vc.elected = true
				r.toLeader(term)
			}
			r.Unlock()
		}
		vc.Unlock()

		return
	}

	if !reply.VoteGranted {
		r.Lock()
		// if we're still really candidating for that term,
		// and someone is ahead of us, revert to follower
		if r.at(term) && r.is(Candidate) && reply.Term > term {
			r.toFollower(reply.Term)
		}
		r.Unlock()

		return
	}

	panic("unreachable")
}

func (r *Raft) requestVotes(term int) {
	var wg sync.WaitGroup

	// start from 1, as we vote for ourselves
	vc := voteCounter{&sync.Mutex{}, 1, false}

	r.forEachPeer(func(peer int) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.requestVote(term, peer, &vc)
		}()
		return nil
	})

	wg.Wait()
}

func (r *Raft) startElection() {
	r.rstElectionTimeout()
	r.state = Candidate
	r.votedFor = r.me
	r.currentTerm++

	go r.requestVotes(r.currentTerm)

	r.Unlock()
}

// assumes we're locked
func (r *Raft) toCandidate() {
	r.currentTerm++
	r.requestVotes(r.currentTerm)
}

func (r *Raft) runElectionTimer() {
	for !r.shouldStop() {
		// TODO: config parameter
		time.Sleep(20 * time.Millisecond)

		r.Lock()

		if r.shouldStartElection() {
			// will r.Unlock()
			r.startElection()
		} else {
			r.Unlock()
		}
	}
}
