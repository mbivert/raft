package main

import (
	"sync"
	"time"

	"math/rand"
)

// parsed from json (
type Config struct {
	Peers           []string         // ip:port
	HeartbeatPeriod time.Duration    // sending heartbeats periodically
	RPCTimeout      time.Duration    // RPC can't last more than
	ElectionTimeout [2]int64         // election timeout choosen between those (ms)
}

// time.ParseDuration()
// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism

type LogEntry struct {
	Data any
	Term int
}

const nullVotedFor = -1

type Raft struct {
	*sync.Mutex
	*Config

	me              int       // Raft.Confir.Peers[] id; command line argument
	state           State     // current state
	currentTerm     int       // current term
	votedFor        int       // who we votedFor (eventually nullVotedFor)
	electionTimeout time.Time // last time we received a heartbeat

	stopped chan struct{} // closed when stopped

	log []*LogEntry
}

// NOTE: The start channel is because we sometimes don't want
// to really start the raft, e.g. while testing individual RPC
// requests.
func NewRaft(c *Config, me int, start <-chan struct{}) *Raft {
	var r Raft

	r.Mutex = &sync.Mutex{}
	r.Config = c

	r.me = me
	r.state = Follower
	r.currentTerm = 0
	r.votedFor = nullVotedFor

	r.stopped = make(chan struct{})

	go func() {
		<-start
		r.runElectionTimer()
	}()

	return &r
}

type State int

//go:generate go run golang.org/x/tools/cmd/stringer -type State -linecomment raft.go
const (
	Down      State = iota // down
	Follower               // follower
	Candidate              // candidate
	Leader                 // leader
)

func (r *Raft) forEachPeer(f func(int)) {
	for peer := range r.Peers {
		if peer != r.me {
			f(peer)
		}
	}
}

func (r *Raft) is(state State) bool {
	return r.state == state
}

// toLeader(), toCandidate() and toFollower() encode
// (raft) state changes

// assumes we're locked
func (r *Raft) toLeader() {
	if r.state != Candidate {
		panic("assert")
	}
}

// assumes we're locked
func (r *Raft) toFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1
}

// sendHeartbeat(), sendHeartbeats(), requestVote(), requestVotes()
// are only relevant for a given term (startTerm). If the current
// term (r.term) is different, they can safely stop running: this
// is encoded, alongside r.stopped management, in canStop().

func (r *Raft) shouldStop() bool {
	select {
	case <-r.stopped:
		return true
	default:
	}

	return false
}

func (r *Raft) shouldStopTerm(startTerm int) bool {
	if r.shouldStop() {
		return true
	}

	r.Lock()
	defer r.Unlock()
	return r.currentTerm != startTerm
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
	d := r.ElectionTimeout[0] + rand.Int63n(r.ElectionTimeout[1])

	r.electionTimeout = time.Now().Add(time.Duration(d) * time.Millisecond)

	// for tests eventually?
	return d
}

func (r *Raft) sendHeartbeat(startTerm int, peer int) {
	var args RequestVoteArgs

	args.Term = startTerm
	args.CandidateId = r.me
}

func (r *Raft) sendHeartbeats(startTerm int) {
	for !r.shouldStopTerm(startTerm) {
		time.Sleep(r.HeartbeatPeriod)
		r.forEachPeer(func(peer int) { go r.sendHeartBeat(startTerm, peer) })
	}
}

func (r *Raft) requestVote(term int, peer int) {
}

func (r *Raft) requestVotes(term int) {
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
		r.Lock()

		if r.shouldStartElection() {
			// will r.Unlock()
			r.startElection()
		} else {
			r.Unlock()
		}
	}
}

func (r *Raft) sendHeartBeat(term int, peer int) {
}
