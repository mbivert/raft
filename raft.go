package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"math/rand"
)

// parsed from json (
type Config struct {
	Peers           []string      // ip:port
	HeartbeatTick   time.Duration // sending heartbeats periodically
	ElectionTick    time.Duration // check for election timeout periodically
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

func init() {
	var programLevel = new(slog.LevelVar)
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	slog.SetDefault(slog.New(h))
	programLevel.Set(slog.LevelDebug)
}

// NOTE: The start channel is because we sometimes don't want
// to really start the raft, e.g. while testing individual RPC
// requests.
func NewRaft(c *Config, me int, setup, start <-chan struct{}, ready chan<- error) *Raft {
	var r Raft

	r.Mutex = &sync.Mutex{}
	r.Config = c

	r.cpeers = make([]*rpc.Client, len(c.Peers))

	r.me = me
	r.state = Follower
	r.currentTerm = 0
	r.votedFor = nullVotedFor

	r.rstElectionTimeout()

	// NOTE/TODO: this is useful for current early tests, but
	// we'll want the code to be smarter (e.g. tolerate a missing
	// peer, in particular when setting up the cluster).
	go func() {
		// wait for start signal: we don't want to start
		// connecting with everyone until every peer has
		// been launched already.
		<-setup

		// connect with everyone
		if err := r.connectPeers(); err != nil {
			ready <- err
			return
		}

		// wait for everyone to be connected to everyone
		ready <- nil

		// Start running the timer
		<-start

		r.runElectionTimer()
	}()

	r.stopped = make(chan struct{})

	slog.Debug("NewRaft", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	return &r
}

// tear down the rPC server
func (r *Raft) disconnect() error {
	slog.Debug("disconnect", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	// a Raft can be created without a working listener.
	// when building a Rafts, we'll try to blindly tear
	// down everyone, including half-baked Rafts.
	if r.listener != nil {
		return r.listener.Close()
	}
	return nil
}

// setup the RPC server
func (r *Raft) connect() (err error) {
	slog.Debug("connect", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

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

// call f on every peer but ourselves
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

// try to dial every peer
func (r *Raft) connectPeers() error {
	slog.Debug("connectPeers", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	return r.forEachPeer(func(p int) (err error) {
		if r.cpeers[p], err = rpc.Dial("tcp", r.Peers[p]); err != nil {
			return err
		}
		return nil
	})
}

// assumes we're locked
func (r *Raft) is(state State) bool {
	return r.state == state
}

// assumes we're locked (so term is actually r.currentTerm)
func (r *Raft) toLeader(term int) {
	r.state = Leader

	slog.Debug("toLeader", "me", r.me, "port", r.Peers[r.me],
		"term", term)

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
// term (r.currentTerm) is different, they can safely stop running

func (r *Raft) shouldStop() bool {
	select {
	case <-r.stopped:
		return true
	default:
	}

	return false
}

// assumes we're locked
func (r *Raft) at(term int) bool {
	return r.currentTerm == term
}

// same as at(), but lock.
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

	// we haven't received a heartbeat recently enough
	return time.Now().After(r.electionTimeout)
}

// assumes we're locked
func (r *Raft) rstElectionTimeout() int64 {
	d := r.ElectionTimeout[0] + rand.Int63n(r.ElectionTimeout[1]-r.ElectionTimeout[0])
	r.electionTimeout = time.Now().Add(time.Duration(d) * time.Millisecond)

	return d
}

func (r *Raft) sendHeartbeats(term int) {
	// I guess it's better to have a waitgroup collecting pending
	// goroutines?
	var wg sync.WaitGroup

	// NOTE: it might be slightly better to check for
	// relevancy within the innermost goroutines, but
	// the impact doesn't seem to be too perceptible so far.
	for !r.shouldStop() && r.lAt(term) {
		r.forEachPeer(func(peer int) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Debug("sendHeartbeats", "from", r.me,
					"to", peer, "port", r.Peers[r.me], "term", term)

				r.callAppendEntries(term, peer)
			}()
			return nil
		})

		// NOTE: because the election timeout isn't reseted on
		// voting, it's (noticeably) better not to delay the first row
		// of heartbeats by sleeping first. Otherwise, peers are
		// more likely to restart an election, and the leader election
		// is more unstable.
		//
		// That kind of issue didn't arised in an earlier, time.Ticker
		// based implementation, but this coarser time.Sleep()-based
		// version remains simpler overall.
		time.Sleep(r.HeartbeatTick)
	}

	wg.Wait()
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
	// TODO: RPCs timeouts
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

	// ¯\_(ツ)_/¯
	if r.hasMajority(vc.count) {
		r.toLeader(term)
		return
	}

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

// assumes we're locked
func (r *Raft) startElection() {
	r.rstElectionTimeout()
	r.state = Candidate
	r.votedFor = r.me
	r.currentTerm++

	slog.Debug("startElection", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	go r.requestVotes(r.currentTerm)
}

func (r *Raft) runElectionTimer() {
	slog.Debug("runElectionTimer", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	for !r.shouldStop() {
		r.Lock()

		if r.shouldStartElection() {
			r.startElection()
		}

		r.Unlock()


		time.Sleep(r.ElectionTick)
	}
}

// assumes we're locked (!) (for now, only used in tests,
// when printing multiple Rafts at once: we want the state
// of the group)
func (r *Raft) String() string {
	return fmt.Sprintf("peer:%d/state:%s/term:%d", r.me, r.state, r.currentTerm)
}
