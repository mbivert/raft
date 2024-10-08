package main

// TODO: use a prefix to tag functions assuming the Raft
// to be locked (we currently use l to indicate variants
// which locks the Raft, which we may want to change as well).

// NOTE: functions prefixed by run* are expected to ran in a goroutine.

import (
	"fmt"
	"log/slog"
	"net"
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
	SendEntriesTick time.Duration // try sending entries periodically
	RPCTimeout      time.Duration // RPC can't last more than
	ElectionTimeout [2]int64      // election timeout choosen between those (ms)

	Testing bool // internal: enable specific code for tests
}

// time.ParseDuration()
//
// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism

type LogEntry struct {
	Term int
	Cmd  any
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("[term:%d, cmd:'%s']", e.Term, e.Cmd)
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
	cpeers   []*rpc.Client // connected/client for r.Peers
	server   *rpc.Server   // our RPC server (for others to query us)
	listener net.Listener  // listener associatod to the server

	// Raft state: general
	me          int   // Raft.Confir.Peers[] id; command line argument
	state       State // current state
	currentTerm int   // current term

	// Raft state: election
	votedFor        int       // who we votedFor (eventually nullVotedFor)
	electionTimeout time.Time // when to start a new election (!Leader)

	// Raft state: replication
	log         []*LogEntry // current log
	commitIndex int         // highest log entry known to be committed
	lastApplied int         // highest log entry applied to the state machine

	// Raft state: replication, leader-specific
	nextIndex  []int // for each peer, index of the next log entry to send to
	matchIndex []int // for each peer, highest log entry known to be replicated

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

	r.log = make([]*LogEntry, 0)
	r.commitIndex = 0
	r.lastApplied = 0

	r.nextIndex = make([]int, len(c.Peers))
	r.matchIndex = make([]int, len(c.Peers))

	r.stopped = make(chan struct{})

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
		// TODO/XXX: we should now be able to remove this,
		// as our Rafts are more resilient to unconnected
		// peers. A few tests wouldn't hurt.
		ready <- nil

		// Start running the timer
		<-start

		r.runElectionTimer()
	}()

	slog.Debug("NewRaft", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	return &r
}

// stop long-running goroutines; Guards against double stop()/kill()
func (r *Raft) stop() {
	slog.Debug("stop", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	select {
	case <-r.stopped:
	default:
		close(r.stopped)
	}
}

// stopAndDisconnect()
func (r *Raft) kill() error {
	// TODO: .Lock() for all our slog.Debug() :-/
	slog.Debug("kill", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	r.Lock()
	r.state = Down
	r.Unlock()

	r.stop()

	// stop RPC server
	return r.disconnect()
}

func (r *Raft) unkill() error {
	slog.Debug("unkill", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)
	r.Lock()
	r.stopped = make(chan struct{})
	r.Unlock()

	// restart RPC server
	if err := r.connect(); err != nil {
		return err
	}

	go r.runElectionTimer()
	return nil
}

// tear down the RPC server
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

	r.Lock()
	defer r.Unlock()

	r.server = rpc.NewServer()

	if err = r.server.Register(r); err != nil {
		return err
	}

	if r.listener, err = net.Listen("tcp", r.Peers[r.me]); err != nil {
		return err
	}

	go r.accept()

	return nil
}

func (r *Raft) accept() {
	if r.Testing {
		r.rudeAccept()
	} else {
		r.niceAccept()
	}
}

// For production code
func (r *Raft) niceAccept() {
	r.server.Accept(r.listener)
}

// A variant of niceAccept(), where we handle connections more finely.
// It allows, in tests, to rudely disconnect a server, and check whether
// other peers handle that well.
func (r *Raft) rudeAccept() {
	var wg sync.WaitGroup

	var mu sync.Mutex
	var cs map[net.Conn]bool = make(map[net.Conn]bool)

	// NOTE: this is to avoid a race with connect(), as
	// r.server might be updated under our nose.
	r.Lock()
	serv := r.server
	r.Unlock()

	for {
		r.Lock()
		select {
		case <-r.stopped:
			r.Unlock()
			mu.Lock()
			for conn := range cs {
				if err := conn.Close(); err != nil {
					slog.Debug("rudeAccept.Close: "+err.Error(),
						"me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)
				}
			}
			mu.Unlock()
			wg.Wait()
			return
		default:
			r.Unlock()
		}

		conn, err := r.listener.Accept()
		if err != nil {
			slog.Debug("rudeAccept.Accept: "+err.Error(),
				"me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)
		} else {
			mu.Lock()
			cs[conn] = true
			mu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				serv.ServeConn(conn)
				mu.Lock()
				delete(cs, conn)
				mu.Unlock()
			}()
		}
	}
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

func (r *Raft) lConnectPeer(peer int) (err error) {
	r.Lock()
	defer r.Unlock()
	return r.connectPeer(peer)
}

func (r *Raft) connectPeer(peer int) (err error) {
	r.cpeers[peer], err = rpc.Dial("tcp", r.Peers[peer])
	return err
}

func (r *Raft) reconnectPeer(peer int) (err error) {
	r.Lock()
	defer r.Unlock()

	if r.cpeers[peer] != nil {
		r.cpeers[peer].Close()
	}

	return r.connectPeer(peer)
}

// try to dial every peer
func (r *Raft) connectPeers() error {
	slog.Debug("connectPeers", "me", r.me, "port", r.Peers[r.me], "term", r.currentTerm)

	return r.forEachPeer(r.lConnectPeer)
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

	r.initIndexes()

	go r.runSendHeartbeats(term)
	go r.runSendEntries(term)
}

// on the leader, (re-)initialize r.initIndex and r.matchIndex
// assumes we're locked.
func (r *Raft) initIndexes() {
	r.forEachPeer(func(peer int) error {
		r.nextIndex[peer] = len(r.log)
		r.matchIndex[peer] = 0
		return nil
	})
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
	r.Lock()
	defer r.Unlock()

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

func (r *Raft) runSendHeartbeats(term int) {
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
	reply, err := r.callRequestVote(term, peer)

	// Most likely, peer cannot be reached
	if err != nil {
		slog.Debug("requestVote: "+err.Error(), "from", r.me,
			"to", peer, "port", r.Peers[r.me], "term", term)
		return
	}

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

func (r *Raft) runSendEntries(term int) {
	for !r.shouldStop() && r.lAt(term) {
		time.Sleep(r.SendEntriesTick)
	}
}

// assumes we're locked (!) (for now, only used in tests,
// when printing multiple Rafts at once: we want the state
// of the group)
func (r *Raft) String() string {
	return fmt.Sprintf("peer:%d/state:%s/term:%d", r.me, r.state, r.currentTerm)
}

// Entry point to start storing data in a Raft cluster. The
// data is encoded as a command for a state machine.
func (r *Raft) AddCmd(cmd any) bool {
	r.Lock()
	defer r.Unlock()

	if !r.is(Leader) {
		return false
	}

	r.log = append(r.log, &LogEntry{r.currentTerm, cmd})
	return true
}
