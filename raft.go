package main

// TODO: use a prefix to tag functions assuming the Raft
// to be locked (we currently use l to indicate variants
// which locks the Raft, which we may want to change as well).
//
// NOTE: functions prefixed by run* are expected to ran in a goroutine.
//
// NOTE: all our indexes start from 0 (matchIndex, nextIndex, etc.)
//
// NOTE: in logging:
//	- "term" is the current term
//	- "lterm" is the local term (for long-running goroutines spawn
//	only for a specific term and expected to shutdown once said term ends)
//	- "aterm" is the argument term, for RPCs.

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
	ApplyTick       time.Duration // try applying committed entries periodically
	RPCTimeout      time.Duration // RPC can't last more than
	ElectionTimeout [2]int64      // election timeout choosen between those (ms)

	Testing bool // internal: enable specific code for tests
}

// time.ParseDuration()
//
// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism

type LogEntry struct {
	Term  int
	// Needed when sending log entries to other peers.
	Index int
	Cmd   any
}

func (e LogEntry) String() string {
	return fmt.Sprintf("[term:%d, index:%d, cmd:'%s']", e.Term, e.Index, e.Cmd)
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
const shutdownTerm = -1

var lgr *slog.Logger

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
	log         []LogEntry // current log
	commitIndex int         // highest log entry known to be committed
	lastApplied int         // highest log entry applied to the state machine

	// Raft state: replication, leader-specific
	nextIndex  []int // for each peer, index of the next log entry to send to
	matchIndex []int // for each peer, highest log entry known to be replicated

	apply chan<- any

	// Channel to gracefully stop all long running goroutines:
	// close to terminate everyone.
	stopped chan struct{} // closed when stopped
}

func init() {
	var programLevel = new(slog.LevelVar)
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	lgr = slog.New(h)
	slog.SetDefault(lgr)
	programLevel.Set(slog.LevelDebug)
}

func (r *Raft) Debug(lgr *slog.Logger, msg string, args ...any) {
	xargs := []any{"me", r.me, "port", r.Peers[r.me], "term", r.currentTerm,
		"state", r.state.String()}

	xargs = append(xargs, args...)

	lgr.Debug(msg, xargs...)
}

func (r *Raft) lDebug(lgr *slog.Logger, msg string, args ...any) {
	r.Lock()
	defer r.Unlock()

	r.Debug(lgr, msg, args...)
}

func (r *Raft) lGetTerm() int {
	r.Lock()
	defer r.Unlock()

	return r.currentTerm
}

// NOTE: The start channel is because we sometimes don't want
// to really start the raft, e.g. while testing individual RPC
// requests. The ready channel is closed once all peers are connected
// to each others during tests (TODO: we may be able to get rid of
// it now)
//
// The apply channel is where we send committed commands.
func NewRaft(c *Config, me int, setup,
	start <-chan struct{}, ready chan<- error, apply chan<- any) *Raft {
	var r Raft

	r.Mutex = &sync.Mutex{}
	r.Config = c

	r.cpeers = make([]*rpc.Client, len(c.Peers))

	r.me = me
	r.state = Follower
	r.currentTerm = 0
	r.votedFor = nullVotedFor

	r.log = make([]LogEntry, 0)
	r.commitIndex = -1
	r.lastApplied = -1

	r.nextIndex = make([]int, len(c.Peers))
	r.matchIndex = make([]int, len(c.Peers))

	r.apply = apply
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

		// Start running the timers
		<-start

		go r.runElectionTimer()
		// go r.runApplyTimer()
	}()

	r.lDebug(lgr, "NewRaft")

	return &r
}

// stop long-running goroutines; guard against double stop()/kill()
func (r *Raft) stop() {
	r.lDebug(lgr, "stop")

	select {
	case <-r.stopped:
	default:
		close(r.stopped)
	}
}

// stopAndDisconnect()
func (r *Raft) kill() error {
	r.Lock()
	r.Debug(lgr, "kill")
	r.state = Down
	r.Unlock()

	r.stop()

	// stop RPC server
	return r.disconnect()
}

func (r *Raft) unkill() error {
	r.Lock()
	r.Debug(lgr, "unkill")
	r.stopped = make(chan struct{})
	r.Unlock()

	// restart RPC server
	if err := r.connect(); err != nil {
		return err
	}

	go r.runElectionTimer()
	// go r.runApplyTimer()

	return nil
}

// tear down the RPC server
func (r *Raft) disconnect() error {
	r.Lock()
	defer r.Unlock()

	r.Debug(lgr, "disconnect")

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
	r.Lock()
	defer r.Unlock()

	r.Debug(lgr, "connect")

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
					r.Debug(lgr, "rudeAccept.Close: "+err.Error())
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
			r.lDebug(lgr, "rudeAccept.Accept: "+err.Error())
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
	r.lDebug(lgr, "connectPeers")

	return r.forEachPeer(r.lConnectPeer)
}

// assumes we're locked
func (r *Raft) is(state State) bool {
	return r.state == state
}

// assumes we're locked (so term is actually r.currentTerm)
func (r *Raft) toLeader(term int) {
	r.state = Leader

	r.Debug(lgr, "toLeader", "lterm", term)

	r.initIndexes()

	go r.runSendHeartbeats(term)
}

// on the leader, (re-)initialize r.initIndex and r.matchIndex
// assumes we're locked.
func (r *Raft) initIndexes() {
	r.forEachPeer(func(peer int) error {
		// assume r.log is empty: then the next log entry
		// we'll one day have to send must be zero (nextIndex),
		// and there couldn't be any entry replicated entry
		// on that host (matchIndex)
		r.nextIndex[peer] = len(r.log)
		r.matchIndex[peer] = -1
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

// assumes we're locked. returns a *copy* of a slice
// of r.log.
func (r *Raft) getEntriesFor(peer int) []LogEntry {
	// This should never happen (well, perhaps it'll happen at some
	// point when we get out of sync? for now at least, consider it
	// a fatal error)
	if len(r.log) < r.nextIndex[peer] {
		panic("assert")
	}

	// all entries are replicated iff len(r.log) == r.nextIndex[peer];
	if len(r.log) == r.nextIndex[peer] {
		return []LogEntry{}
	}

	// so there's something to send iff len(r.log) > r.nextIndex[peer].
	i := r.nextIndex[peer]
	if len(r.log) > i {
		n := len(r.log)-i
		xs := make([]LogEntry, n)
		if copy(xs, r.log[i:]) != n {
			panic("TODO")
		}
		return xs
	}

	panic("unreachable")
}

// returns true iff a request was successfully sent and processed
// to/by the remote peer.
func (r *Raft) sendEntriesTo(term, peer int) bool {
	r.Lock()
	defer r.Unlock()

	entries := r.getEntriesFor(peer)

	// messy
	r.Unlock()
	reply, err := r.callAppendEntries(term, peer, entries)
	r.Lock()

	// peer unreacheable atm (most likely)
	if err != nil {
		r.Debug(lgr, "sendEntries: "+err.Error(),
			"to", peer, "rport", r.Peers[peer], "lterm", term)
		return false
	}

	// Don't mess up the state if we've shifted to another
	// term already
	//
	// TODO/XXX: subtle and untested
	if !r.at(term) {
		return false
	}

	if reply.Success {
		r.nextIndex[peer] = len(r.log)
		r.matchIndex[peer] = len(r.log) - 1
		return true
	}

	if !reply.Success {
		// peer is in the process of shuting itself down.
		if reply.Term == shutdownTerm {
			return false
		}

		if reply.Term > term {
			r.toFollower(reply.Term)
			return false
		}

		// This shouldn't happen. Not really a fatal issue,
		// but it means something is poorly wired somewhere.
		if reply.Term < term {
			panic("assert")
		}

		if reply.Term == term {
			r.nextIndex[peer]--

			// Again, something is poorly wired somewhere
			if r.nextIndex[peer] < 0 {
				panic("assert")
			}

			// We'll naturally retry next time around.
			return false
		}
	}

	panic("unreachable")
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
				r.lDebug(lgr, "runSendHeartbeats",
					"to", peer, "rport", r.Peers[peer])
				r.sendEntriesTo(term, peer)
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
		r.lDebug(lgr, "requestVote: "+err.Error(), "to", peer,
			"rport", r.Peers[peer], "lterm", term)

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
		r.Lock()
		r.toLeader(term)
		r.Unlock()
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

	r.Debug(lgr, "startElection")

	go r.requestVotes(r.currentTerm)
}

// assumes we're locked
func (r *Raft) maybeStartElection() bool {
	if r.shouldStartElection() {
		r.startElection()
		return true
	}
	return false
}

func (r *Raft) runElectionTimer() {
	r.lDebug(lgr, "runElectionTimer")

	for !r.shouldStop() {
		r.Lock()
		r.maybeStartElection()
		r.Unlock()

		time.Sleep(r.ElectionTick)
	}
}

// assumes we're locked
func (r *Raft) maybeApply() bool {
	if r.commitIndex > r.lastApplied {
		r.lastApplied++
		r.apply <- r.log[r.lastApplied]
		return true
	}
	return false
}

func (r *Raft) runApplyTimer() {
	r.lDebug(lgr, "runApplyTimer")

	for !r.shouldStop() {
		r.Lock()
		r.maybeApply()
		r.Unlock()

		time.Sleep(r.ApplyTick)
	}
}

// assumes we're locked (!) (for now, only used in tests,
// when printing multiple Rafts at once: we want the state
// of the group)
func (r *Raft) String() string {
	return fmt.Sprintf("[peer:%d, state:%s, term:%d]", r.me, r.state, r.currentTerm)
}

// Entry point to start storing data in a Raft cluster. The
// data is encoded as a command for a state machine.
func (r *Raft) AddCmd(cmd any) bool {
	r.Lock()
	defer r.Unlock()

	if !r.is(Leader) {
		return false
	}

	r.log = append(r.log, LogEntry{r.currentTerm, len(r.log), cmd})
	return true
}
