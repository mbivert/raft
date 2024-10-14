/*
 * RPC handlers (AppendEntries, RequestVote)
 */
package main

import (
	"errors"
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []*LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int  // « currentTerm, for candidate to update itself »
	VoteGranted bool // « true means candidate received vote »
}

// assumes we're locked
func (r *Raft) doAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	entries := args.Entries
	for _, entry := range entries {
		if entry.Index == len(r.log) {
			r.log = append(r.log, entry)

			continue
		}

		panic("TODO")
	}
	return nil
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.Lock()
	defer r.Unlock()

	r.Debug(lgr, "AppendEntries", "leader", args.LeaderId, "aterm", args.Term)

	// In the process of being shutdown
	if r.is(Down) {
		reply.Term = shutdownTerm
		reply.Success = false

		return nil
	}

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false

		return nil
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)

		r.rstElectionTimeout()

		// assume success, for now
		reply.Term = r.currentTerm
		reply.Success = true

		return r.doAppendEntries(args, reply)
	}

	if args.Term == r.currentTerm {
		switch r.state {
		case Leader:
			panic("two leaders for the same term?!")
		case Candidate:
			r.toFollower(args.Term)
		case Follower:
		}

		r.rstElectionTimeout()

		// assume success, for now
		reply.Term = r.currentTerm
		reply.Success = true

		return r.doAppendEntries(args, reply)
	}

	panic("unreachable")
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.Lock()
	defer r.Unlock()

	r.Debug(lgr, "RequestVote", "candidate", args.CandidateId, "aterm", args.Term)

	// In the process of being shutdown
	if r.is(Down) {
		reply.Term = r.currentTerm
		reply.VoteGranted = false

		return nil
	}

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false

		return nil
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)
		r.votedFor = args.CandidateId
		// r.rstElectionTimeout()

		reply.Term = r.currentTerm
		reply.VoteGranted = true

		return nil
	}

	if args.Term == r.currentTerm {
		if r.is(Candidate) {
			reply.Term = r.currentTerm
			reply.VoteGranted = false

			return nil
		}

		if r.is(Leader) {
			reply.Term = r.currentTerm
			reply.VoteGranted = false

			return nil
		}

		if r.is(Follower) {
			if r.votedFor == nullVotedFor || r.votedFor == args.CandidateId {
				r.votedFor = args.CandidateId
				// r.rstElectionTimeout()

				reply.Term = r.currentTerm
				reply.VoteGranted = true

				return nil
			}

			if r.votedFor != nullVotedFor {
				reply.Term = r.currentTerm
				reply.VoteGranted = false

				return nil
			}
		}
	}

	panic("unreachable")
}

// wrap a RPC call with a timeout; detect disconnected peer and try
// to reconnect, eventually.
func (r *Raft) tryCall(fn string, args any, reply any, peer int) error {
	c := make(chan error, 1)

	r.Lock()
	cl := r.cpeers[peer]
	r.Unlock()

	go func() {
		if cl != nil {
			c <- cl.Call(fn, args, reply)
		} else {
			c <- fmt.Errorf("Peer not connected")
		}
	}()

	select {
	case err := <-c:
		if err == nil {
			return nil
		}

		// such errors are abnormal, and indicate network
		// failures & the like, so try to reconnect
		return errors.Join(err, r.reconnectPeer(peer))

	case <-time.After(r.RPCTimeout):
		return errors.Join(
			fmt.Errorf("peer %d timeout (%s)", peer, fn),
			r.reconnectPeer(peer),
		)
	}

	panic("unreachable")
}

func (r *Raft) callAppendEntries(term, peer int, entries []*LogEntry) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: r.me,
		Entries:  entries,
	}

	return &reply, r.tryCall("Raft.AppendEntries", &args, &reply, peer)
}

func (r *Raft) callRequestVote(term, peer int) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: r.me,
	}

	return &reply, r.tryCall("Raft.RequestVote", &args, &reply, peer)
}
