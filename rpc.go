/*
 * RPC handlers (AppendEntries, RequestVote)
 */
package main

import "log/slog"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
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

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.Lock()
	defer r.Unlock()

	slog.Debug("AppendEntries", "from", args.LeaderId, "me", r.me,
		"port", r.Peers[r.me], "term", r.currentTerm, "rterm", args.Term)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false

		return nil
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)

		r.rstElectionTimeout()

		reply.Term = r.currentTerm
		reply.Success = true

		return nil
	}

	if args.Term == r.currentTerm {
		r.rstElectionTimeout()

		reply.Term = r.currentTerm
		reply.Success = true

		return nil
	}

	panic("unreachable")
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.Lock()
	defer r.Unlock()

	slog.Debug("RequestVote", "from", args.CandidateId, "me", r.me,
		"port", r.Peers[r.me], "term", r.currentTerm, "rterm", args.Term)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false

		return nil
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)
		r.votedFor = args.CandidateId

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
