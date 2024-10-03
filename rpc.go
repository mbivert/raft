/*
 * RPC handlers (AppendEntries, RequestVote)
 */
package main

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

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false

		return nil
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)

		reply.Term = r.currentTerm
		reply.Success = true

		return nil
	}

	if args.Term == r.currentTerm {
		panic("TODO")
	}

	panic("unreachable")
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.Lock()
	defer r.Unlock()

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
