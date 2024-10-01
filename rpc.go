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

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.Lock()
	defer r.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)
		reply.Term = r.currentTerm
		reply.Success = true
		return
	}

	if args.Term == r.currentTerm {
	}
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	r.Lock()
	defer r.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > r.currentTerm {
		r.toFollower(args.Term)
		r.votedFor = args.CandidateId

		reply.Term = r.currentTerm
		reply.VoteGranted = true
		return
	}

	if args.Term == r.currentTerm {
	}
}
