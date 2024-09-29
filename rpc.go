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
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
}
