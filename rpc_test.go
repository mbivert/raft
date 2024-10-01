/*
 * Test individual RPCs
 */
package main

import (
	"testing"
	"github.com/mbivert/ftests"
)

func tAppendEntries(r *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	var reply AppendEntriesReply
	r.AppendEntries(args, &reply)
	return &reply
}

func tRequestVote(r *Raft, args *RequestVoteArgs) *RequestVoteReply {
	var reply RequestVoteReply
	r.RequestVote(args, &reply)
	return &reply
}

// <=> no log entries are sent
func TestAppendEntriesHeartbeat(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))
	r.currentTerm = 1

	ftests.Run(t, []ftests.Test{{
		"receiving from lower term",
		tAppendEntries,
		[]any{
			r,
			&AppendEntriesArgs{
				Term: 0,
				LeaderId: -1,
			},
		},
		[]any{
			&AppendEntriesReply{
				Term: r.currentTerm,
				Success: false,
			},
		},
	}})
}

func TestAppendEntries(t *testing.T) {

}

func TestRequestVote(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))
	r.currentTerm = 1

	ftests.Run(t, []ftests.Test{{
		"receiving from lower term",
		tRequestVote,
		[]any{
			r,
			&RequestVoteArgs{
				Term: 0,
				CandidateId: -1,
			},
		},
		[]any{
			&RequestVoteReply{
				Term: r.currentTerm,
				VoteGranted: false,
			},
		},
	}})
}
