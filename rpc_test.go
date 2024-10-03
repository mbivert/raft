/*
 * Test individual RPCs
 */
package main

import (
	"testing"

	"github.com/mbivert/ftests"
)

func tAppendEntries(r *Raft, args *AppendEntriesArgs) (*AppendEntriesReply, *Raft) {
	var reply AppendEntriesReply
	r.AppendEntries(args, &reply)
	return &reply, r
}

func tRequestVote(r *Raft, args *RequestVoteArgs) (*RequestVoteReply, *Raft) {
	var reply RequestVoteReply
	r.RequestVote(args, &reply)
	return &reply, r
}

// heartbeat <=> no log entries
func TestAppendEntriesHeartbeat(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))
	r.currentTerm = 1
	r.votedFor = 42

	ftests.Run(t, []ftests.Test{
		{
			"receiving from lower term",
			tAppendEntries,
			[]any{
				r,
				&AppendEntriesArgs{
					Term:     r.currentTerm - 1,
					LeaderId: -1,
				},
			},
			[]any{
				&AppendEntriesReply{
					Term:    r.currentTerm,
					Success: false,
				},
				r,
			},
		},
		{
			"receiving from greater term",
			tAppendEntries,
			[]any{
				r,
				&AppendEntriesArgs{
					Term:     r.currentTerm + 2,
					LeaderId: -1,
				},
			},
			[]any{
				&AppendEntriesReply{
					Term:    r.currentTerm + 2,
					Success: true,
				},
				&Raft{
					Mutex:  r.Mutex,
					Config: r.Config,
					me:     r.me,
					// Candidate → Follower
					state: Follower,
					// voted for the requester
					// term updated accordingly
					currentTerm: r.currentTerm + 2,
					// reset
					votedFor: nullVotedFor,
					stopped:  r.stopped,
					log:      r.log,
				},
			},
		},
	})
}

func TestAppendEntries(t *testing.T) {

}

func TestRequestVoteFromLowerTerm(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))

	rst := func(state State) {
		r.state = state
		r.me = 0
		r.votedFor = r.me
		r.currentTerm = 1
	}

	for _, state := range []State{Follower, Candidate, Leader} {
		rst(state)
		ftests.Run(t, []ftests.Test{
			{
				"receiving from lower term when " + state.String(),
				tRequestVote,
				[]any{
					r,
					&RequestVoteArgs{
						Term:        r.currentTerm - 1,
						CandidateId: -1,
					},
				},
				[]any{
					&RequestVoteReply{
						Term:        r.currentTerm,
						VoteGranted: false,
					},
					r,
				},
			},
		})
	}
}

func TestRequestVoteFromHigherTerm(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))

	rst := func(state State) {
		r.state = state
		r.me = 0
		r.votedFor = r.me
		r.currentTerm = 1
	}

	for _, state := range []State{Follower, Candidate, Leader} {
		rst(state)
		ftests.Run(t, []ftests.Test{
			{
				"receiving from greater term when " + state.String(),
				tRequestVote,
				[]any{
					r,
					&RequestVoteArgs{
						Term:        r.currentTerm + 2,
						CandidateId: r.me + 3,
					},
				},
				[]any{
					&RequestVoteReply{
						Term:        r.currentTerm + 2,
						VoteGranted: true,
					},
					// remember, the r below is the one at compile-time
					&Raft{
						Mutex:  r.Mutex,
						Config: r.Config,
						me:     r.me,
						// * → Follower
						state: Follower,
						// term updated accordingly
						currentTerm: r.currentTerm + 2,
						// voted for the requester
						votedFor: r.me + 3,
						stopped:  r.stopped,
						log:      r.log,
					},
				},
			},
		})
	}
}

func TestRequestVoteFromEqTerm(t *testing.T) {
	r := NewRaft(nil, 0, make(chan struct{}))

	rst := func(state State, peer int, term int) {
		r.state = state
		r.me = 0
		r.votedFor = peer
		r.currentTerm = term
	}

	// Candidate/Leader won't grant vote
	for _, state := range []State{Candidate, Leader} {
		rst(state, r.me, 1)
		ftests.Run(t, []ftests.Test{{
			"receiving from equal term when " + r.state.String(),
			tRequestVote,
			[]any{
				r,
				&RequestVoteArgs{
					Term:        r.currentTerm,
					CandidateId: r.me + 3,
				},
			},
			[]any{
				&RequestVoteReply{
					Term:        r.currentTerm,
					VoteGranted: false,
				},
				// remember, the r below is the one at compile-time
				r,
			},
		}})
	}

	rst(Follower, nullVotedFor, 1)
	ftests.Run(t, []ftests.Test{{
		"voted for no-one; receiving from greater term when " + r.state.String(),
		tRequestVote,
		[]any{
			r,
			&RequestVoteArgs{
				Term:        r.currentTerm,
				CandidateId: r.me + 3,
			},
		},
		[]any{
			&RequestVoteReply{
				Term:        r.currentTerm,
				VoteGranted: true,
			},
			// remember, the r below is the one at compile-time
			&Raft{
				Mutex:       r.Mutex,
				Config:      r.Config,
				me:          r.me,
				state:       r.state,
				currentTerm: r.currentTerm,
				// voted for the requester
				votedFor: r.me + 3,
				stopped:  r.stopped,
				log:      r.log,
			},
		},
	}})

	rst(Follower, r.me+2, 1)
	ftests.Run(t, []ftests.Test{{
		"voted for someone else; receiving from greater term when " + r.state.String(),
		tRequestVote,
		[]any{
			r,
			&RequestVoteArgs{
				Term:        r.currentTerm,
				CandidateId: r.me + 3,
			},
		},
		[]any{
			&RequestVoteReply{
				Term:        r.currentTerm,
				VoteGranted: false,
			},
			r,
		},
	}})

	rst(Follower, r.me+3, 1)
	ftests.Run(t, []ftests.Test{{
		"voted for that guy already; receiving from greater term when " + r.state.String(),
		tRequestVote,
		[]any{
			r,
			&RequestVoteArgs{
				Term:        r.currentTerm,
				CandidateId: r.me + 3,
			},
		},
		[]any{
			&RequestVoteReply{
				Term:        r.currentTerm,
				VoteGranted: true,
			},
			r,
		},
	}})
}
