/*
 * Testing individual RPCs.
 */
package main

import (
	"testing"
	"time"

	"github.com/mbivert/ftests"
)

// wrappers to ease tests (in particular, we want to check
// Raft objects's states is properly updated)
//
// Last return value indicates whether r.electionTimeout has changed;
// it's returned as it was before the call in the returned Raft object
// to ease testing.
func tAppendEntries(r *Raft, args *AppendEntriesArgs) (*AppendEntriesReply, *Raft, bool) {
	var reply AppendEntriesReply
	a := r.electionTimeout
	r.AppendEntries(args, &reply)
	b := r.electionTimeout
	r.electionTimeout = a
	return &reply, r, a != b
}

func tRequestVote(r *Raft, args *RequestVoteArgs) (*RequestVoteReply, *Raft) {
	var reply RequestVoteReply
	r.RequestVote(args, &reply)
	return &reply, r
}

// heartbeat <=> no log entries
func TestAppendEntriesHeartbeat(t *testing.T) {
	r := NewRaft(&Config{
		Peers:           []string{":0"},
		ElectionTimeout: [2]int64{150, 300},
	}, 0, make(chan struct{}), make(chan struct{}), make(chan error))
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
				false,
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
					Mutex:    r.Mutex,
					Config:   r.Config,
					me:       r.me,
					cpeers:   r.cpeers,
					listener: r.listener,
					// Candidate → Follower
					state: Follower,
					// voted for the requester
					// term updated accordingly
					currentTerm: r.currentTerm + 2,
					// reset
					votedFor:        nullVotedFor,
					stopped:         r.stopped,
					log:             r.log,
					electionTimeout: r.electionTimeout,
				},
				true,
			},
		},
	})
}

func TestRequestVoteFromLowerTerm(t *testing.T) {
	r := NewRaft(&Config{
		Peers:           []string{":0"},
		ElectionTimeout: [2]int64{150, 300},
	}, 0, make(chan struct{}), make(chan struct{}), make(chan error))

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
	r := NewRaft(&Config{
		Peers:           []string{":0"},
		ElectionTimeout: [2]int64{150, 300},
	}, 0, make(chan struct{}), make(chan struct{}), make(chan error))

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
						Mutex:    r.Mutex,
						Config:   r.Config,
						cpeers:   r.cpeers,
						listener: r.listener,
						me:       r.me,
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
	r := NewRaft(&Config{
		Peers:           []string{":0"},
		ElectionTimeout: [2]int64{150, 300},
	}, 0, make(chan struct{}), make(chan struct{}), make(chan error))

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
				cpeers:      r.cpeers,
				listener:    r.listener,
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

func TestAppendEntries(t *testing.T) {

}

// setup two peers, connect them, and perform
// a genuine RPC call
func TestAppendHeartBeatRPC(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":6767", ":6868"},
		ElectionTick:    20 * time.Millisecond,
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	r0, r1 := rs[0], rs[1]

	if r0.cpeers[0] != nil {
		t.Errorf("peer0 can't talk to itself")
	}

	if r0.cpeers[1] == nil {
		t.Errorf("peer0 can't talk to peer1")
	}

	if r1.cpeers[0] == nil {
		t.Errorf("peer1 can't talk to peer0")
	}

	if r1.cpeers[1] != nil {
		t.Errorf("peer1 can't talk to itself")
	}

	r1t := r1.currentTerm
	ftests.Run(t, []ftests.Test{
		{
			"sending (0→1) from lower term",
			r0.callAppendEntries,
			[]any{
				r1.currentTerm - 1,
				r1.me,
			},
			[]any{
				&AppendEntriesReply{
					Term:    r1.currentTerm,
					Success: false,
				},
			},
		},
		{
			"sending (0→1) from higher term",
			r0.callAppendEntries,
			[]any{
				r1.currentTerm + 1,
				r1.me,
			},
			[]any{
				&AppendEntriesReply{
					Term:    r1t + 1,
					Success: true,
				},
			},
		},
	})

	if r1.currentTerm != r1t+1 {
		t.Errorf("r1's currentTerm not updated")
	}

	rs.kill()
}
