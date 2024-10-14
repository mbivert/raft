/*
 * Testing utilities.
 */
package main

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mbivert/ftests"
)

func TestRstElectionTimeout(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":9090"},
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		rs.kill()
		t.Fatalf(err.Error())
	}

	r := rs[0]

	for n := 0; n < 50; n++ {
		d := r.rstElectionTimeout()

		if d < r.ElectionTimeout[0] || d >= r.ElectionTimeout[1] {
			t.Errorf("Election timeout out of bounds: %d ∉ [%d, %d[",
				d, r.ElectionTimeout[0], r.ElectionTimeout[1])
		}
	}

	rs.kill()
}

func TestRequestVote(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":6767", ":6868"},
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		rs.kill()
		t.Fatalf(err.Error())
	}

	rs[0].currentTerm = 1
	rs[0].state = Candidate

	rs[1].currentTerm = 1
	rs[1].state = Follower

	ftests.Run(t, []ftests.Test{
		{
			"sending (0→1) from lower term",
			rs[0].requestVote,
			[]any{
				rs[0].currentTerm,
				rs[1].me,
				&voteCounter{&sync.Mutex{}, 1, false},
			},
			nil,
		},
	})

	rs.kill()
}

func TestAddCmd(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":6767"},
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		rs.kill()
		t.Fatalf(err.Error())
	}

	for _, state := range []State{Follower, Candidate} {
		rs[0].state = state

		ftests.Run(t, []ftests.Test{{
			"can't add commands to " + state.String(),
			rs[0].AddCmd,
			[]any{
				"foo",
			},
			[]any{false},
		}})
		if len(rs[0].log) != 0 {
			t.Errorf("Log should still be empty")
		}
	}

	rs[0].state = Leader

	cmd := "foo"
	ftests.Run(t, []ftests.Test{{
		"can add commands to leader",
		rs[0].AddCmd,
		[]any{
			cmd,
		},
		[]any{true},
	}})

	e := &LogEntry{rs[0].currentTerm, 0, cmd}
	if len(rs[0].log) != 1 {
		t.Errorf("Log entry should contain exactly one element, has %d", len(rs[0].log))
	} else if !reflect.DeepEqual(rs[0].log[0], e) {
		t.Errorf("Invalid log entry: %s vs %s", rs[0].log[0], e)
	}

	rs.kill()
}

func tSendEntries1(r0, r1 *Raft) (bool, []*LogEntry, []int, []int) {
	term := r0.lGetTerm()

	ret := r0.sendEntries1(term, r1.me)

	return ret, r1.log, r0.nextIndex, r0.matchIndex
}

func TestSendEntries1(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":6767", ":6868"},
		ElectionTimeout: [2]int64{150, 300},
		RPCTimeout:      500 * time.Millisecond,
	})
	if err != nil {
		rs.kill()
		t.Fatalf(err.Error())
	}

	r0, r1 := rs[0], rs[1]

	// XXX manual toLeader(): we don't want to send heartbeats
	r0.Lock()
	r0.state = Leader
	r0.currentTerm = 1
	r0.initIndexes()
	r0.Unlock()

	r1.toFollower(1)

	ftests.Run(t, []ftests.Test{{
		"no command to send",
		tSendEntries1,
		[]any{
			r0,
			r1,
		},
		[]any{
			false,
			[]*LogEntry{},
			[]int{0, 0},
			[]int{0,-1},
		},
	}})

	cmd := "first command"
	if !r0.AddCmd(cmd) {
		rs.kill()
		t.Fatalf("Can't add command?!")
	}

	ftests.Run(t, []ftests.Test{{
		"one command correctly sent",
		tSendEntries1,
		[]any{
			r0,
			r1,
		},
		[]any{
			true,
			[]*LogEntry{r0.log[0]},
			[]int{0, 1},
			[]int{0, 0},
		},
	}})

	rs.kill()
}
