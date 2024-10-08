/*
 * Testing utilities.
 */
package main

import (
	"reflect"
	"sync"
	"testing"

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

	e := &LogEntry{rs[0].currentTerm, cmd}
	if len(rs[0].log) != 1 {
		t.Errorf("Log entry should contain exactly one element, has %d", len(rs[0].log))
	} else if !reflect.DeepEqual(rs[0].log[0], e) {
		t.Errorf("Invalid log entry: %s vs %s", rs[0].log[0], e)
	}

	rs.kill()
}
