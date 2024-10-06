/*
 * Testing utilities.
 */
package main

import (
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
		t.Errorf(err.Error())
	}

	r := rs[0]

	for n := 0; n < 50; n++ {
		d := r.rstElectionTimeout()

		if d < r.ElectionTimeout[0] || d >= r.ElectionTimeout[1] {
			t.Errorf("Election timeout out of bounds: %d ∉ [%d, %d[",
				d, r.ElectionTimeout[0], r.ElectionTimeout[1])
		}
	}
}

func TestRequestVote(t *testing.T) {
	rs, _, err := NewRafts(&Config{
		Peers:           []string{":6767", ":6868"},
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		t.Errorf(err.Error())
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
