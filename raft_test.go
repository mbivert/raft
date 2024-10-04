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
	c := Config{
		Peers:           []string{":1010"},
		ElectionTimeout: [2]int64{150, 300},
	}

	r := NewRaft(&c, 0, make(chan struct{}), make(chan error))
	for n := 0; n < 50; n++ {
		d := r.rstElectionTimeout()
		if d < c.ElectionTimeout[0] || d >= c.ElectionTimeout[1] {
			t.Errorf("Election timeout out of bounds: %d ∉ [%d, %d[",
				d, c.ElectionTimeout[0], c.ElectionTimeout[1])
		}
	}
}

func TestRequestVote(t *testing.T) {
	rs, err := mkNetwork(&Config{
		Peers:           []string{":7070", ":7171"},
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

	close(rs[0].stopped)
	close(rs[1].stopped)
}
