// For tests: network of Raft peers.
package main

import (
	"strings"
	"time"
)

type Rafts []*Raft

func (rs Rafts) Lock() {
	for _, r := range rs {
		r.Lock()
	}
}

func (rs Rafts) Unlock() {
	for _, r := range rs {
		r.Unlock()
	}
}

func (rs Rafts) getLeader() int {
	i := nullVotedFor

	rs.Lock()
	defer rs.Unlock()

	for j, r := range rs {
		if r.is(Leader) && i != nullVotedFor {
			panic("multiple leaders")
		}
		if r.is(Leader) {
			i = j
		}
	}

	return i
}

func (rs Rafts) String() string {
	rs.Lock()
	defer rs.Unlock()

	xs := make([]string, len(rs))

	for _, r := range rs {
		xs = append(xs, r.String())
	}

	return strings.Join(xs, "\n")
}

// Create a network of raft peers for tests purposes
func NewRafts(c *Config) (Rafts, chan<- struct{}, error) {
	rafts := make([]*Raft, len(c.Peers))
	readys := make([]chan error, len(c.Peers))
	setup, start := make(chan struct{}), make(chan struct{})

	for i := range rafts {
		readys[i] = make(chan error)
		rafts[i] = NewRaft(c, i, setup, start, readys[i])

		if err := rafts[i].connect(); err != nil {
			return rafts, start, err
		}
	}

	// all listeners have been opened; start connecting
	// everyone with everyone
	close(setup)

	// wait for everyone to be connected with everyone
	for i := range rafts {
		if err := <-readys[i]; err != nil {
			return rafts, start, err
		}
	}

	return rafts, start, nil
}

// remove/kill a peer network
func (rs Rafts) kill() {
	for i := range rs {
		// some peer may have failed to initialized.
		if rs[i] != nil {
			rs[i].kill()
		}
	}
}

// wait for a leader to be elected; returns nullVotedFor if none
// after we reached the timeout. Second return value is the term.
func (rs Rafts) waitForLeader(d time.Duration) (int, int) {
	timeout := time.NewTimer(d)

	for {
		time.Sleep(20 * time.Millisecond)
		select {
		case <-timeout.C:
			return nullVotedFor, 0
		default:
		}

		if lead := rs.getLeader(); lead != nullVotedFor {
			// meh... things may have changed since getLeader()
			rs[lead].Lock()
			term := rs[lead].currentTerm
			rs[lead].Unlock()
			return lead, term
		}
	}
}
