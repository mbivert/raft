package main

import (
	"testing"
	"time"
)

func TestUnperturbatedElection(t *testing.T) {
	peerss := [][]string{
		[]string{":6767"},
		[]string{":6767", ":6868"},
		[]string{":6767", ":6868", ":6969"},
		[]string{":6767", ":6868", ":6969", ":7070", ":7171"},
	}

	for _, peers := range peerss {
		rs, start, err := NewRafts(&Config{
			Peers:           peers,
			RPCTimeout:      500 * time.Millisecond,
			ElectionTimeout: [2]int64{150, 300},
			ElectionTick:    20 * time.Millisecond,
			HeartbeatTick:   20 * time.Millisecond,
		})
		if err != nil {
			rs.kill()
			t.Fatalf(err.Error())
		}

		close(start)

		lead, term := rs.waitForLeader(10 * time.Second)

		// somewhat arbitrary
		time.Sleep(time.Duration(rs[0].ElectionTimeout[1]) * time.Millisecond)

		// XXX this sometimes still fails (go test -v -race . -count 20 -failfast);
		// the practical impact should be negligible though: we're stable enough.
		if x := rs.getLeader(); lead != x {
			t.Errorf("Network is stable: leader shouldn't have changed: %d ≠ %d", lead, x)
		} else if x := rs[lead].currentTerm; term != x {
			t.Errorf("Network is stable: term shouldn't have changed: %d ≠ %d", term, x)
		}

		rs.kill()
	}
}

func TestLeadInOutElection(t *testing.T) {
	peerss := [][]string{
		[]string{":6767", ":6868", ":6969"},
		[]string{":6767", ":6868", ":6969", ":7070", ":7171"},
	}

	for _, peers := range peerss {
		rs, start, err := NewRafts(&Config{
			Peers:           peers,
			RPCTimeout:      500 * time.Millisecond,
			ElectionTimeout: [2]int64{150, 300},
			ElectionTick:    20 * time.Millisecond,
			HeartbeatTick:   20 * time.Millisecond,
			Testing:         true,
		})
		if err != nil {
			rs.kill()
			t.Fatalf(err.Error())
			return
		}

		close(start)

		lead, term := rs.waitForLeader(10 * time.Second)
		if lead == nullVotedFor {
			t.Errorf("No leader elected in 10s: %s", rs)
			rs.kill()
			return
		}

		if err := rs[lead].kill(); err != nil {
			t.Errorf("Failed to kill %d: %s", lead, err.Error())
			rs.kill()
			return
		}

		// wait for election timeout to fail
		time.Sleep(
			time.Duration(rs[0].ElectionTimeout[1])*time.Millisecond +
				rs[0].HeartbeatTick*2)

		nlead, nterm := rs.waitForLeader(10 * time.Second)
		if lead == nullVotedFor {
			t.Errorf("No leader re-elected after 10s: %s", rs)
			rs.kill()
			return
		}
		if nlead == lead || nterm == term {
			t.Errorf("Expected new term/leader %d/%d", lead, term)
			rs.kill()
			return
		}

		rs[lead].unkill()

		// This should be long enough for either rs[lead] to receive
		// a heartbeat or convert back to follower.
		time.Sleep(
			time.Duration(rs[0].ElectionTimeout[1])*time.Millisecond +
				rs[0].HeartbeatTick*2)

		if rs[lead].lAt(term) {
			t.Errorf("Ancient leader should have moved to new term")
		}

		rs.kill()
	}
}
