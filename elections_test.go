/*
 * TODO: test with unreliable network
 */
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
			ElectionTimeout: [2]int64{150, 300},
			ElectionTick:    20 * time.Millisecond,
			HeartbeatTick:   20 * time.Millisecond,
		})
		if err != nil {
			t.Errorf(err.Error())
			rs.kill()
			return
		}

		close(start)

		lead, term := rs.waitForLeader(10 * time.Second)

		// somewhat arbitrary
		time.Sleep(time.Duration(rs[0].ElectionTimeout[1]) * time.Millisecond)

		if x := rs.getLeader(); lead != x {
			t.Errorf("Network is stable: leader shouldn't have changed: %d ≠ %d", lead, x)
		} else if x := rs[lead].currentTerm; term != x {
			t.Errorf("Network is stable: term shouldn't have changed: %d ≠ %d", term, x)
		}

		rs.kill()
	}
}

func TestPeerInOutElection(t *testing.T) {
	peerss := [][]string{
		[]string{":6767", ":6868", ":6969"},
		//[]string{":6767", ":6868", ":6969", ":7070", ":7171"},
	}

	for _, peers := range peerss {
		rs, start, err := NewRafts(&Config{
			Peers:           peers,
			ElectionTimeout: [2]int64{150, 300},
			ElectionTick:    20 * time.Millisecond,
			HeartbeatTick:   20 * time.Millisecond,
		})
		if err != nil {
			t.Errorf(err.Error())
			rs.kill()
			return
		}

		close(start)

		lead, _ := rs.waitForLeader(10 * time.Second)
		if lead == nullVotedFor {
			t.Errorf("No leader elected in 10s: %s", rs)
			rs.kill()
			return
		}

		rs[lead].disconnect()

		// somewhat arbitrary
		time.Sleep(time.Duration(rs[0].HeartbeatTick) * 3)

		nlead, _ := rs.waitForLeader(10 * time.Second)
		if lead == nullVotedFor {
			t.Errorf("No leader re-elected after 10s: %s", rs)
			rs.kill()
			return
		}

		// TODO: reconnect rs[lead]; currently other peers
		// won't re-dial down peers periodically, so this
		// for now should fail.
		nlead = nlead

		rs.kill()
	}
}
