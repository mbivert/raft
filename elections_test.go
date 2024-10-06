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

		timeout := time.NewTimer(10 * time.Second)

		lead := nullVotedFor
		term := 0

		for {
			time.Sleep(20 * time.Millisecond)
			select {
			case <-timeout.C:
				t.Errorf("No leader elected in 10s: %s", rs)
				rs.kill()
				return
			default:
			}

			if lead = rs.getLeader(); lead != nullVotedFor {
				// meh... things may have changed since getLeader()
				rs[lead].Lock()
				term = rs[lead].currentTerm
				rs[lead].Unlock()
				break
			}
		}

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
