/*
 * Tests scenarios:
 *	- single-peer network:
 *		- launch the network
 *		- wait for electiontimeout×2 or something
 *		- our peer should be the self-elected leader
 *	- two-peers network:
 *		- launch the network
 *		- wait for electiontimeout×2 or something
 *		- one peer should be the self-elected leader
 *		- wait a little
 *		- same peer should still be self-elected
 *
 * Then, start to add flaws and more peers.
 */
package main

import (
	"testing"
	"time"
)

func TestSinglePeer(t *testing.T) {
	rs, start, err := mkNetwork(&Config{
		Peers:           []string{":6767"},
		ElectionTimeout: [2]int64{150, 300},
	})
	if err != nil {
		t.Errorf(err.Error())
	}

	close(start)

	time.Sleep(time.Millisecond*time.Duration(rs[0].ElectionTimeout[1]) + rs[0].ElectionTick)

	rs[0].Lock()
	if !rs[0].is(Leader) {
		t.Errorf("Expecting to have been elected, is '%s'",  rs[0].state.String())
	}
	if !rs[0].at(1) {
		t.Errorf("First term should be 1, not '%d", +rs[0].currentTerm)
	}
	rs[0].Unlock()

	rmNetwork(rs)
}
