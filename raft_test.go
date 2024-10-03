/*
 * Testing utilities.
 */
package main

import (
	"testing"
)

func TestRstElectionTimeout(t *testing.T) {
	c := Config{
		Peers: []string{":1010"},
		ElectionTimeout: [2]int64{150, 300},
	}

	r := NewRaft(&c, 0, make(chan struct{}), make(chan error))
	for n := 0; n < 50; n++ {
		d := r.rstElectionTimeout()
		if d < c.ElectionTimeout[0] || d > c.ElectionTimeout[1] {
			t.Errorf("Election timeout out of bounds: %d ∉ [%d, %d[",
				d, c.ElectionTimeout[0], c.ElectionTimeout[1])
		}
	}
}
