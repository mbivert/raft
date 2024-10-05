package main

// Create a network of raft peers for tests purposes
func mkNetwork(c *Config) ([]*Raft, chan<- struct{}, error) {
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

// remove/kill a peer network instantiated by mkNetwork()
func rmNetwork(rafts []*Raft) {
	for i := range rafts {
		// stop long-running goroutines
		close(rafts[i].stopped)

		// stop RPC server
		rafts[i].disconnect()
	}
}
