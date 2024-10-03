package main

// Create a network of raft peers for tests purposes
func mkNetwork(c *Config) ([]*Raft, error) {
	rafts := make([]*Raft, len(c.Peers))
	readys := make([]chan struct{}, len(c.Peers))
	start := make(chan struct{})

	for i := range rafts {
		readys[i] = make(chan struct{})
		rafts[i] = NewRaft(c, i, start, readys[i])

		if err := rafts[i].connect(); err != nil {
			return rafts, err
		}
	}

	close(start)

	for i := range rafts {
		<-readys[i]
	}

	return rafts, nil
}
