package syncq

import "time"

type Config struct {
	QueueSize int // default 1000 max queue size

	// worker config
	WorkerBatchSize  int           // default 200
	WorkerLingerTime time.Duration // default 10*time.Millisecond
}

func (c *Config) check() {
	if c.QueueSize <= 0 {
		c.QueueSize = 1000
	}

	if c.WorkerBatchSize <= 0 {
		c.WorkerBatchSize = 200
	}

	if c.WorkerLingerTime <= 0 {
		c.WorkerLingerTime = 10 * time.Millisecond
	}
}
