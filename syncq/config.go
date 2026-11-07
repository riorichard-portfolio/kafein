package syncq

import "time"

type Config struct {
	QueueSize int // default 1000 max queue size

	// worker config
	NumWorkers       int           // default 1 worker
	WorkerBatchSize  int           // default 200
	WorkerLingerTime time.Duration // default 10*time.Millisecond

	// producer config
	NumPartition int // default 10

}

func (c *Config) check() {
	if c.QueueSize <= 0 {
		c.QueueSize = 1000
	}

	if c.NumWorkers <= 0 {
		c.NumWorkers = 1
	}

	if c.WorkerBatchSize <= 0 {
		c.WorkerBatchSize = 200
	}

	if c.WorkerLingerTime <= 0 {
		c.WorkerLingerTime = 10 * time.Millisecond
	}

	if c.NumPartition <= 0 {
		c.NumPartition = 10
	}
}
