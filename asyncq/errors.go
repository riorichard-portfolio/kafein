package asyncq

import "errors"

var (
	QueueFullErr          = errors.New("queue full cannot add more task")
	ProducerStoppedErr    = errors.New("producer is stopped")
	InvalidDurablePathErr = errors.New("durable path must be a valid path")
	DurablePathExistsErr  = errors.New("durable path already exists")
	FailedDiskWriteErr    = errors.New("failed to write to disk for durability")
)
