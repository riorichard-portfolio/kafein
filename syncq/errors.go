package syncq

import "errors"

var(
	QueueFullErr = errors.New("queue full cannot add more task")
	ProducerStoppedErr = errors.New("producer is stopped")
)