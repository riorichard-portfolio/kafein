package asyncq

import (
	"context"
	"sync/atomic"
	"time"
)

type producer[Input any] struct {
	taskQueue chan flushdTask[Input]
	isStopped atomic.Bool
	deadline  time.Duration
}

func (p *producer[Input]) Stop() {
	p.isStopped.Store(true)
}

func (p *producer[Input]) AddTaskQ(ctx context.Context, in Input) (err error) {
	if p.isStopped.Load() {
		return ProducerStoppedErr
	}
	errChan := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()

	case p.taskQueue <- flushdTask[Input]{
		input:    in,
		replyErr: errChan,
	}:
		err := <-errChan
		return err
	default:
		return QueueFullErr
	}
}
