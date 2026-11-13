package asyncq

import (
	"context"
	"sync/atomic"
)

type producer[Input any] struct {
	taskQueue chan Task[Input]
	isStopped atomic.Bool
}

func (p *producer[Input]) Stop() {
	p.isStopped.Store(true)
}

func (p *producer[Input]) AddTaskQ(ctx context.Context, in Input) (err error) {
	if p.isStopped.Load() {
		return ProducerStoppedErr
	}
	select {
	case <-ctx.Done():
		return ctx.Err()

	case p.taskQueue <- Task[Input]{
		input: in,
	}:
		return nil
	default:
		return QueueFullErr
	}
}
