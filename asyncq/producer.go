package asyncq

import (
	"context"
	"sync/atomic"
)

type Producer[Input any] struct {
	taskQueue chan Task[Input]
	isStopped atomic.Bool
}

func (p *Producer[Input]) Stop() {
	p.isStopped.Store(true)
}

func (p *Producer[Input]) AddTaskQ(ctx context.Context, in Input) (err error) {
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
