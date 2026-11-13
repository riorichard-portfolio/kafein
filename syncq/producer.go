package syncq

import (
	"context"
	"sync/atomic"
	"time"
)

type producer[Input any, Output any] struct {
	taskQueue chan Task[Input, Output]
	isStopped atomic.Bool
	deadline  time.Duration
}

func (p *producer[Input, Output]) Stop() {
	p.isStopped.Store(true)
}

func (p *producer[Input, Output]) AddTaskQ(ctx context.Context, in Input) (out Output, err error) {
	var failedOut Output
	if p.isStopped.Load() {
		return failedOut, ProducerStoppedErr
	}
	outChan := make(chan Output, 1)
	errChan := make(chan error, 1)
	select {
	case <-ctx.Done():
		return failedOut, ctx.Err()

	case p.taskQueue <- Task[Input, Output]{
		input:    in,
		replyOut: outChan,
		replyErr: errChan,
	}:
		select {
		case out := <-outChan:
			return out, nil
		case err := <-errChan:
			return failedOut, err
		case <-ctx.Done():
			return failedOut, ctx.Err()
		case <-time.After(p.deadline):
			return failedOut, DeadlineErr
		}
	default:
		return failedOut, QueueFullErr
	}
}
