package syncq

import (
	"context"
)

type Producer[Input any, Output any] struct {
	taskQueue chan Task[Input, Output]
}

func (p *Producer[Input, Output]) AddTaskQ(ctx context.Context, in Input) (out Output, err error) {
	var failedOut Output
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
		}
	default:
		return failedOut, QueueFullErr
	}
}
