package syncq

import "context"

type Producer[Input any, Output any] interface {
	AddTaskQ(ctx context.Context, in Input) (out Output, err error)
}

type Worker[Input any, Output any] interface {
	Run(flusher func(ctx context.Context, tasks []Task[Input, Output]))
}
