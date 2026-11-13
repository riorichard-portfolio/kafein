package asyncq

import "context"

type Producer[Input any] interface {
	AddTaskQ(ctx context.Context, in Input) (err error)
}

type Worker[Input any] interface {
	Run(flusher func(ctx context.Context, tasks []Task[Input]))
}
