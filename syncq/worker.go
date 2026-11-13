package syncq

import (
	"context"

	"kafein/internal/qworker"
)

type worker[Input any, Output any] struct {
	worker *qworker.Worker[Task[Input, Output]]
}

func (w *worker[Input, Output]) Stop() {
	w.worker.Stop()
}

func (w *worker[Input, Output]) Run(
	flusher func(ctx context.Context, tasks []Task[Input, Output]) error,
) {
	w.worker.RunWorker(flusher)
}
