package asyncq

import (
	"context"

	"kafein/internal/qworker"
)

type worker[Input any] struct {
	worker *qworker.Worker[Task[Input]]
}

func (w *worker[Input]) Stop() {
	w.worker.Stop()
}

func (w *worker[Input]) Run(
	flusher func(ctx context.Context, tasks []Task[Input]),
) {
	w.worker.RunWorker(flusher)
}
