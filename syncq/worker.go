package syncq

import (
	"kafein/internal/qworker"
)

type Worker[Input any, Output any] struct {
	worker *qworker.Worker[Task[Input, Output]]
}

func (w *Worker[Input, Output]) Stop() {
	w.worker.Stop()
}

func (w *Worker[Input, Output]) Run(
	flusher worker.Flusher[Task[Input, Output]],
) {
	w.worker.RunWorker(flusher)
}
