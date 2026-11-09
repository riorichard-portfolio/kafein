package syncq

import (
	"kafein/internal/worker"
)

type Worker[Input any, Output any] struct {
	worker *worker.Worker[Task[Input, Output]]
}

func (w *Worker[Input, Output]) Stop() {
	w.worker.Stop()
}

func (w *Worker[Input, Output]) Run(
	flusher worker.Flusher[Task[Input, Output]],
) {
	w.worker.RunWorker(flusher)
}
