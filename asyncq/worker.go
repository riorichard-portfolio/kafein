package asyncq

import (
	"kafein/internal/qworker"
)

type Worker[Input any] struct {
	worker *qworker.Worker[Task[Input]]
}

func (w *Worker[Input]) Stop() {
	w.worker.Stop()
}

func (w *Worker[Input]) Run(
	flusher qworker.Flusher[Task[Input]],
) {
	w.worker.RunWorker(flusher)
}
