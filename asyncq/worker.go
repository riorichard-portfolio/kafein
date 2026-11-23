package asyncq

import (
	"context"

	"github.com/riorichard-portfolio/kafein/internal/qworker"
)

type worker[Input any] struct {
	worker        *qworker.Worker[flushdTask[Input]]
	walPath       string
	commitedPath  string
	corruptedPath string
	tempPath      string
}

func (w *worker[Input]) Stop() {
	w.worker.Stop()
}

func (w *worker[Input]) Run(
	flusher func(ctx context.Context, tasks []Task[Input]) error,
) {
	// w.worker.RunWorker(flusher)
}
