package asyncq

import (
	"github.com/riorichard-portfolio/kafein/internal/qworker"
)

type worker[Input any] struct {
	worker           *qworker.Worker[flushdTask[Input]]
	walPath          string
	commitedPath     string
	corruptedPath    string
	tempPath         string
	readerSig        chan struct{}
	readerErrHandler func(walPath WALPath, err error)
}

func (w *worker[Input]) Stop() {
	w.worker.Stop()
}

func (w *worker[Input]) Run(
	flusher func(tasks []Task[Input]) error,
) {
	// run kafein worker (goroutine) to flush to disk when running
	w.worker.RunWorker(w.flushToDisk)

	// background for wal reader , first scan then accepting reader signal
	go func() {
		w.readWAL(flusher)
		w.readWALOnSIG(flusher)
	}()
}
