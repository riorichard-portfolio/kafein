package syncq

import (
	"context"
	"time"

	"kafein/internal/timer"
)

type Worker[Input any, Output any] struct {
	taskQueue  chan Task[Input, Output]
	batchSize  int
	lingerTime time.Duration
	stopSign   chan struct{}
	timeout    time.Duration
}

type Flusher[Input any, Output any] func(ctx context.Context, tasks []Task[Input, Output])

func (w *Worker[Input, Output]) flushWithCtx(
	flusher Flusher[Input, Output],
	tasksBatch []Task[Input, Output],
) {
	if len(tasksBatch) <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	flusher(ctx, tasksBatch)
}

func (w *Worker[Input, Output]) RunWorker(
	flusher Flusher[Input, Output],
) {
	tasksBatch := make([]Task[Input, Output], 0, w.batchSize)
	t := timer.NewTimer(w.lingerTime)
	for {
		select {
		case <-w.stopSign:
			t.Stop()
			// stop need to drain first
			for {
				select {
				case task, ok := <-w.taskQueue:
					if !ok {
						w.flushWithCtx(flusher, tasksBatch)
						return
					}
					tasksBatch = append(tasksBatch, task)

					// checking if batch is full then flush current draining
					if len(tasksBatch) >= w.batchSize {
						w.flushWithCtx(flusher, tasksBatch)
						tasksBatch = tasksBatch[:0]
					}
				default:
					// not receiving any task drained complete need to flush
					w.flushWithCtx(flusher, tasksBatch)
					return
				}
			}
		case task, ok := <-w.taskQueue:
			if !ok {
				// when queue channel failed flush data for the last time
				w.flushWithCtx(flusher, tasksBatch)
				return
			}

			tasksBatch = append(tasksBatch, task)

			if len(tasksBatch) == 1 {
				t.Start()
			}

			if len(tasksBatch) >= w.batchSize {
				t.Stop()
				// when batch full run flusher
				w.flushWithCtx(flusher, tasksBatch)
				tasksBatch = tasksBatch[:0]
			}
		case <-t.Timeout():
			// when linger time over
			w.flushWithCtx(flusher, tasksBatch)
			tasksBatch = tasksBatch[:0]
		}
	}
}
