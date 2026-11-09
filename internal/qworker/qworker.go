package qworker

import (
	"context"
	"time"

	"kafein/internal/timer"
)

type Worker[Task any] struct {
	taskQueue  chan Task
	batchSize  int
	lingerTime time.Duration
	stopSignal chan struct{}
	timeout    time.Duration
}

func New[Task any](
	taskQueue chan Task,
	batchSize int,
	lingerTime time.Duration,
	ftimeout time.Duration,
) *Worker[Task] {
	stopSignal := make(chan struct{}, 1)
	return &Worker[Task]{
		taskQueue:  taskQueue,
		batchSize:  batchSize,
		lingerTime: lingerTime,
		timeout:    ftimeout,
		stopSignal: stopSignal,
	}
}

type Flusher[Task any] func(ctx context.Context, tasks []Task)

func (w *Worker[Task]) Stop() {
	w.stopSignal <- struct{}{}
}

func (w *Worker[Task]) flushWithCtx(
	flusher Flusher[Task],
	tasksBatch []Task,
) {
	if len(tasksBatch) <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	flusher(ctx, tasksBatch)
}

func (w *Worker[Task]) RunWorker(
	flusher Flusher[Task],
) {
	tasksBatch := make([]Task, 0, w.batchSize)
	t := timer.NewTimer(w.lingerTime)
	defer t.Stop()
	for {
		select {
		case <-w.stopSignal:
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
