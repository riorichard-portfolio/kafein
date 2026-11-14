package qworker

import (
	"context"
	"fmt"
	"time"

	"github.com/riorichard-portfolio/kafein/internal/timer"
)

type Worker[Task any] struct {
	taskQueue  chan Task
	batchSize  int
	lingerTime time.Duration
	stopSignal chan struct{}
	timeout    time.Duration
	errHandler func(err error)
}

func New[Task any](
	taskQueue chan Task,
	batchSize int,
	lingerTime time.Duration,
	ftimeout time.Duration,
	errHandler func(err error),
) *Worker[Task] {
	stopSignal := make(chan struct{}, 1)
	return &Worker[Task]{
		taskQueue:  taskQueue,
		batchSize:  batchSize,
		lingerTime: lingerTime,
		timeout:    ftimeout,
		stopSignal: stopSignal,
		errHandler: errHandler,
	}
}

func (w *Worker[Task]) Stop() {
	w.stopSignal <- struct{}{}
}

func (w *Worker[Task]) handleFlushErr(err error) {
	if w.errHandler != nil && err != nil {
		w.errHandler(err)
	}
}

func (w *Worker[Task]) flush(
	flusher func(ctx context.Context, tasks []Task) error,
	tasksBatch []Task,
) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("[CRITICAL] Worker panicked: %v", r)
		}
		w.handleFlushErr(err)
	}()
	if len(tasksBatch) <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	err = flusher(ctx, tasksBatch)
}

func (w *Worker[Task]) RunWorker(
	flusher func(ctx context.Context, tasks []Task) error,
) {
	go func() {
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
							w.flush(flusher, tasksBatch)
							return
						}
						tasksBatch = append(tasksBatch, task)

						// checking if batch is full then flush current draining
						if len(tasksBatch) >= w.batchSize {
							w.flush(flusher, tasksBatch)
							tasksBatch = tasksBatch[:0]
						}
					default:
						// not receiving any task drained complete need to flush
						w.flush(flusher, tasksBatch)
						return
					}
				}
			case task, ok := <-w.taskQueue:
				if !ok {
					// when queue channel failed flush data for the last time
					w.flush(flusher, tasksBatch)
					return
				}

				tasksBatch = append(tasksBatch, task)

				if len(tasksBatch) == 1 {
					t.Start()
				}

				if len(tasksBatch) >= w.batchSize {
					t.Stop()
					// when batch full run flusher
					w.flush(flusher, tasksBatch)
					tasksBatch = tasksBatch[:0]
				}
			case <-t.Timeout():
				// when linger time over
				w.flush(flusher, tasksBatch)
				tasksBatch = tasksBatch[:0]
			}
		}
	}()
}
