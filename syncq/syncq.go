package syncq

import "kafein/internal/qworker"

func New[Input any, Output any](
	config Config,
) (*worker[Input, Output], *producer[Input, Output]) {
	config.check()
	taskQueue := make(chan Task[Input, Output], config.QueueSize)
	kafeinWorker := qworker.New(
		taskQueue,
		config.WorkerBatchSize,
		config.WorkerLingerTime,
		config.FTimeout,
		config.ErrHandler,
	)
	worker := &worker[Input, Output]{
		worker: kafeinWorker,
	}
	producer := &producer[Input, Output]{
		taskQueue: taskQueue,
		deadline: config.PDeadline,
	}
	producer.isStopped.Store(false)
	return worker, producer
}
