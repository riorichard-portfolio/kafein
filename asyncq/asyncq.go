package asyncq

import "kafein/internal/qworker"

func New[Input any](
	config Config,
) (*worker[Input], *producer[Input]) {
	config.check()
	taskQueue := make(chan Task[Input], config.QueueSize)
	kafeinWorker := qworker.New(
		taskQueue,
		config.WorkerBatchSize,
		config.WorkerLingerTime,
		config.FTimeout,
	)
	worker := &worker[Input]{
		worker: kafeinWorker,
	}
	producer := &producer[Input]{
		taskQueue: taskQueue,
	}
	producer.isStopped.Store(false)
	return worker, producer
}
