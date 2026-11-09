package syncq

import "kafein/internal/qworker"

func New[Input any, Output any](
	config Config,
) (*Worker[Input, Output], *Producer[Input, Output]) {
	config.check()
	taskQueue := make(chan Task[Input, Output], config.QueueSize)
	kafeinWorker := qworker.New(
		taskQueue,
		config.WorkerBatchSize,
		config.WorkerLingerTime,
		config.FTimeout,
	)
	worker := &Worker[Input, Output]{
		worker: kafeinWorker,
	}
	producer := &Producer[Input, Output]{
		taskQueue: taskQueue,
	}
	producer.isStopped.Store(false)
	return worker, producer
}
