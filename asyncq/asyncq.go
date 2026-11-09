package asyncq

import "kafein/internal/qworker"

func New[Input any](
	config Config,
) (*Worker[Input], *Producer[Input]) {
	config.check()
	taskQueue := make(chan Task[Input], config.QueueSize)
	kafeinWorker := qworker.New(
		taskQueue,
		config.WorkerBatchSize,
		config.WorkerLingerTime,
		config.FTimeout,
	)
	worker := &Worker[Input]{
		worker: kafeinWorker,
	}
	producer := &Producer[Input]{
		taskQueue: taskQueue,
	}
	producer.isStopped.Store(false)
	return worker, producer
}
