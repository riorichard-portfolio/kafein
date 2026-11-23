package asyncq

import "github.com/riorichard-portfolio/kafein/internal/qworker"

var (
	durablePathExists = map[string]struct{}{}
)

// durablePath for durability task folder path
func New[Input any](
	durablePath string,
	config Config,
) (*worker[Input], *producer[Input], error) {
	_, exists := durablePathExists[durablePath]
	if exists {
		return nil, nil, DurablePathExistsErr
	}
	durablePathExists[durablePath] = struct{}{}
	err := mkDirAll(durablePath)
	if err != nil {
		return nil, nil, err
	}
	config.check()
	taskQueue := make(chan flushdTask[Input], config.QueueSize)
	kafeinWorker := qworker.New(
		taskQueue,
		config.WorkerBatchSize,
		config.WorkerLingerTime,
		config.FTimeout,
		config.ErrHandler,
	)
	worker := &worker[Input]{
		worker:        kafeinWorker,
		walPath:       joinPath(durablePath, wal),
		commitedPath:  joinPath(durablePath, commited),
		corruptedPath: joinPath(durablePath, corrupted),
		tempPath:      joinPath(durablePath, temp),
	}
	producer := &producer[Input]{
		taskQueue: taskQueue,
	}
	producer.isStopped.Store(false)
	return worker, producer, nil
}
