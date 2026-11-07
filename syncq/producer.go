package syncq

import (
	"context"
	"sync/atomic"

	"kafein/internal/hashing"
)

type Producer[Input any, Output any] struct {
	taskQueues    []chan Task[Input, Output]
	roundRobinIdx atomic.Uint64
}

func (p *Producer[Input, Output]) AddTaskQ(ctx context.Context, in Input, taskCfg TaskCfg) (out Output, err error) {
	var qIndice uint64
	numPartitions := uint64(len(p.taskQueues))
	if !taskCfg.IsWithPartition() {
		counter := p.roundRobinIdx.Add(1)
		qIndice = (counter - 1) % numPartitions
	} else {
		hash := hashing.Fnv1a(taskCfg.PartitionKey)
		qIndice = hash % numPartitions
	}
	var failedOut Output
	outChan := make(chan Output, 1)
	errChan := make(chan error, 1)
	select {
	case <-ctx.Done():
		return failedOut, ctx.Err()

	case p.taskQueues[qIndice] <- Task[Input, Output]{
		input:    in,
		replyOut: outChan,
		replyErr: errChan,
	}:
		select {
		case out := <-outChan:
			return out, nil
		case err := <-errChan:
			return failedOut, err
		case <-ctx.Done():
			return failedOut, ctx.Err()
		}
	default:
		return failedOut, QueueFullErr
	}
}
