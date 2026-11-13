package syncq

import "sync/atomic"

type Task[Input any, Output any] struct {
	input    Input
	replyOut chan Output
	replyErr chan error
	isReplied atomic.Bool
}

func (t *Task[Input, Output]) GetInput() Input {
	return t.input
}

func (t *Task[Input, Output]) SendOutput(out Output) {
	if !t.isReplied.CompareAndSwap(false,true) {
		return
	}
	t.replyOut <- out
}

func (t *Task[Input, Output]) SendError(err error) {
	if !t.isReplied.CompareAndSwap(false,true) {
		return
	}
	t.replyErr <- err
}