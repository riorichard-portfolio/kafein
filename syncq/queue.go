package syncq

type TaskQueue[Input any, Output any] struct {
	input    Input
	replyOut chan Output
	replyErr chan error
}

func (q *TaskQueue[Input, Output]) GetInput() Input {
	return q.input
}

func (q *TaskQueue[Input, Output]) SendOutput(out Output) {
	q.replyOut <- out
}

func (q *TaskQueue[Input, Output]) SendError(err error) {
	q.replyErr <- err
}
