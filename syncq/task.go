package syncq

type Task[Input any, Output any] struct {
	input    Input
	replyOut chan Output
	replyErr chan error
}

func (q *Task[Input, Output]) GetInput() Input {
	return q.input
}

func (q *Task[Input, Output]) SendOutput(out Output) {
	q.replyOut <- out
}

func (q *Task[Input, Output]) SendError(err error) {
	q.replyErr <- err
}