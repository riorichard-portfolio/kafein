package syncq

type Task[Input any, Output any] struct {
	input    Input
	replyOut chan Output
	replyErr chan error
}

func (t *Task[Input, Output]) GetInput() Input {
	return t.input
}

func (t *Task[Input, Output]) SendOutput(out Output) {
	t.replyOut <- out
}

func (t *Task[Input, Output]) SendError(err error) {
	t.replyErr <- err
}