package asyncq

type Task[Input any] struct {
	input    Input
}

type flushTask[Input any] struct {
	input    Input
	replyErr chan error
}

func (t *Task[Input]) GetInput() Input {
	return t.input
}
