package asyncq

type Task[Input any] struct {
	input Input
}

func (t *Task[Input]) GetInput() Input {
	return t.input
}
