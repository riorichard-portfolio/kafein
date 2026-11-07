package syncq

type TaskQueue[Input any, Output any] struct {
	input Input
	reply chan Output
}

func (q *TaskQueue[Input, Output]) GetInput() Input {
	return q.input
}

func (q *TaskQueue[Input, Output]) SendOutput(out Output) {
	q.reply <- out
}
