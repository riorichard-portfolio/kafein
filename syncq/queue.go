package syncq

type taskQueue[Input any, Output any] struct {
	input Input
	reply chan Output
}

func (q *taskQueue[Input, Output]) GetInput() Input {
	return q.input
}

func (q *taskQueue[Input, Output]) SendOutput(out Output) {
	q.reply <- out
}
