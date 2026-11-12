package trigger

import "fmt"

type trigger struct {
	signal     chan struct{}
	errHandler func(err error)
}

func New() *trigger {
	return &trigger{
		signal: make(chan struct{}, 1),
	}
}

func (t *trigger) Fire() {
	select {
	case t.signal <- struct{}{}:
	default:
	}
}

func (t *trigger) OnErr(handler func(err error)) {
	t.errHandler = handler
}

func (t *trigger) handlerErr(err error) {
	if t.errHandler != nil && err != nil {
		t.errHandler(err)
	}
}

func (t *trigger) safelyRunJob(job func() error) {
	defer func() {
		if r := recover(); r != nil {
			// get error from panic
			var err error
			switch x := r.(type) {
			case error:
				err = x
			default:
				err = fmt.Errorf("panic recovered: %v", r)
			}
			t.handlerErr(err)
		}
	}()
	err := job()
	t.handlerErr(err)
}

func (t *trigger) Listen(job func() error) {
	go func() {
		t.safelyRunJob(job)
		for {
			<-t.signal
			t.safelyRunJob(job)
		}
	}()
}
