package timer

import (
	"time"
)

type Timer struct {
	timer    *time.Timer
	duration time.Duration
}

// duration default 10ms if duration <= 0 ms
func NewTimer(duration time.Duration) *Timer {
	if duration <= 0 {
		duration = 10 * time.Millisecond
	}
	t := time.NewTimer(duration)
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	return &Timer{
		timer:    t,
		duration: duration,
	}
}

func (t *Timer) drainTimerC() {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
}

func (t *Timer) Start() {
	t.drainTimerC()
	t.timer.Reset(t.duration)
}

func (t *Timer) Stop() {
	t.drainTimerC()
}

func (t *Timer) Timeout() <-chan time.Time {
	return t.timer.C
}
