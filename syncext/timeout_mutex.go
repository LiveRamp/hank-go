package syncext

import (
	"time"
)

type TimeoutMutex struct {
	c chan struct{}
}

func NewMutex() *TimeoutMutex {
	mutex := &TimeoutMutex{make(chan struct{}, 1)}
	mutex.Unlock()
	return mutex
}

func (m *TimeoutMutex) Lock() {
	<-m.c
}

func (m *TimeoutMutex) Unlock() {
	m.c <- struct{}{}
}

func (m *TimeoutMutex) TryLock(timeout time.Duration) bool {

	//	the timeout == 0 case is just an optimization so we don't spin up a timer for for 0 ms
	if timeout == 0 {
		select {
		case <-m.c:
			return true
		default:
			return false
		}
	} else {
		timer := time.NewTimer(timeout)
		select {
		case <-m.c:
			timer.Stop()
			return true
		case <-time.After(timeout):
			return false
		}
	}
}

//	no promises about the second after it is tested.  only approximate.
func (m *TimeoutMutex) TestIsLocked() bool {

	locked := m.TryLock(0)
	if locked {
		m.Unlock()
	}

	return !locked
}

func (m *TimeoutMutex) TryLockNoWait() bool {
	return m.TryLock(0)
}
