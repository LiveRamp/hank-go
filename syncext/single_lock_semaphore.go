package syncext

type SingleLockSemaphore struct {
	c chan struct{}
}

func NewSingleLockSemaphore() *SingleLockSemaphore {
	mutex := &SingleLockSemaphore{make(chan struct{}, 1)}
	mutex.Release()
	return mutex
}

func (m *SingleLockSemaphore) Read() {
	m.c <- struct{}{}
}

func (m *SingleLockSemaphore) Release() bool {
	select {
	case <-m.c:
		return true
	default:
		return false
	}
}