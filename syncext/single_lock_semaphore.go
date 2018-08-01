package syncext

type SingleLockSemaphore struct {
	c chan struct{}
}

//	this does NOT start unlocked
func NewSingleLockSemaphore() *SingleLockSemaphore {
	mutex := &SingleLockSemaphore{make(chan struct{}, 1)}
	mutex.Read()
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
