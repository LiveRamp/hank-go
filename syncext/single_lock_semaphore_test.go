package syncext

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LiveRamp/hank-go-client/fixtures"
	"time"
)

func TestLock(t *testing.T) {

	sem := NewSingleLockSemaphore()

	sem.Release()
	sem.Release()

	sem.Read()

	read := false

	go func() {
		sem.Read()
		read = true
	}()

	time.Sleep(time.Second)
	assert.False(t, read)
	sem.Release()

	fixtures.WaitUntilOrFail(t, func() bool {
		return read
	})

}

func TestInitialState(t *testing.T) {

	sem := NewSingleLockSemaphore()

	read := false

	go func() {
		sem.Read()
		read = true
	}()

	time.Sleep(time.Second)
	assert.False(t, read)
	sem.Release()

	fixtures.WaitUntilOrFail(t, func() bool {
		return read
	})

}

