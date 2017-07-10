package curatorext

import (
	"reflect"
	"github.com/liveramp/hank-go-client/fixtures"
	"github.com/curator-go/curator"
	"fmt"
	"time"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/liveramp/hank-go-client/thriftext"
)

func TestZkWatchedNode(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	wn, err := NewBytesWatchedNode(client, curator.PERSISTENT,
		"/some/location",
		[]byte("0"),
	)

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	ctx := thriftext.NewThreadCtx()

	time.Sleep(time.Second)

	wn.Set(ctx, []byte("data1"))

	fixtures.WaitUntilOrFail(t, func() bool {
		val, _ := wn.Get().([]byte)
		return string(val) == "data1"
	})

	fixtures.TeardownZookeeper(cluster, client)

}

func TestZkWatchedNode2(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	node, _ := NewBytesWatchedNode(client, curator.PERSISTENT, "/some/path", []byte("0"))
	node2, _ := LoadBytesWatchedNode(client, "/some/path")

	ctx := thriftext.NewThreadCtx()

	testData := "Test String"
	setErr := node.Set(ctx, []byte(testData))

	if setErr != nil {
		assert.Fail(t, "Failed")
	}

	fixtures.WaitUntilOrFail(t, func() bool {
		val := asBytes(node2.Get())
		if val != nil {
			return reflect.DeepEqual(string(val), testData)
		}
		return false
	})

	fixtures.TeardownZookeeper(cluster, client)
}

func asBytes(val interface{}) []byte {
	if val != nil {
		return val.([]byte)
	}
	return nil
}
