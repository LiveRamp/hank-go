package curatorext

import (
	"reflect"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/curator-go/curator"
	"github.com/bpodgursky/hank-go-client/iface"
	"fmt"
	"time"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
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

	ctx := iface.NewThreadCtx()

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

	ctx := iface.NewThreadCtx()

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

func TestUpdateWatchedNode(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	hostData := hank.NewHostAssignmentsMetadata()
	hostData.Domains = make(map[int32]*hank.HostDomainMetadata)
	hostData.Domains[0] = hank.NewHostDomainMetadata()

	ctx := iface.NewThreadCtx()

	node, _ := NewThriftWatchedNode(client, curator.PERSISTENT, "/some/path", ctx, iface.NewHostAssignmentMetadata, hostData)
	node2, _ := LoadThriftWatchedNode(client, "/some/path", iface.NewHostAssignmentMetadata)

	node.Update(ctx, func(val interface{}) interface{} {
		meta := val.(*hank.HostAssignmentsMetadata)
		meta.Domains[1] = hank.NewHostDomainMetadata()
		return meta
	})

	fixtures.WaitUntilOrFail(t, func() bool {
		meta := iface.AsHostAssignmentsMetadata(node2.Get())
		return meta != nil && len(meta.Domains) == 2
	})

	fixtures.TeardownZookeeper(cluster, client)
}

func asBytes(val interface{}) []byte {
	if val != nil {
		return val.([]byte)
	}
	return nil
}
