package curatorext

import (
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/iface"
	"time"
	"path"
	"reflect"
	"github.com/curator-go/curator"
	"testing"
)

func LoadString(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, path string) (interface{}, error) {
	data, error := client.GetData().ForPath(path)
	return string(data), error
}

func TestZkWatchedMap(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	root := "/some/path"

	wmap, _ := NewZkWatchedMap(client, root, &iface.NoOp{}, LoadString)
	time.Sleep(time.Second)

	child1Path := path.Join(root, "child1")

	client.Create().ForPathWithData(child1Path, []byte("data1"))
	fixtures.WaitUntilOrFail(t, func() bool {
		return wmap.Get("child1") == "data1"
	})
	fixtures.WaitUntilOrFail(t, func() bool {
		return reflect.DeepEqual(wmap.KeySet(), []string{"child1"})
	})
	fixtures.WaitUntilOrFail(t, func() bool {
		return reflect.DeepEqual(wmap.Values(), []interface{}{"data1"})
	})

	client.SetData().ForPathWithData(child1Path, []byte("data2"))
	fixtures.WaitUntilOrFail(t, func() bool {
		return wmap.Get("child1") == "data2"
	})

	client.Delete().ForPath(child1Path)
	fixtures.WaitUntilOrFail(t, func() bool {
		return wmap.Get("child1") == nil
	})

	fixtures.TeardownZookeeper(cluster, client)
}

