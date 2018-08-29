package zk_coordinator

import (
	"testing"

	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"
	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/curatorext"
	"github.com/LiveRamp/hank-go-client/fixtures"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/thriftext"
)

//	TODO get some non-hank dummy thrift types and test this in the curatorext package
func TestUpdateWatchedNode(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	hostData := hank.NewHostAssignmentsMetadata()
	hostData.Domains = make(map[int32]*hank.HostDomainMetadata)
	hostData.Domains[0] = hank.NewHostDomainMetadata()

	ctx := thriftext.NewThreadCtx()

	node, _ := curatorext.NewThriftWatchedNode(client, curator.PERSISTENT, "/some/path", ctx, iface.NewHostAssignmentMetadata, hostData)
	node2, _ := curatorext.LoadThriftWatchedNode(client, "/some/path", iface.NewHostAssignmentMetadata)


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
