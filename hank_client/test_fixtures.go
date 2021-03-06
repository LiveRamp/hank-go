package hank_client

import (
	"strconv"
	"testing"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"
	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go/fixtures"
	"github.com/LiveRamp/hank-go/iface"
	"github.com/LiveRamp/hank-go/thrift_services"
	"github.com/LiveRamp/hank-go/thriftext"
	"github.com/LiveRamp/hank-go/zk_coordinator"
)

func createHostServer(t *testing.T, ctx *thriftext.ThreadCtx, client curator.CuratorFramework, i int, server hank.PartitionServer) (iface.Host, func()) {
	host, _ := createHost(ctx, client, i)
	return host, createServer(t, ctx, host, server)
}

func createServer(t *testing.T, ctx *thriftext.ThreadCtx, host iface.Host, server hank.PartitionServer) func() {
	_, close := thrift_services.Serve(
		server,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		host.GetAddress().Print())
	host.SetState(ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrFail(t, func() bool {
		return host.GetState() == iface.HOST_SERVING
	})

	return close
}

func createHost(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, i int) (iface.Host, error) {
	return zk_coordinator.CreateZkHost(ctx, client, &thriftext.NoOp{}, "/hank/host/host"+strconv.Itoa(i), "127.0.0.1", 12345+i, []string{})
}
