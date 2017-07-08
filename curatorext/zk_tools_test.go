package curatorext

import (
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
This isn't really testing our code, it's just a sanity check that the zk / curator packages work, in case we update them
*/

func TestLocalZkServer(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	conn, _, _ := cluster.ConnectAll()
	conn.Create("/something", []byte("data1"), 0, zk.WorldACL(zk.PermAll))
	get, _, _ := conn.Get("/something")
	assert.Equal(t, "data1", string(get))

	fixtures.TeardownZookeeper(cluster, client)
}

func TestCurator(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	client.Create().ForPathWithData("/something", []byte("data1"))
	data, _ := client.GetData().ForPath("/something")
	assert.Equal(t, "data1", string(data))

	fixtures.TeardownZookeeper(cluster, client)
}
