package hank_client

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/thrift_services"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
	"github.com/bpodgursky/hank-go-client/zk_coordinator"
)

func TestSmartClient(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := iface.NewThreadCtx()

	coordinator, _ := zk_coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	rg, _ := coordinator.AddRingGroup(ctx, "group1")

	fixtures.WaitUntilOrFail(t, func() bool {
		return coordinator.GetRingGroup("group1") != nil
	})

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2)

	smartClient, _ := New(coordinator, "group1", options)
	smartClient2, _ := New(coordinator, "group1", options)

	fixtures.WaitUntilOrFail(t, func() bool {
		return len(rg.GetClients()) == 2
	})

	ring, _ := rg.AddRing(ctx, 0)
	host, _ := ring.AddHost(ctx, "127.0.0.1", 54321, []string{})

	fmt.Println(ring)
	fmt.Println(host)

	fmt.Println(smartClient)
	fmt.Println(smartClient2)

	fixtures.TeardownZookeeper(cluster, client)
}

func TestIt(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := iface.NewThreadCtx()

	coord, _ := zk_coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	domain0, err := coord.AddDomain(ctx, "existent_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, err := coord.AddRingGroup(ctx, "rg1")
	ring1, err := rg1.AddRing(ctx, iface.RingID(0))

	host0, err := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, err := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	host1, err := ring1.AddHost(ctx, "127.0.0.1", 12346, []string{})
	fmt.Println(err)

	host1Domain, err := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(1))

	dg1, err := coord.AddDomainGroup(ctx, "dg1")

	versions := make(map[iface.DomainID]iface.VersionID)
	versions[domain0.GetId()] = iface.VersionID(0)
	dg1.SetDomainVersions(ctx, versions)

	partitioner := &zk_coordinator.Murmur64Partitioner{}

	values := make(map[string]string)
	values["key1"] = "value1"
	values["key2"] = "value2"
	values["key3"] = "value3"
	values["key4"] = "value4"
	values["key5"] = "value5"
	values["key6"] = "value6"
	values["key7"] = "value7"
	values["key8"] = "value8"
	values["key9"] = "value9"
	values["key0"] = "value0"

	server1Values := make(map[string]string)
	server2Values := make(map[string]string)

	for key, val := range values {
		partition := partitioner.Partition([]byte(key), 2)
		fmt.Printf("%v => %v\n", key, partition)
		if partition == 0 {
			server1Values[key] = val
		} else {
			server2Values[key] = val
		}
	}

	handler1 := thrift_services.NewPartitionServerHandler(server1Values)
	handler2 := thrift_services.NewPartitionServerHandler(server2Values)

	close1 := createServer(t, ctx, host0, handler1)
	close2 := createServer(t, ctx, host1, handler2)

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100)

	smartClient, err := New(coord, "rg1", options)

	//	check each record can be found
	for key, value := range values {
		val, _ := smartClient.Get(domain0.GetName(), []byte(key))
		assert.Equal(t, value, string(val.Value))
	}

	fakeDomain, _ := smartClient.Get("fake", []byte("na"))
	assert.True(t, reflect.DeepEqual(noSuchDomain(), fakeDomain))

	//	no replicas live if updating
	setStateBlocking(t, host1, ctx, iface.HOST_UPDATING)
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual(NoConnectionAvailableResponse(), updating)
	})

	//	when offline, try anyway if it's the only replica
	setStateBlocking(t, host1, ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	ok again when serving
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	//	test when a new domain is added, the client picks it up
	domain1, err := coord.AddDomain(ctx, "second_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	//	assign partitions to it
	host1Domain1, err := host1.AddDomain(ctx, domain1)
	host1Domain1.AddPartition(ctx, iface.PartitionID(1))

	host0Domain2, err := host0.AddDomain(ctx, domain1)
	host0Domain2.AddPartition(ctx, iface.PartitionID(0))

	fixtures.WaitUntilOrFail(t, func() bool {
		return len(host1.GetAssignedDomains(ctx)) == 2 && len(host0.GetAssignedDomains(ctx)) == 2
	})

	//	verify that the client can query the domain now
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, _ := smartClient.Get(domain1.GetName(), []byte("key1"))
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	test caching

	handler1.ClearRequestCounters()
	handler2.ClearRequestCounters()

	cachingOptions := NewHankSmartClientOptions().
		SetResponseCacheEnabled(true).
		SetResponseCacheNumItems(10).
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100).
		SetResponseCacheExpiryTime(time.Second)

	cachingClient, err := New(coord, "rg1", cachingOptions)

	//	query once
	val, err := cachingClient.Get(domain1.GetName(), []byte("key1"))
	assert.True(t, reflect.DeepEqual("value1", string(val.Value)))
	assert.Equal(t, int32(1), handler2.NumRequests)

	// verify was found in cache
	val, err = cachingClient.Get(domain1.GetName(), []byte("key1"))
	assert.True(t, reflect.DeepEqual("value1", string(val.Value)))
	assert.Equal(t, int32(1), handler2.NumRequests)

	//	test cache not found
	handler2.ClearRequestCounters()

	val, err = cachingClient.Get(domain1.GetName(), []byte("beef"))
	assert.True(t, *val.NotFound)
	assert.Equal(t, int32(1), handler1.NumRequests)

	val, err = cachingClient.Get(domain1.GetName(), []byte("beef"))
	assert.True(t, *val.NotFound)
	assert.Equal(t, int32(1), handler1.NumRequests)

	//	test cache expires
	time.Sleep(time.Second * 2)
	val, err = cachingClient.Get(domain1.GetName(), []byte("beef"))
	assert.True(t, *val.NotFound)
	assert.Equal(t, int32(2), handler1.NumRequests)

	//	test adding a new server and taking one of the original ones down

	setStateBlocking(t, host1, ctx, iface.HOST_UPDATING)

	host2, err := ring1.AddHost(ctx, "localhost", 12347, []string{})
	host2Domain, err := host2.AddDomain(ctx, domain0)
	host2Domain.AddPartition(ctx, iface.PartitionID(1))

	handler3 := thrift_services.NewPartitionServerHandler(server2Values)
	close3 := createServer(t, ctx, host2, handler3)

	//	make server 1 unreachable
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
		fmt.Println(updating)
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	smartClient.Stop()
	cachingClient.Stop()

	close1()
	close2()
	close3()

	fixtures.TeardownZookeeper(cluster, client)
}

func setStateBlocking(t *testing.T, host iface.Host, ctx *iface.ThreadCtx, state iface.HostState) {
	host.SetState(ctx, state)
	fixtures.WaitUntilOrFail(t, func() bool {
		return host.GetState() == state
	})
}
