package hank_client

import (
	"github.com/curator-go/curator"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/LiveRamp/hank-go-client/fixtures"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/thrift_services"
	"github.com/LiveRamp/hank-go-client/thriftext"
	"github.com/LiveRamp/hank-go-client/zk_coordinator"
)

func TestSmartClient(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()
	coordinator, _ := initializeCoordinator(client)

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
	ring.AddHost(ctx, "127.0.0.1", 54321, []string{})

	smartClient.Stop()
	smartClient2.Stop()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestIt(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, _ := coord.AddRingGroup(ctx, "rg1")
	ring1, _ := rg1.AddRing(ctx, iface.RingID(0))

	host0, _ := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	host1, _ := ring1.AddHost(ctx, "127.0.0.1", 12346, []string{})

	host1Domain, _ := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(1))

	dg1, _ := coord.AddDomainGroup(ctx, "dg1")

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

	if err != nil{
		t.Fail()
	}

	//	check each record can be found
	for key, value := range values {
		val, _ := smartClient.Get(domain0.GetName(), []byte(key))
		assert.Equal(t, value, string(val.Value))
	}

	_, err = smartClient.Get("fake", []byte("na"))

	assert.Equal(t, "domain fake not found", err.Error())

	//	no replicas live if updating
	setStateBlocking(t, host1, ctx, iface.HOST_UPDATING)
	fixtures.WaitUntilOrFail(t, func() bool {
		_, err := smartClient.Get(domain0.GetName(), []byte("key1"))
		return reflect.DeepEqual("no connection available to domain "+domain0.GetName(), err.Error())
	})

	//	when offline, try anyway if it's the only replica
	setStateBlocking(t, host1, ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrFail(t, func() bool {

		updating, err := smartClient.Get(domain0.GetName(), []byte("key1"))

		if err != nil{
			return false
		}

		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	ok again when serving
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)
	fixtures.WaitUntilOrFail(t, func() bool {
		updating, err := smartClient.Get(domain0.GetName(), []byte("key1"))
		if err != nil{
			return false
		}

		return reflect.DeepEqual("value1", string(updating.Value))
	})

	//	test when a new domain is added, the client picks it up
	domain1, _ := coord.AddDomain(ctx, "second_domain", 2, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	//	assign partitions to it
	host1Domain1, _ := host1.AddDomain(ctx, domain1)
	host1Domain1.AddPartition(ctx, iface.PartitionID(1))

	host0Domain2, _ := host0.AddDomain(ctx, domain1)
	host0Domain2.AddPartition(ctx, iface.PartitionID(0))

	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrFail(t, func() bool {
		return len(host1.GetAssignedDomains(ctx)) == 2 && len(host0.GetAssignedDomains(ctx)) == 2
	})

	//	verify that the client can query the domain now
	fixtures.WaitUntilOrFail(t, func() bool {

		updating, err := smartClient.Get(domain1.GetName(), []byte("key1"))
		if err != nil{
			return false
		}

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

	cachingClient, _ := New(coord, "rg1", cachingOptions)

	//	query once
	val, err := cachingClient.Get(domain1.GetName(), []byte("key1"))
	assert.Nil(t, err)

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
		return reflect.DeepEqual("value1", string(updating.Value))
	})

	smartClient.Stop()
	cachingClient.Stop()

	close1()
	close2()
	close3()

	fixtures.TeardownZookeeper(cluster, client)
}


func TestSelectiveCacheRebuilds(t *testing.T) {

	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, _ := coord.AddRingGroup(ctx, "rg1")
	ring1, _ := rg1.AddRing(ctx, iface.RingID(0))

	host0, _ := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))
	assigned := host0.GetAssignedDomains(ctx)
	partition := assigned[0].GetPartitions()[0]

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(1).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	server1Values := make(map[string]string)
	server1Values["key1"] = "value1"

	handler := thrift_services.NewPartitionServerHandler(server1Values)
	close1 := createServer(t, ctx, host0, handler)

	fixtures.WaitUntilOrFail(t, queryMatches("key1", "value1", "rg1", domain0.GetName(), coord, options))

	smartClient, _ := New(coord, "rg1", options)

	assert.Equal(t, 0, smartClient.numCacheRebuildTriggers)
	assert.Equal(t, 0, smartClient.numClosedConnections)
	assert.Equal(t, 1, smartClient.numCreatedConnections)

	// do something important.
	rg1.AddRing(ctx, iface.RingID(1))
	fixtures.WaitUntilOrFail(t, func() bool{
		return smartClient.numCacheRebuildTriggers == 1
	})

	assert.Equal(t, 0, smartClient.numClosedConnections)
	assert.Equal(t, 1, smartClient.numCreatedConnections)

	assert.Equal(t, 0, smartClient.numSkippedRebuildTriggers)

	//	update the partition version (eg, what a server does when updating)
	partition.SetCurrentDomainVersion(ctx, iface.VersionID(1))
	time.Sleep(time.Second * 2)

	//	assert we skipped the update
	fixtures.WaitUntilOrFail(t, func() bool {
		return smartClient.numSkippedRebuildTriggers == 1
	})
	assert.Equal(t, 1, smartClient.numCacheRebuildTriggers)

	smartClient2, _ := New(coord, "rg1", options)

	//	make sure it registers
	fixtures.WaitUntilOrFail(t, func() bool {
		return len(rg1.GetClients()) == 2
	})

	//	assert this message wasn't passed at all
	fixtures.WaitUntilOrFail(t, func() bool {
		return smartClient.numSkippedRebuildTriggers == 1
	})
	assert.Equal(t, 1, smartClient.numCacheRebuildTriggers)

	smartClient.Stop()
	smartClient2.Stop()

	close1()
	fixtures.TeardownZookeeper(cluster, client)

}

func TestDeadHost(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()

	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, _ := coord.AddRingGroup(ctx, "rg1")
	ring1, _ := rg1.AddRing(ctx, iface.RingID(0))

	host0, _ := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	host0.SetState(ctx, iface.HOST_OFFLINE)

	coord2, err := zk_coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)

	assert.Nil(t, err)
	assert.NotNil(t, coord2)

	hosts := coord2.GetRingGroup("rg1").GetRing(0).GetHosts(ctx)

	assert.Equal(t, iface.HOST_OFFLINE, hosts[0].GetState())
	assert.Equal(t, 1, len(hosts))

	fixtures.TeardownZookeeper(cluster, client)
}

func TestDeletedHost(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg0, _ := coord.AddRingGroup(ctx, "rg0")

	serverValues := make(map[string]string)
	serverValues["key1"] = "value1"

	ring0, _ := rg0.AddRing(ctx, iface.RingID(0))
	host0, _ := ring0.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	handler0 := thrift_services.NewPartitionServerHandler(serverValues)
	close0 := createServer(t, ctx, host0, handler0)
	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	ring1, _ := rg0.AddRing(ctx, iface.RingID(1))
	host1, _ := ring1.AddHost(ctx, "localhost", 12346, []string{})
	host1Domain, _ := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	handler1 := thrift_services.NewPartitionServerHandler(serverValues)
	close1 := createServer(t, ctx, host1, handler1)
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)

	//	create client
	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	newClient, _ := New(coord, "rg0", options)

	val, _ := newClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value1"), val.Value)

	fixtures.WaitUntilOrFail(t, func() bool {
		return newClient.numCacheRebuildTriggers == 0
	})

	ring1.RemoveHost(ctx, "localhost", 12346)

	time.Sleep(time.Second * 5)

	fixtures.WaitUntilOrFail(t, func() bool {
		return newClient.numCacheRebuildTriggers > 0 &&
			newClient.numSuccessfulCacheRefreshes > 0
	})

	val, _ = newClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value1"), val.Value)

	close0()
	close1()

	fixtures.TeardownZookeeper(cluster, client)
}

func initializeCoordinator(client curator.CuratorFramework) (*zk_coordinator.ZkCoordinator, error) {
	return zk_coordinator.InitializeZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)
}

func TestUnresponsiveHost(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx := thriftext.NewThreadCtx()

	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg0, _ := coord.AddRingGroup(ctx, "rg0")

	serverValues := make(map[string]string)
	serverValues["key1"] = "value1"

	ring0, _ := rg0.AddRing(ctx, iface.RingID(0))
	host0, _ := ring0.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	handler0 := thrift_services.NewPartitionServerHandler(serverValues)
	close0 := createServer(t, ctx, host0, handler0)
	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	ring1, _ := rg0.AddRing(ctx, iface.RingID(1))
	host1, _ := ring1.AddHost(ctx, "localhost", 12346, []string{})
	host1Domain, _ := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	//	second server is created but not responding to requests
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)

	//	create client

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	smartClient, _ := New(coord, "rg0", options)

	val, _ := smartClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value1"), val.Value)

	close0()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestAddRemoveHost(t *testing.T){
	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg0, _ := coord.AddRingGroup(ctx, "rg0")

	serverValues := make(map[string]string)
	serverValues["key1"] = "value1"

	ring0, _ := rg0.AddRing(ctx, iface.RingID(0))
	host0, _ := ring0.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	handler0 := thrift_services.NewPartitionServerHandler(serverValues)
	close0 := createServer(t, ctx, host0, handler0)
	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(1).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	smartClient, _ := New(coord, "rg0", options)

	//	verify we're up
	val, _ := smartClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value1"), val.Value)

	//	add a ring
	ring1, _ := rg0.AddRing(ctx, iface.RingID(1))
	host1, _ := ring1.AddHost(ctx, "localhost", 12346, []string{})
	host1Domain, _ := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	//	add a host w/ different data
	server1Values := make(map[string]string)
	server1Values["key1"] = "value2"

	handler1 := thrift_services.NewPartitionServerHandler(server1Values)
	close1 := createServer(t, ctx, host1, handler1)
	setStateBlocking(t, host1, ctx, iface.HOST_SERVING)

	//	remove old host
	ring0.RemoveHost(ctx, "localhost", 12345)

	//	verify new result is new data
	val, _ = smartClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value2"), val.Value)

	close0()
	close1()

	fixtures.TeardownZookeeper(cluster, client)

}

func TestOfflineHost(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg0, _ := coord.AddRingGroup(ctx, "rg0")

	serverValues := make(map[string]string)
	serverValues["key1"] = "value1"

	ring0, _ := rg0.AddRing(ctx, iface.RingID(0))
	host0, _ := ring0.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	handler0 := thrift_services.NewPartitionServerHandler(serverValues)
	close0 := createServer(t, ctx, host0, handler0)
	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	ring1, _ := rg0.AddRing(ctx, iface.RingID(1))
	host1, _ := ring1.AddHost(ctx, "localhost", 12346, []string{})
	host1Domain, _ := host1.AddDomain(ctx, domain0)
	host1Domain.AddPartition(ctx, iface.PartitionID(0))

	//	second server is but offline
	setStateBlocking(t, host1, ctx, iface.HOST_OFFLINE)

	//	create client
	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(1).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	smartClient, _ := New(coord, "rg0", options)

	val, _ := smartClient.get(domain0, []byte("key1"))
	assert.Equal(t, []byte("value1"), val.Value)

	close0()

	fixtures.TeardownZookeeper(cluster, client)
}



//	TODO host is offline / has no status.  new clients still start up.

//	verify that the client fails fast when it's not able to connect to enough partition servers during creation.
//	relies on SetMinConnectionsPerPartition being set in the options
func TestFailConnect(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, _ := coord.AddRingGroup(ctx, "rg1")
	ring1, _ := rg1.AddRing(ctx, iface.RingID(0))

	host0, _ := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	fixtures.WaitUntilOrFail(t, func() bool {
		return len(coord.GetRingGroup("rg1").GetRing(0).GetHosts(ctx)[0].GetHostDomain(ctx, 0).GetPartitions()) == 1
	})

	//	this should not succeed because there is no partition server
	_, err := New(coord, "rg1", options)

	assert.Equal(t, "Could not establish 1 connections to partition 0 for domain 0", err.Error())

	server1Values := make(map[string]string)
	server1Values["key1"] = "value1"

	handler := thrift_services.NewPartitionServerHandler(server1Values)
	close1 := createServer(t, ctx, host0, handler)

	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	fixtures.WaitUntilOrFail(t, func() bool {

		smartClient, err := New(coord, "rg1", options)

		if err != nil {
			return false
		}

		response, err := smartClient.Get(domain0.GetName(), []byte("key1"))
		smartClient.Stop()

		if response.Xception != nil {
			return false
		}

		if response.Value != nil {
			return "value1" == string(response.Value)
		}

		return false

	})

	close1()

	fixtures.TeardownZookeeper(cluster, client)
}

//	test that we skip rebuilding the connection cache if we aren't able to get the connections we need for
//	MinConnectionsPerPartition.  in this case it is caused by a ZooKeeper disconnect -- verify that losing
//	ZooKeeper doesn't cause us to drop our live connections
func TestSkipConnectionCacheRebuild(t *testing.T) {

	cluster, client := fixtures.SetupZookeeper(t)
	ctx := thriftext.NewThreadCtx()
	coord, _ := initializeCoordinator(client)

	domain0, _ := coord.AddDomain(ctx, "existent_domain", 1, "", "", "com.liveramp.hank.partitioner.Murmur64Partitioner", []string{})

	rg1, _ := coord.AddRingGroup(ctx, "rg1")
	ring1, _ := rg1.AddRing(ctx, iface.RingID(0))

	host0, _ := ring1.AddHost(ctx, "localhost", 12345, []string{})
	host0Domain, _ := host0.AddDomain(ctx, domain0)
	host0Domain.AddPartition(ctx, iface.PartitionID(0))

	options := NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2).
		SetQueryTimeoutMs(100).
		SetMinConnectionsPerPartition(1)

	fixtures.WaitUntilOrFail(t, func() bool {
		hosts := coord.GetRingGroup("rg1").GetRing(0).GetHosts(ctx)
		if len(hosts) == 0{
			return false
		}

		return len(hosts[0].GetHostDomain(ctx, 0).GetPartitions()) == 1
	})

	server1Values := make(map[string]string)
	server1Values["key1"] = "value1"

	handler := thrift_services.NewPartitionServerHandler(server1Values)
	close1 := createServer(t, ctx, host0, handler)

	setStateBlocking(t, host0, ctx, iface.HOST_SERVING)

	//	verify that we can do a basic query
	fixtures.WaitUntilOrFail(t, queryMatches("key1", "value1", "rg1", domain0.GetName(), coord, options))

	smartClient, _ := New(coord, "rg1", options)

	//	verify we can query using this client too
	resp, _ := smartClient.Get(domain0.GetName(), []byte("key1"))
	assert.Equal(t, "value1", string(resp.Value))

	//	kill zookeeper
	fixtures.TeardownZookeeper(cluster, client)

	//	no seriously, zookeeper is dead
	fixtures.WaitUntilOrFail(t, func() bool {
		_, err := client.CheckExists().ForPath("/hank")
		return err != nil
	})

	time.Sleep(5 * time.Second)

	//	should happen automatically, but force it to just for good measure
	smartClient.updateConnectionCache(ctx)

	//	verify we can still query the record because we didn't disconnect from the server
	resp, _ = smartClient.Get(domain0.GetName(), []byte("key1"))
	assert.Equal(t, "value1", string(resp.Value))

	close1()

}

func queryMatches(key string, value string, rgName string, domainName string, coord *zk_coordinator.ZkCoordinator, options *hankSmartClientOptions) func() bool {
	return func() bool {
		smartClient, err := New(coord, rgName, options)

		if err != nil {
			return false
		}

		response, err := smartClient.Get(domainName, []byte(key))
		smartClient.Stop()

		if response.Xception != nil {
			return false
		}

		if response.Value != nil {
			return value == string(response.Value)
		}

		return false

	}
}

func setStateBlocking(t *testing.T, host iface.Host, ctx *thriftext.ThreadCtx, state iface.HostState) {
	host.SetState(ctx, state)
	fixtures.WaitUntilOrFail(t, func() bool {
		return host.GetState() == state
	})
}
