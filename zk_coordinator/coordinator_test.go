package zk_coordinator

import (
	"fmt"
	"github.com/curator-go/curator"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/iface"
)

func TestZkCoordinator(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	zkCoordinator, err1 := createCoordinator(client)
	zkCoordinator3, err2 := createCoordinator(client)

	ctx := iface.NewThreadCtx()

	if err1 != nil {
		assert.Fail(t, "Error initializing coordinator 1")
	}

	if err2 != nil {
		assert.Fail(t, "Error initializing coordinator 2")
	}

	dg1, createError := zkCoordinator.AddDomainGroup(ctx, "group1")

	if createError != nil {
		assert.Fail(t, "Error adding domain group")
	}

	//  check the name
	group := zkCoordinator.GetDomainGroup("group1")
	assert.Equal(t, "group1", group.GetName())

	//  make sure this one picked up the message
	fixtures.WaitUntilOrFail(t, func() bool {
		fmt.Println(zkCoordinator3)
		domainGroup := zkCoordinator3.GetDomainGroup("group1")
		return domainGroup != nil
	})

	//  can't create a second one
	_, err := zkCoordinator.AddDomainGroup(ctx, "group1")
	if err == nil {
		assert.Fail(t, "Should have thrown an error")
	}

	//  get the same thing with a fresh coordinator
	zkCoordinator2, _ := createCoordinator(client)
	dg1coord2 := zkCoordinator2.GetDomainGroup("group1")
	assert.Equal(t, "group1", dg1coord2.GetName())

	//  verify that rg/rings show up in other coordinators

	rg1Coord1, _ := zkCoordinator.AddRingGroup(ctx, "rg1")

	var rg1Coord2 iface.RingGroup
	fixtures.WaitUntilOrFail(t, func() bool {
		rg1Coord2 = zkCoordinator2.GetRingGroup("rg1")
		return rg1Coord2 != nil
	})

	ringCoord1, _ := rg1Coord1.AddRing(ctx, 0)

	var ringCoord2 iface.Ring
	fixtures.WaitUntilOrFail(t, func() bool {
		ringCoord2 = rg1Coord2.GetRing(0)
		return ringCoord2 != nil
	})

	host1Coord1, _ := ringCoord1.AddHost(ctx, "127.0.0.1", 54321, []string{})

	var hostsCoord2 []iface.Host
	fixtures.WaitUntilOrFail(t, func() bool {
		hostsCoord2 = ringCoord2.GetHosts(ctx)
		return len(hostsCoord2) == 1
	})

	var host1Coord2 = hostsCoord2[0]

	fixtures.WaitUntilOrFail(t, func() bool {
		return host1Coord2.GetMetadata(ctx).HostName == "127.0.0.1"
	})

	flags := make(map[string]string)
	flags["flag1"] = "value1"
	host1Coord1.SetEnvironmentFlags(ctx, flags)

	fixtures.WaitUntilOrFail(t, func() bool {
		return reflect.DeepEqual(host1Coord2.GetEnvironmentFlags(ctx), flags)
	})

	fmt.Println(host1Coord1)

	domain1, _ := zkCoordinator.AddDomain(ctx, "domain0", 1, "", "", "", []string{})
	zkCoordinator.AddDomain(ctx, "domain1", 1, "", "", "", []string{})

	fixtures.WaitUntilOrFail(t, func() bool {
		domain, _ := zkCoordinator2.GetDomainById(ctx, 0)
		return domain != nil && domain.GetName() == "domain0"
	})

	fixtures.WaitUntilOrFail(t, func() bool {
		domain, _ := zkCoordinator2.GetDomainById(ctx, 1)
		return domain != nil && domain.GetName() == "domain1"
	})

	hostDomain, err := host1Coord1.AddDomain(ctx, domain1)
	hostDomain.AddPartition(ctx, 0)

	fixtures.WaitUntilOrFail(t, func() bool {
		hostDomain := host1Coord2.GetHostDomain(ctx, iface.DomainID(0))

		if hostDomain == nil {
			return false
		}

		assignedPartitions := hostDomain.GetPartitions()
		if len(assignedPartitions) != 1 {
			return false
		}

		return assignedPartitions[0].GetPartitionNumber() == 0

	})

	dg1.SetDomainVersions(ctx, map[iface.DomainID]iface.VersionID{0: 0})

	fixtures.WaitUntilOrFail(t, func() bool {
		version := dg1coord2.GetDomainVersion(iface.DomainID(0))

		if version == nil {
			return false
		}

		return version.VersionID == 0
	})

	//  let messages flush to make shutdown cleaner.  dunno a better way.
	time.Sleep(time.Second)

	fixtures.TeardownZookeeper(cluster, client)
}
func createCoordinator(client curator.CuratorFramework) (*ZkCoordinator, error) {
	return NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)
}
