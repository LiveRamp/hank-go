package iface

import (
	"strconv"

	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"

	"github.com/LiveRamp/hank-go-client/thriftext"
)

/*
Interfaces are all a subset of the interfaces implemented in the Java client.
Should be fleshed out as necessary for use or testing.
*/

type Coordinator interface {
	GetRingGroup(ringGroupName string) RingGroup

	AddDomainGroup(ctx *thriftext.ThreadCtx, name string) (DomainGroup, error)

	GetDomainGroup(domainGroupName string) DomainGroup

	GetRingGroups() []RingGroup

	GetDomainById(ctx *thriftext.ThreadCtx, domainId DomainID) (Domain, error)

	AddDomain(ctx *thriftext.ThreadCtx,
		domainName string,
		numParts int32,
		storageEngineFactoryName string,
		storageEngineOptions string,
		partitionerName string,
		requiredHostFlags []string,
	) (Domain, error)

	GetDomain(domain string) Domain
}

type DomainGroup interface {
	GetName() string

	SetDomainVersions(ctx *thriftext.ThreadCtx, flags map[DomainID]VersionID) error

	GetDomainVersions(ctx *thriftext.ThreadCtx) []*DomainAndVersion

	GetDomainVersion(domainID DomainID) *DomainAndVersion
}

type Ring interface {
	AddHost(ctx *thriftext.ThreadCtx, hostName string, port int, hostFlags []string) (Host, error)

	GetHosts(ctx *thriftext.ThreadCtx) []Host

	GetNum() RingID

	RemoveHost(ctx *thriftext.ThreadCtx, hostName string, port int) bool
}

type RingGroup interface {
	GetName() string

	GetRings() []Ring

	AddRing(ctx *thriftext.ThreadCtx, ringNum RingID) (Ring, error)

	GetRing(ringNum RingID) Ring

	RegisterClient(ctx *thriftext.ThreadCtx, metadata *hank.ClientMetadata) (id string, err error)

	DeregisterClient(ctx *thriftext.ThreadCtx, id string) error

	GetClients() []*hank.ClientMetadata

	AddListener(listener thriftext.DataChangeNotifier)
}

type HostState string

const (
	HOST_IDLE     HostState = "IDLE"
	HOST_SERVING  HostState = "SERVING"
	HOST_UPDATING HostState = "UPDATING"
	HOST_OFFLINE  HostState = "OFFLINE"
)

type Host interface {
	GetMetadata(ctx *thriftext.ThreadCtx) *hank.HostMetadata

	GetAssignedDomains(ctx *thriftext.ThreadCtx) []HostDomain

	GetEnvironmentFlags(ctx *thriftext.ThreadCtx) map[string]string

	SetEnvironmentFlags(ctx *thriftext.ThreadCtx, flags map[string]string) error

	AddDomain(ctx *thriftext.ThreadCtx, domain Domain) (HostDomain, error)

	GetAddress() *PartitionServerAddress

	GetHostDomain(ctx *thriftext.ThreadCtx, domainId DomainID) HostDomain

	AddStateChangeListener(listener thriftext.DataListener)

	SetState(ctx *thriftext.ThreadCtx, state HostState) error

	GetState() HostState

	GetID() string

	GetPath() string

	Delete()
}

type Partitioner interface {
	Partition(key []byte, numPartitions int32) int32
}

type Domain interface {
	GetName() string

	GetId() DomainID

	GetPartitioner() Partitioner

	GetNumParts() int32

	GetPath() string
}

type HostDomainPartition interface {
	GetPartitionNumber() PartitionID

	GetCurrentDomainVersion() VersionID

	SetCurrentDomainVersion(ctx *thriftext.ThreadCtx, version VersionID) error

	IsDeletable() bool
}

type HostDomain interface {
	GetDomain(ctx *thriftext.ThreadCtx, coordinator Coordinator) (Domain, error)

	AddPartition(ctx *thriftext.ThreadCtx, partNum PartitionID) HostDomainPartition

	GetPartitions() []HostDomainPartition
}

type PartitionServerAddress struct {
	HostName   string
	PortNumber int32
}

func (p *PartitionServerAddress) Print() string {
	return p.HostName + ":" + strconv.Itoa(int(p.PortNumber))
}

type DomainAndVersion struct {
	DomainID  DomainID
	VersionID VersionID
}
