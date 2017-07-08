package zk_coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"strings"
	"github.com/bpodgursky/hank-go-client/iface"
	"fmt"
	"github.com/satori/go.uuid"
	"math/big"
	"strconv"
	"github.com/bpodgursky/hank-go-client/curatorext"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

const ASSIGNMENTS_PATH string = "a"
const STATE_PATH string = "s"

type ZkHost struct {
	path string

	metadata           *curatorext.ZkWatchedNode
	assignedPartitions *curatorext.ZkWatchedNode
	state              *curatorext.ZkWatchedNode

	listener iface.DataChangeNotifier
}

func CreateZkHost(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, basePath string, hostName string, port int, flags []string) (iface.Host, error) {

	uuid := uuid.NewV4().Bytes()
	last := uuid[len(uuid)-8:]

	var number big.Int
	number.SetBytes(last)
	rootPath := path.Join(basePath, strconv.FormatInt(number.Int64(), 10))

	metadata := hank.NewHostMetadata()
	metadata.HostName = hostName
	metadata.PortNumber = int32(port)
	metadata.Flags = strings.Join(flags, ",")

	node, err := curatorext.NewThriftWatchedNode(client, curator.PERSISTENT, rootPath, ctx, iface.NewHostMetadata, metadata)
	if err != nil {
		fmt.Println("Error creating host node at path: ", rootPath, err)
		return nil, err
	}

	adapter := &iface.Adapter{Notifier: listener}
	node.AddListener(adapter)

	assignmentMetadata := hank.NewHostAssignmentsMetadata()
	assignmentMetadata.Domains = make(map[int32]*hank.HostDomainMetadata)

	assignmentsRoot := assignmentsRoot(rootPath)
	partitionAssignments, err := curatorext.NewThriftWatchedNode(client,
		curator.PERSISTENT,
		assignmentsRoot,
		ctx,
		iface.NewHostAssignmentMetadata,
		assignmentMetadata)
	if err != nil {
		fmt.Println("Error creating assignments node at path: ", assignmentsRoot, err)
		return nil, err
	}

	statePath := path.Join(rootPath, STATE_PATH)
	state, err := curatorext.NewStringWatchedNode(client,
		curator.EPHEMERAL,
		statePath,
		string(iface.HOST_OFFLINE))
	state.AddListener(adapter)


	if err != nil {
		fmt.Println("Error creating state node at path: ", statePath, err)
		return nil, err
	}

	return &ZkHost{rootPath, node, partitionAssignments, state, listener}, nil
}

func loadZkHost(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, rootPath string) (interface{}, error) {

	node, err := curatorext.LoadThriftWatchedNode(client, rootPath, iface.NewHostMetadata)
	if err != nil {
		return nil, err
	}

	adapter := &iface.Adapter{Notifier: listener}
	node.AddListener(adapter)

	assignments, err := curatorext.LoadThriftWatchedNode(client, assignmentsRoot(rootPath), iface.NewHostAssignmentMetadata)
	if err != nil {
		return nil, err
	}
	assignments.AddListener(adapter)

	state, err := curatorext.LoadStringWatchedNode(client,
		path.Join(rootPath, STATE_PATH))
	if err != nil {
		return nil, err
	}
	state.AddListener(adapter)

	return &ZkHost{rootPath, node, assignments, state, listener}, nil
}


func assignmentsRoot(rootPath string) string {
	return path.Join(rootPath, ASSIGNMENTS_PATH)
}


func (p *ZkHost) addPartition(ctx *iface.ThreadCtx, domainId iface.DomainID, partNum iface.PartitionID) iface.HostDomainPartition {

	p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)

		if _, ok := metadata.Domains[int32(domainId)]; !ok {
			metadata.Domains[int32(domainId)] = hank.NewHostDomainMetadata()
		}

		partitionMetadata := hank.NewHostDomainPartitionMetadata()
		partitionMetadata.Deletable = false

		metadata.Domains[int32(domainId)].Partitions[int32(partNum)] = partitionMetadata
		return metadata
	})

	return newZkHostDomainPartition(p, domainId, partNum)
}

//  for zk impls

func (p *ZkHost) getCurrentDomainGroupVersion(domainId iface.DomainID, partitionNumber iface.PartitionID) iface.VersionID {

	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]
	if domainMetadata == nil {
		return iface.NO_VERSION
	}

	partitionMetadata := domainMetadata.Partitions[int32(partitionNumber)]
	if partitionMetadata == nil || partitionMetadata.CurrentVersionNumber == nil {
		return iface.NO_VERSION
	}

	return iface.VersionID(*partitionMetadata.CurrentVersionNumber)
}

func (p *ZkHost) isDeletable(domainId iface.DomainID, partitionNumber iface.PartitionID) bool {

	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]
	if domainMetadata == nil {
		return false
	}

	partitionMetadata := domainMetadata.Partitions[int32(partitionNumber)]
	if partitionMetadata == nil {
		return false
	}

	return partitionMetadata.Deletable
}

func (p *ZkHost) setCurrentDomainGroupVersion(ctx *iface.ThreadCtx, domainId iface.DomainID, partitionNumber iface.PartitionID, version iface.VersionID) error {

	_, err := p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)
		ensureDomain(metadata, domainId)

		partitionMetadata := hank.NewHostDomainPartitionMetadata()
		thisVariableExistsBecauseGoIsAStupidLanguage := int32(version)
		partitionMetadata.CurrentVersionNumber = &thisVariableExistsBecauseGoIsAStupidLanguage
		partitionMetadata.Deletable = false

		metadata.Domains[int32(domainId)].Partitions[int32(partitionNumber)] = partitionMetadata

		return metadata
	})

	return err
}

func (p *ZkHost) getPartitions(domainId iface.DomainID) []iface.HostDomainPartition {
	domainMetadata := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get()).Domains[int32(domainId)]

	var values []iface.HostDomainPartition
	for key := range domainMetadata.Partitions {
		values = append(values, newZkHostDomainPartition(p, domainId, iface.PartitionID(key)))
	}

	return values
}

//  public

func (p  *ZkHost) GetID() string {
	return path.Base(p.path)
}

func (p *ZkHost) AddStateChangeListener(listener iface.DataListener) {
	p.state.AddListener(listener)
}

func (p *ZkHost) GetMetadata(ctx *iface.ThreadCtx) *hank.HostMetadata {
	return iface.AsHostMetadata(p.metadata.Get())
}

func (p *ZkHost) GetAssignedDomains(ctx *iface.ThreadCtx) []iface.HostDomain {
	assignedDomains := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get())

	hostDomains := []iface.HostDomain{}
	for domainId := range assignedDomains.Domains {
		hostDomains = append(hostDomains, newZkHostDomain(p, iface.DomainID(domainId)))
	}

	return hostDomains
}

func (p *ZkHost) GetEnvironmentFlags(ctx *iface.ThreadCtx) map[string]string {
	return iface.AsHostMetadata(p.metadata.Get()).EnvironmentFlags
}

func (p *ZkHost) SetEnvironmentFlags(ctx *iface.ThreadCtx, flags map[string]string) error {

	_, err := p.metadata.Update(ctx, func(val interface{}) interface{} {
		metadata := iface.AsHostMetadata(val)
		metadata.EnvironmentFlags = flags
		return metadata
	})

	return err
}

func (p *ZkHost) SetState(ctx *iface.ThreadCtx, state iface.HostState) error {
	return p.state.Set(ctx, string(state))
}

func (p *ZkHost) GetState() iface.HostState {
	return iface.HostState(p.state.Get().(string))
}

func (p *ZkHost) AddDomain(ctx *iface.ThreadCtx, domain iface.Domain) (iface.HostDomain, error) {
	domainId := domain.GetId()

	_, err := p.assignedPartitions.Update(ctx, func(orig interface{}) interface{} {
		metadata := iface.AsHostAssignmentsMetadata(orig)
		ensureDomain(metadata, domainId)
		return metadata
	})

	if err != nil {
		return nil, err
	}

	p.listener.OnChange()

	return newZkHostDomain(p, domainId), nil
}
func ensureDomain(metadata *hank.HostAssignmentsMetadata, domainId iface.DomainID) {
	if _, ok := metadata.Domains[int32(domainId)]; !ok {
		domainMetadata := hank.NewHostDomainMetadata()
		domainMetadata.Partitions = make(map[int32]*hank.HostDomainPartitionMetadata)

		metadata.Domains[int32(domainId)] = domainMetadata
	}
}

func (p *ZkHost) GetAddress() *iface.PartitionServerAddress {
	metadata := iface.AsHostMetadata(p.metadata.Get())
	return &iface.PartitionServerAddress{HostName: metadata.HostName, PortNumber: metadata.PortNumber}
}

func (p *ZkHost) GetHostDomain(ctx *iface.ThreadCtx, domainId iface.DomainID) iface.HostDomain {

	assignedDomains := iface.AsHostAssignmentsMetadata(p.assignedPartitions.Get())
	metadata := assignedDomains.Domains[int32(domainId)]

	if metadata == nil {
		return nil
	}

	return newZkHostDomain(p, domainId)

}
