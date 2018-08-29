package zk_coordinator

import (
	"path"

	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/curatorext"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/thriftext"
	"github.com/pkg/errors"
)

const KEY_DOMAIN_ID_COUNTER string = ".domain_id_counter"

type ZkCoordinator struct {
	ringGroups   *curatorext.ZkWatchedMap
	domainGroups *curatorext.ZkWatchedMap
	domains      *curatorext.ZkWatchedMap
	client       curator.CuratorFramework

	domainIDCounter *curatorext.ZkWatchedNode
}

func InitializeZkCoordinator(client curator.CuratorFramework,
	domainsRoot string,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error) {

	return newZkCoordinator(client, true, domainsRoot, ringGroupsRoot, domainGroupsRoot)
}

func NewZkCoordinator(client curator.CuratorFramework,
	domainsRoot string,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error){
	return newZkCoordinator(client, false, domainsRoot, ringGroupsRoot, domainGroupsRoot)
}

func newZkCoordinator(client curator.CuratorFramework,
	initialize bool,
	domainsRoot string,
	ringGroupsRoot string,
	domainGroupsRoot string) (*ZkCoordinator, error) {

	ringGroups, rgError := curatorext.NewZkWatchedMap(client, initialize, ringGroupsRoot, thriftext.NewMultiNotifier(), loadZkRingGroup)
	domainGroups, dgError := curatorext.NewZkWatchedMap(client, initialize, domainGroupsRoot, thriftext.NewMultiNotifier(), loadZkDomainGroup)
	domains, dmError := curatorext.NewZkWatchedMap(client, initialize, domainsRoot, thriftext.NewMultiNotifier(), loadZkDomain)

	if rgError != nil {
		return nil, errors.Wrap(rgError, "Error loading zk ring group")
	}

	if dgError != nil {
		return nil, errors.Wrap(dgError, "Error loading zk domain group")
	}

	if dmError != nil {
		return nil, errors.Wrap(dmError, "Error loading zk domains")
	}

	counter, error := getDomainIDCounter(client, path.Join(domainsRoot, KEY_DOMAIN_ID_COUNTER))
	if error != nil {
		return nil, errors.Wrap(error, "Error getting domain ID counter")
	}

	return &ZkCoordinator{
		ringGroups,
		domainGroups,
		domains,
		client,
		counter,
	}, nil

}

func getDomainIDCounter(client curator.CuratorFramework, path string) (*curatorext.ZkWatchedNode, error) {
	domainCount, err := client.CheckExists().ForPath(path)
	if err != nil {
		return nil, err
	}

	if domainCount != nil {
		return curatorext.LoadIntWatchedNode(client, path)
	} else {
		return curatorext.NewIntWatchedNode(client, curator.PERSISTENT, path, -1)
	}
}

func (p *ZkCoordinator) GetRingGroup(name string) iface.RingGroup {
	return iface.AsRingGroup(p.ringGroups.Get(name))
}

func (p *ZkCoordinator) GetRingGroups() []iface.RingGroup {

	groups := []iface.RingGroup{}
	for _, item := range p.ringGroups.Values() {
		i := item.(iface.RingGroup)
		groups = append(groups, i)
	}

	return groups

}

func (p *ZkCoordinator) GetDomainGroup(name string) iface.DomainGroup {
	return iface.AsDomainGroup(p.domainGroups.Get(name))
}

func (p *ZkCoordinator) AddDomainGroup(ctx *thriftext.ThreadCtx, name string) (iface.DomainGroup, error) {

	group, err := createZkDomainGroup(ctx, p.client, name, p.domainGroups.Root)
	if err != nil {
		return nil, err
	}

	err = p.domainGroups.WaitUntilContains(name)
	if err != nil {
		return nil, err
	}

	return group, nil

}

func (p *ZkCoordinator) AddRingGroup(ctx *thriftext.ThreadCtx, name string) (iface.RingGroup, error) {

	group, err := createZkRingGroup(ctx, p.client, name, p.ringGroups.Root)
	if err != nil {
		return nil, err
	}

	err = p.ringGroups.WaitUntilContains(name)
	if err != nil {
		return nil, err
	}

	return group, nil
}

func (p *ZkCoordinator) AddDomain(ctx *thriftext.ThreadCtx,
	domainName string,
	numParts int32,
	storageEngineFactoryName string,
	storageEngineOptions string,
	partitionerName string,
	requiredHostFlags []string) (iface.Domain, error) {

	id, err := p.getNextDomainID(ctx)
	if err != nil {
		return nil, err
	}

	domain, err := createZkDomain(ctx, path.Join(p.domains.Root, domainName), domainName, iface.DomainID(id), numParts,
		storageEngineFactoryName,
		storageEngineOptions,
		partitionerName,
		requiredHostFlags,
		p.client)

	if err != nil {
		return nil, err
	}

	err = p.domains.WaitUntilContains(domainName)
	if err != nil {
		return nil, err
	}

	return domain, nil
}

func (p *ZkCoordinator) getNextDomainID(ctx *thriftext.ThreadCtx) (iface.DomainID, error) {

	val, error := p.domainIDCounter.Update(ctx, func(val interface{}) interface{} {
		nextID := val.(int)
		return nextID + 1
	})

	if error != nil {
		return -1, error
	}

	return iface.DomainID(val.(int)), nil

}

func (p *ZkCoordinator) GetDomainById(ctx *thriftext.ThreadCtx, domainId iface.DomainID) (iface.Domain, error) {

	for _, inst := range p.domains.Values() {
		domain := inst.(iface.Domain)
		if domain.GetId() == domainId {
			return domain, nil
		}
	}

	return nil, nil
}

func (p *ZkCoordinator) GetDomain(domain string) iface.Domain {
	return iface.AsDomain(p.domains.Get(domain))
}
