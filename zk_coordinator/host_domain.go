package zk_coordinator

import (
	"github.com/LiveRamp/hank-go/iface"
	"github.com/LiveRamp/hank-go/thriftext"
)

type ZkHostDomain struct {
	host     *ZkHost
	domainId iface.DomainID
}

func newZkHostDomain(host *ZkHost, domainId iface.DomainID) *ZkHostDomain {
	return &ZkHostDomain{host: host, domainId: domainId}
}

func (p *ZkHostDomain) GetDomain(ctx *thriftext.ThreadCtx, coordinator iface.Coordinator) (iface.Domain, error) {
	return coordinator.GetDomainById(ctx, p.domainId)
}

func (p *ZkHostDomain) AddPartition(ctx *thriftext.ThreadCtx, partNum iface.PartitionID) iface.HostDomainPartition {
	return p.host.addPartition(ctx, p.domainId, partNum)
}

func (p *ZkHostDomain) GetPartitions() []iface.HostDomainPartition {
	return p.host.getPartitions(p.domainId)
}
