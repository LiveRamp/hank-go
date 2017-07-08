package zk_coordinator

import (
  "github.com/bpodgursky/hank-go-client/iface"
)

type ZkHostDomainPartition struct {
  host            *ZkHost
  domainId        iface.DomainID
  partitionNumber iface.PartitionID
}

func newZkHostDomainPartition(host *ZkHost, domainId iface.DomainID, partitionNumber iface.PartitionID) *ZkHostDomainPartition {
  return &ZkHostDomainPartition{host, domainId, partitionNumber}
}

func (p *ZkHostDomainPartition) GetPartitionNumber() iface.PartitionID {
  return p.partitionNumber
}

func (p *ZkHostDomainPartition) GetCurrentDomainVersion() iface.VersionID {
  return p.host.getCurrentDomainGroupVersion(p.domainId, p.partitionNumber)
}

func (p *ZkHostDomainPartition) SetCurrentDomainVersion(ctx *iface.ThreadCtx, version iface.VersionID) error {
  return p.host.setCurrentDomainGroupVersion(ctx, p.domainId, p.partitionNumber, version)
}

func (p *ZkHostDomainPartition) IsDeletable() bool {
	return p.host.isDeletable(p.domainId, p.partitionNumber)
}