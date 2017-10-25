package zk_coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/curatorext"
	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"
	"github.com/LiveRamp/hank-go-client/thriftext"
)

type ZkDomainGroup struct {
	name     string
	metadata *curatorext.ZkWatchedNode
}

func createZkDomainGroup(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (*ZkDomainGroup, error) {

	metadataPath := path.Join(rootPath, name)

	err := curatorext.AssertEmpty(client, metadataPath)
	if err != nil {
		return nil, err
	}

	metadata := hank.NewDomainGroupMetadata()
	metadata.DomainVersions = make(map[int32]int32)

	node, nodeErr := curatorext.NewThriftWatchedNode(
		client,
		curator.PERSISTENT,
		metadataPath,
		ctx,
		iface.NewDomainGroupMetadata,
		metadata,
	)

	if nodeErr != nil {
		return nil, nodeErr
	}

	return &ZkDomainGroup{name: name, metadata: node}, nil
}

func loadZkDomainGroup(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, listener thriftext.DataChangeNotifier, fullPath string) (interface{}, error) {

	name := path.Base(fullPath)

	err := curatorext.AssertExists(client, fullPath)
	if err != nil {
		return nil, err
	}

	node, nodeErr := curatorext.LoadThriftWatchedNode(client, fullPath, iface.NewDomainGroupMetadata)
	if nodeErr != nil {
		return nil, nodeErr
	}

	return &ZkDomainGroup{name: name, metadata: node}, nil
}

//  public stuff

func (p *ZkDomainGroup) GetName() string {
	return p.name
}

func (p *ZkDomainGroup) GetDomainVersions(ctx *thriftext.ThreadCtx) []*iface.DomainAndVersion {
	metadata := iface.AsDomainGroupMetadata(p.metadata.Get())

	versions := []*iface.DomainAndVersion{}
	for domainID, version := range metadata.DomainVersions {
		versions = append(versions, &iface.DomainAndVersion{DomainID: iface.DomainID(domainID), VersionID: iface.VersionID(version)})
	}
	return versions
}

func (p *ZkDomainGroup) SetDomainVersions(ctx *thriftext.ThreadCtx, versions map[iface.DomainID]iface.VersionID) error {

	_, err := p.metadata.Update(ctx, func(val interface{}) interface{} {
		metadata := iface.AsDomainGroupMetadata(val)

		for key, val := range versions {
			metadata.DomainVersions[int32(key)] = int32(val)
		}
		return metadata
	})

	return err
}

func (p *ZkDomainGroup) GetDomainVersion(domain iface.DomainID) *iface.DomainAndVersion {

	version, ok := iface.AsDomainGroupMetadata(p.metadata.Get()).DomainVersions[int32(domain)]
	if !ok {
		return nil
	}

	return &iface.DomainAndVersion{DomainID: domain, VersionID: iface.VersionID(version)}
}
