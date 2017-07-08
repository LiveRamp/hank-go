package zk_coordinator

import (
	"github.com/curator-go/curator"
	"path"
	"strconv"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/bpodgursky/hank-go-client/curatorext"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

const CLIENT_ROOT string = "c"
const CLIENT_NODE string = "c"

type ZkRingGroup struct {
	ringGroupPath string
	name          string
	client        curator.CuratorFramework

	clients *curatorext.ZkWatchedMap
	rings   *curatorext.ZkWatchedMap

	localNotifier *iface.MultiNotifier
}

func createZkRingGroup(ctx *iface.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (iface.RingGroup, error) {
	rgRootPath := path.Join(rootPath, name)

	err := curatorext.AssertEmpty(client, rgRootPath)
	if err != nil {
		return nil, err
	}

	curatorext.CreateWithParents(client, curator.PERSISTENT, rgRootPath, nil)

	listener := iface.NewMultiNotifier()

	clients, err := curatorext.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), listener, loadClientMetadata)
	if err != nil {
		return nil, err
	}

	rings, err := curatorext.NewZkWatchedMap(client, rgRootPath, listener, loadZkRing)
	if err != nil {
		return nil, err
	}

	return &ZkRingGroup{ringGroupPath: rootPath, name: name, client: client, clients: clients, rings: rings, localNotifier: listener}, nil

}

func loadZkRingGroup(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, rgRootPath string) (interface{}, error) {

	err := curatorext.AssertExists(client, rgRootPath)
	if err != nil {
		return nil, err
	}

	clients, err := curatorext.NewZkWatchedMap(client, path.Join(rgRootPath, CLIENT_ROOT), listener, loadClientMetadata)
	if err != nil {
		return nil, err
	}

	multiListener := iface.NewMultiNotifier()
	multiListener.AddClient(listener)

	rings, err := curatorext.NewZkWatchedMap(client, rgRootPath, multiListener, loadZkRing)

	return &ZkRingGroup{ringGroupPath: rgRootPath, client: client, clients: clients, rings: rings, localNotifier: multiListener}, nil
}

//  loader

func loadClientMetadata(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, path string) (interface{}, error) {
	metadata := hank.NewClientMetadata()
	curatorext.LoadThrift(ctx, path, client, metadata)
	return metadata, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(ctx *iface.ThreadCtx, metadata *hank.ClientMetadata) error {
	return ctx.SetThrift(curatorext.CreateEphemeralSequential(path.Join(p.clients.Root, CLIENT_NODE), p.client), metadata)
}

func (p *ZkRingGroup) GetName() string {
	return p.name
}

func (p *ZkRingGroup) AddListener(listener iface.DataChangeNotifier) {
	p.localNotifier.AddClient(listener)
}

func (p *ZkRingGroup) GetClients() []*hank.ClientMetadata {

	groups := []*hank.ClientMetadata{}
	for _, item := range p.clients.Values() {
		i := item.(*hank.ClientMetadata)
		groups = append(groups, i)
	}

	return groups
}

func ringName(ringNum iface.RingID) string {
	return "ring-" + strconv.Itoa(int(ringNum))
}

func (p *ZkRingGroup) AddRing(ctx *iface.ThreadCtx, ringNum iface.RingID) (iface.Ring, error) {
	ringChild := ringName(ringNum)
	ringRoot := path.Join(p.rings.Root, ringChild)

	ring, err := createZkRing(ctx, ringRoot, ringNum, p.localNotifier, p.client)
	if err != nil {
		return nil, err
	}

	err = p.rings.WaitUntilContains(ringChild)
	if err != nil{
		return nil, err
	}

	return ring, nil
}

func (p *ZkRingGroup) GetRing(ringNum iface.RingID) iface.Ring {
	return iface.AsRing(p.rings.Get(ringName(ringNum)))
}

func (p *ZkRingGroup) GetRings() []iface.Ring {

	rings := []iface.Ring{}
	for _, item := range p.rings.Values() {
		i := item.(iface.Ring)
		rings = append(rings, i)
	}

	return rings
}
