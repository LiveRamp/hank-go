package zk_coordinator

import (
	"path"
	"strconv"

	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"
	"github.com/curator-go/curator"

	"path/filepath"

	"github.com/LiveRamp/hank-go/curatorext"
	"github.com/LiveRamp/hank-go/iface"
	"github.com/LiveRamp/hank-go/thriftext"
	"github.com/pkg/errors"
)

const CLIENT_ROOT string = "c"
const CLIENT_NODE string = "c"

type ZkRingGroup struct {
	ringGroupPath string
	name          string
	client        curator.CuratorFramework

	clients *curatorext.ZkWatchedMap
	rings   *curatorext.ZkWatchedMap

	localNotifier *thriftext.MultiNotifier
}

func createZkRingGroup(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, name string, rootPath string) (iface.RingGroup, error) {
	rgRootPath := path.Join(rootPath, name)

	err := curatorext.AssertEmpty(client, rgRootPath)
	if err != nil {
		return nil, err
	}

	listener := thriftext.NewMultiNotifier()

	rings, err := curatorext.NewZkWatchedMap(client, true, rgRootPath, listener, loadZkRing)
	if err != nil {
		return nil, err
	}

	//	we are intentionally not notifying any listeners about new clients.  it's just noise.
	clients, err := curatorext.NewZkWatchedMap(client, true, path.Join(rgRootPath, CLIENT_ROOT), &thriftext.NoOp{}, loadClientMetadata)
	if err != nil {
		return nil, err
	}

	return &ZkRingGroup{ringGroupPath: rootPath, name: name, client: client, clients: clients, rings: rings, localNotifier: listener}, nil

}

func loadZkRingGroup(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, listener thriftext.DataChangeNotifier, rgRootPath string) (interface{}, error) {

	err := curatorext.AssertExists(client, rgRootPath)
	if err != nil {
		return nil, errors.Wrap(err, "Error asserting zk rg path exists")
	}

	multiListener := thriftext.NewMultiNotifier()
	multiListener.AddClient(listener)

	//	we are intentionally not notifying any listeners about new clients.  it's just noise.
	clients, err := curatorext.NewZkWatchedMap(client, false, path.Join(rgRootPath, CLIENT_ROOT), &thriftext.NoOp{}, loadClientMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "Error loading zk clients")
	}

	rings, err := curatorext.NewZkWatchedMap(client, false, rgRootPath, multiListener, loadZkRing)
	if err != nil {
		return nil, errors.Wrap(err, "Error loading zk ring")
	}

	return &ZkRingGroup{ringGroupPath: rgRootPath, client: client, clients: clients, rings: rings, localNotifier: multiListener}, nil
}

//  loader

func loadClientMetadata(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, listener thriftext.DataChangeNotifier, path string) (interface{}, error) {
	metadata := hank.NewClientMetadata()
	curatorext.LoadThrift(ctx, path, client, metadata)
	return metadata, nil
}

//  methods

func (p *ZkRingGroup) RegisterClient(ctx *thriftext.ThreadCtx, metadata *hank.ClientMetadata) (id string, err error) {
	path, err := ctx.SetThrift(curatorext.CreateEphemeralSequential(path.Join(p.clients.Root, CLIENT_NODE), p.client), metadata)

	if err != nil {
		return "", err
	}

	return filepath.Base(path), nil
}

func (p *ZkRingGroup) DeregisterClient(ctx *thriftext.ThreadCtx, id string) error {
	return p.client.Delete().ForPath(path.Join(p.clients.Root, id))
}

func (p *ZkRingGroup) GetName() string {
	return p.name
}

func (p *ZkRingGroup) AddListener(listener thriftext.DataChangeNotifier) {
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

func (p *ZkRingGroup) AddRing(ctx *thriftext.ThreadCtx, ringNum iface.RingID) (iface.Ring, error) {
	ringChild := ringName(ringNum)
	ringRoot := path.Join(p.rings.Root, ringChild)

	ring, err := createZkRing(ctx, ringRoot, ringNum, p.localNotifier, p.client)
	if err != nil {
		return nil, err
	}

	err = p.rings.WaitUntilContains(ringChild)
	if err != nil {
		return nil, err
	}

	return ring, nil
}

func (p *ZkRingGroup) GetRing(ringNum iface.RingID) iface.Ring {
	return iface.AsRing(p.rings.Get(ringName(ringNum)))
}

func (p *ZkRingGroup) GetRings() []iface.Ring {

	rings := []iface.Ring{}

	if p.rings != nil {
		for _, item := range p.rings.Values() {
			i := item.(iface.Ring)
			rings = append(rings, i)
		}
	}

	return rings
}
