package zk_coordinator

import (
	"path"
	"regexp"
	"strconv"

	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/curatorext"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/thriftext"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

const HOSTS_PATH_SEGMENT string = "hosts"

type ZkRing struct {
	root   string
	num    iface.RingID
	client curator.CuratorFramework

	hosts    *curatorext.ZkWatchedMap
	listener thriftext.DataChangeNotifier
}

func loadZkRing(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, listener thriftext.DataChangeNotifier, root string) (interface{}, error) {
	matches := RING_REGEX.FindStringSubmatch(path.Base(root))

	//  dumb design and rings are directly in the RG root, but can't change it here
	if matches != nil && len(matches) > 0 {

		num, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}

		hosts, err := curatorext.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
		if err != nil {
			return nil, err
		}

		return &ZkRing{root, iface.RingID(num), client, hosts, listener}, nil
	}

	return nil, nil
}

func createZkRing(ctx *thriftext.ThreadCtx, root string, num iface.RingID, listener thriftext.DataChangeNotifier, client curator.CuratorFramework) (*ZkRing, error) {
	curatorext.CreateWithParents(client, curator.PERSISTENT, root, nil)

	hosts, err := curatorext.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
	if err != nil {
		return nil, err
	}

	return &ZkRing{root, num, client, hosts, listener}, nil
}

//  public methods

func (p *ZkRing) AddHost(ctx *thriftext.ThreadCtx, hostName string, port int, hostFlags []string) (iface.Host, error) {

	host, err := CreateZkHost(ctx, p.client, p.listener, p.hosts.Root, hostName, port, hostFlags)
	if err != nil {
		return nil, err
	}

	err = p.hosts.WaitUntilContains(host.GetID())
	if err != nil {
		return nil, err
	}

	return host, err
}

func (p *ZkRing) GetHosts(ctx *thriftext.ThreadCtx) []iface.Host {

	hosts := []iface.Host{}
	for _, item := range p.hosts.Values() {
		i := item.(iface.Host)
		hosts = append(hosts, i)
	}

	return hosts
}
