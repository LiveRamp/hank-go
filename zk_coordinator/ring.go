package zk_coordinator

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/curator-go/curator"
	"path"
	"regexp"
	"strconv"
	"github.com/bpodgursky/hank-go-client/curatorext"
)

var RING_REGEX = regexp.MustCompile("ring-([0-9]+)")

const HOSTS_PATH_SEGMENT string = "hosts"

type ZkRing struct {
	root   string
	num    iface.RingID
	client curator.CuratorFramework

	hosts    *curatorext.ZkWatchedMap
	listener iface.DataChangeNotifier
}

func loadZkRing(ctx *iface.ThreadCtx, client curator.CuratorFramework, listener iface.DataChangeNotifier, root string) (interface{}, error) {
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

func createZkRing(ctx *iface.ThreadCtx, root string, num iface.RingID, listener iface.DataChangeNotifier, client curator.CuratorFramework) (*ZkRing, error) {
	curatorext.CreateWithParents(client, curator.PERSISTENT, root, nil)

	fmt.Println("Created via creation")

	hosts, err := curatorext.NewZkWatchedMap(client, path.Join(root, HOSTS_PATH_SEGMENT), listener, loadZkHost)
	if err != nil {
		return nil, err
	}

	return &ZkRing{root, num, client, hosts, listener}, nil
}

//  public methods

func (p *ZkRing) AddHost(ctx *iface.ThreadCtx, hostName string, port int, hostFlags []string) (iface.Host, error) {

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

func (p *ZkRing) GetHosts(ctx *iface.ThreadCtx) []iface.Host {

	hosts := []iface.Host{}
	for _, item := range p.hosts.Values() {
		i := item.(iface.Host)
		hosts = append(hosts, i)
	}

	return hosts
}
