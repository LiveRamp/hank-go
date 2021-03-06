package curatorext

import (
	"path"

	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"

	"github.com/LiveRamp/hank-go/thriftext"

	log "github.com/sirupsen/logrus"

	"sync"

	"fmt"

	"github.com/pkg/errors"
)

type Loader func(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, listener thriftext.DataChangeNotifier, path string) (interface{}, error)

type ZkWatchedMap struct {
	Root string

	node         *cache.TreeCache
	client       curator.CuratorFramework
	loader       Loader
	internalData map[string]interface{}
	listener     []thriftext.DataChangeNotifier
}

type ChildLoader struct {
	internalData map[string]interface{}
	loader       Loader
	root         string
	listener     thriftext.DataChangeNotifier
	lock         *sync.Mutex

	ctx *thriftext.ThreadCtx
}

func (p *ChildLoader) ChildEvent(client curator.CuratorFramework, event cache.TreeCacheEvent) error {

	p.lock.Lock()
	defer p.lock.Unlock()

	switch event.Type {
	case cache.TreeCacheEventNodeUpdated:
		fallthrough
	case cache.TreeCacheEventNodeAdded:
		fullChildPath := event.Data.Path()

		if IsSubdirectory(p.root, fullChildPath) {
			err := conditionalInsert(p.ctx, client, p.loader, p.listener, p.internalData, fullChildPath)
			p.listener.OnChange(fullChildPath)
			if err != nil {
				//	log here because it's called by zk events and doesn't ever percolate up to the user
				log.WithError(err).WithField("child", fullChildPath).WithField("root", p.root).Error("Error inserting child")
				return err
			}
		}
	case cache.TreeCacheEventNodeRemoved:
		fullChildPath := event.Data.Path()
		delete(p.internalData, path.Base(fullChildPath))
		p.listener.OnChange(fullChildPath)
	}

	return nil
}

func conditionalInsert(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, loader Loader, listener thriftext.DataChangeNotifier, internalData map[string]interface{}, fullChildPath string) error {

	newKey := path.Base(fullChildPath)

	item, err := loader(ctx, client, listener, fullChildPath)
	if err != nil {
		return err
	}

	if item != nil {
		internalData[newKey] = item
	}

	return nil
}

func NewZkWatchedMap(
	client curator.CuratorFramework,
	initialize bool,
	root string,
	listener thriftext.DataChangeNotifier,
	loader Loader) (*ZkWatchedMap, error) {

	internalData := make(map[string]interface{})

	if initialize {

		err := CreateWithParents(client, curator.PERSISTENT, root, nil)
		if err != nil {
			return nil, err
		}

	} else {

		stat, err := client.CheckExists().ForPath(root)
		if err != nil {
			return nil, err
		}

		if stat == nil {
			return nil, errors.New(fmt.Sprintf("cannot load watched map because root %v does not exist", root))
		}
	}

	node := cache.NewTreeCache(client, root, cache.DefaultTreeCacheSelector).
		SetMaxDepth(1).
		SetCacheData(false)

	insertLock := &sync.Mutex{}

	node.Listenable().AddListener(&ChildLoader{
		internalData: internalData,
		loader:       loader,
		root:         root,
		ctx:          thriftext.NewThreadCtx(),
		listener:     listener,
		lock:         insertLock,
	})

	initialChildren, err := client.GetChildren().ForPath(root)
	if err != nil {
		return nil, err
	}

	ctx := thriftext.NewThreadCtx()

	for _, element := range initialChildren {
		child := path.Join(root, element)
		err := conditionalInsert(ctx, client, loader, listener, internalData, child)
		if err != nil {
			return nil, errors.Wrapf(err, "Error loading initial child %v into root %v", child, root)
		}
	}

	startError := node.Start()
	if startError != nil {
		return nil, startError
	}

	return &ZkWatchedMap{node: node, client: client, Root: root, loader: loader, internalData: internalData}, nil
}

func (p *ZkWatchedMap) Get(key string) interface{} {
	return p.internalData[key]
}

func (p *ZkWatchedMap) WaitUntilContains(key string) error {
	return WaitUntilOrErr(func() bool {
		return p.Contains(key)
	})
}

//  TODO these methods are inefficient;  is there an equivalent to ImmutableMap?

func (p *ZkWatchedMap) Contains(key string) bool {
	_, ok := p.internalData[key]
	return ok
}

func (p *ZkWatchedMap) KeySet() []string {

	keys := make([]string, len(p.internalData))
	i := 0
	for k := range p.internalData {
		keys[i] = k
		i++
	}

	return keys
}

func (p *ZkWatchedMap) Values() []interface{} {

	values := make([]interface{}, len(p.internalData))
	i := 0
	for k := range p.internalData {
		values[i] = p.internalData[k]
		i++
	}

	return values
}
