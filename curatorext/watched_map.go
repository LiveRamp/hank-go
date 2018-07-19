package curatorext

import (
	"fmt"
	"path"

	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"

	"github.com/LiveRamp/hank-go-client/thriftext"
	"sync"
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

	switch event.Type {
	case cache.TreeCacheEventNodeUpdated:
		fallthrough
	case cache.TreeCacheEventNodeAdded:
		fullChildPath := event.Data.Path()

		if IsSubdirectory(p.root, fullChildPath) {
			err := conditionalInsert(p.ctx, client, p.loader, p.listener, p.lock, p.internalData, fullChildPath)
			p.listener.OnChange()
			if err != nil {
				fmt.Println("Error inserting child: ", err)
				return err
			}
		}
	case cache.TreeCacheEventNodeRemoved:
		fullChildPath := event.Data.Path()
		p.lock.Lock()
		delete(p.internalData, path.Base(fullChildPath))
		p.lock.Unlock()
		p.listener.OnChange()
	}

	return nil
}

func conditionalInsert(ctx *thriftext.ThreadCtx, client curator.CuratorFramework, loader Loader, listener thriftext.DataChangeNotifier, lock *sync.Mutex, internalData map[string]interface{}, fullChildPath string) error {

	newKey := path.Base(fullChildPath)

	item, err := loader(ctx, client, listener, fullChildPath)
	if err != nil {
		return err
	}

	if item != nil {
		lock.Lock()
		internalData[newKey] = item
		lock.Unlock()
	}

	return nil
}

func NewZkWatchedMap(
	client curator.CuratorFramework,
	root string,
	listener thriftext.DataChangeNotifier,
	loader Loader) (*ZkWatchedMap, error) {

	internalData := make(map[string]interface{})

	SafeEnsureParents(client, curator.PERSISTENT, root)

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

	startError := node.Start()
	if startError != nil {
		return nil, startError
	}

	initialChildren, err := client.GetChildren().ForPath(root)
	if err != nil {
		return nil, err
	}

	ctx := thriftext.NewThreadCtx()

	for _, element := range initialChildren {
		// Hope this doesn't run into memory problems
		child := path.Join(root, element)
		err := conditionalInsert(ctx, client, loader, listener, insertLock, internalData, child)
		if err != nil {
			fmt.Println(fmt.Sprintf("Error loading initial child: %v", child))
			return nil, err
		}
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
