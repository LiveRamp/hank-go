package curatorext

import (
	"errors"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/curator-go/curator"
	"github.com/curator-go/curator/recipes/cache"
	"github.com/samuel/go-zookeeper/zk"

	"github.com/LiveRamp/hank-go-client/thriftext"

	log "github.com/sirupsen/logrus"
	"sync"
	errors2 "github.com/pkg/errors"
	"fmt"
)

type Constructor func() interface{}

type Deserializer func(ctx *thriftext.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error)
type Serializer func(ctx *thriftext.ThreadCtx, val interface{}) ([]byte, error)

type ZkWatchedNode struct {
	client      curator.CuratorFramework
	path        string
	constructor Constructor
	ctx         *thriftext.ThreadCtx
	listeners   []thriftext.DataListener

	serializer   Serializer
	deserializer Deserializer

	lock 		*sync.Mutex

	value interface{}
	stat  *zk.Stat
}

type ObjLoader struct {
	watchedNode *ZkWatchedNode
}

func (p *ObjLoader) ChildEvent(client curator.CuratorFramework, event cache.TreeCacheEvent) error {

	p.watchedNode.lock.Lock()
	defer p.watchedNode.lock.Unlock()

	prevVersion := int32(-1)

	if p.watchedNode.stat != nil {
		prevVersion = p.watchedNode.stat.Version
	}

	switch event.Type {
	case cache.TreeCacheEventNodeUpdated:
		fallthrough

	case cache.TreeCacheEventNodeAdded:
		data := event.Data

		obj, err := p.watchedNode.deserializer(p.watchedNode.ctx, data.Data(), p.watchedNode.constructor)
		if err != nil {
			//	log here because it's called by zk events and doesn't ever percolate up to the user
			log.WithField("node_added", event.Data.Path()).WithError(err).Error("error loading watched node child")
			return err
		}

		p.watchedNode.value = obj
		p.watchedNode.stat = data.Stat()

	case cache.TreeCacheEventNodeRemoved:
		p.watchedNode.value = nil
		p.watchedNode.stat = &zk.Stat{}
	}

	if p.watchedNode.stat != nil && p.watchedNode.stat.Version != prevVersion {
		for _, listener := range p.watchedNode.listeners {

			if event.Data != nil {
				err := listener.OnDataChange(p.watchedNode.value, event.Data.Path())
				if err != nil {
					log.WithField("removed_path", event.Data.Path()).WithError(err).Error("error OnDataChange")
				}
			}
		}
	}

	return nil
}

//  generic

func NewZkWatchedNode(
	client curator.CuratorFramework,
	mode curator.CreateMode,
	path string,
	data []byte,
	constuctor Constructor,
	serializer Serializer,
	deserializer Deserializer) (*ZkWatchedNode, error) {

	err := CreateWithParents(client, mode, path, data)

	if err != nil {
		return nil, err
	}

	return LoadZkWatchedNode(client, path, constuctor, serializer, deserializer, true)
}

func LoadZkWatchedNode(client curator.CuratorFramework, path string, constructor Constructor, serializer Serializer, deserializer Deserializer, requireData bool) (*ZkWatchedNode, error) {

	watchedNode := &ZkWatchedNode{client: client, path: path, constructor: constructor, ctx: thriftext.NewThreadCtx(), listeners: []thriftext.DataListener{}, serializer: serializer, deserializer: deserializer, lock: &sync.Mutex{}}

	node := cache.NewTreeCache(client, path, cache.DefaultTreeCacheSelector).
		SetMaxDepth(0).
		SetCacheData(false)

	node.Listenable().AddListener(&ObjLoader{watchedNode})
	err := node.Start()
	if err != nil {
		return nil, err
	}


	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	//	IF we don't require the node to exist, AND the path definitely doesn't exist, return early
	if !requireData {
		stat, err := client.CheckExists().ForPath(path)
		if err != nil {
			return nil, err
		}
		if stat == nil {
			return watchedNode, nil
		}
	}

	err = backoff.Retry(func() error {
		res := watchedNode.value != nil
		if !res {
			return errors.New("node does not exist yet")
		}
		return nil
	}, backoffStrat)

	if err != nil {

		fmtStr := "Never found data for node path %v"

		//	if the path doesn't exist, we shouldn't be here at all -- indicate upstream somehow that this node should not exist
		stat, err := client.CheckExists().ForPath(path)
		if err != nil {
			return nil, errors2.Wrapf(err, fmtStr, path)
		}

		if stat == nil {
			return nil, nil
		}

		data, err := client.GetData().ForPath(path)
		if err != nil {
			return nil, errors2.Wrapf(err, fmtStr, path)
		}

		//	if the path exists but doesn't have data (but requireData), something is highly wtf
		//	if the path exists and has data and requireData, something is wtf
		return nil, errors.New(fmt.Sprintf("Never found data for node path %v, path exists, data exists = %v", path, data != nil))

	}

	return watchedNode, nil
}

func (p *ZkWatchedNode) Get() interface{} {
	return p.value
}

func (p *ZkWatchedNode) Set(ctx *thriftext.ThreadCtx,
	value interface{}) error {

	bytes, err := p.serializer(ctx, value)
	if err != nil {
		return err
	}

	exists, err := p.client.CheckExists().ForPath(p.path)
	if err != nil {
		return err
	}

	if exists == nil {
		p.client.Create().ForPath(p.path)
	}

	_, err = p.client.SetData().ForPathWithData(p.path, bytes)
	return err
}

func (p *ZkWatchedNode) Delete() error {
	err := p.client.Delete().ForPath(p.path)
	return err
}

func (p *ZkWatchedNode) AddListener(listener thriftext.DataListener) {
	p.listeners = append(p.listeners, listener)
}

// Note: update() should not modify its argument
type Updater func(interface{}) interface{}

func (p *ZkWatchedNode) Update(ctx *thriftext.ThreadCtx, updater Updater) (interface{}, error) {

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	var newValue interface{}

	error := backoff.Retry(func() error {

		newValue = updater(p.value)

		bytes, err := p.serializer(ctx, newValue)
		if err != nil {
			return err
		}

		_, err = p.client.SetData().WithVersion(p.stat.Version).ForPathWithData(p.path, bytes)
		if err != nil {
			return err
		}

		return nil

	}, backoffStrat)

	if error != nil {
		return nil, error
	}

	return newValue, nil
}
