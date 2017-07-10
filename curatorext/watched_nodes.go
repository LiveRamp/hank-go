package curatorext

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/curator-go/curator"
	"strconv"
	"github.com/liveramp/hank-go-client/thriftext"
)

//  thrift

func TDeserializer(ctx *thriftext.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
	inst := constructor()
	err := ctx.ReadThriftBytes(raw, inst.(thrift.TStruct))
	if err != nil {
		return nil, err
	}
	return inst, nil
}

func TSerializer(ctx *thriftext.ThreadCtx, val interface{}) ([]byte, error) {
	bytes, err := ctx.ToBytes(val.(thrift.TStruct))
	if err != nil {
		return nil, err
	}
	return bytes, err
}

func LoadThriftWatchedNode(client curator.CuratorFramework,
	path string,
	constructor Constructor) (*ZkWatchedNode, error) {
	return LoadZkWatchedNode(client, path, constructor, TSerializer, TDeserializer)
}

func NewThriftWatchedNode(client curator.CuratorFramework,
	mode curator.CreateMode,
	path string,
	ctx *thriftext.ThreadCtx,
	constructor Constructor,
	initialValue thrift.TStruct) (*ZkWatchedNode, error) {

	serialized, err := TSerializer(ctx, initialValue)
	if err != nil {
		return nil, err
	}

	return NewZkWatchedNode(client, mode, path, serialized, constructor, TSerializer, TDeserializer)
}

//  raw bytes

//  just casting
func ByteArraySerializer(ctx *thriftext.ThreadCtx, val interface{}) ([]byte, error) {
	return val.([]byte), nil
}

func ByteArrayDeserializer(ctx *thriftext.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
	return raw, nil
}

func LoadBytesWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error) {
	return LoadZkWatchedNode(client, path, nil, ByteArraySerializer, ByteArrayDeserializer)
}

func NewBytesWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, initialValue []byte) (*ZkWatchedNode, error) {
	return NewZkWatchedNode(client, mode, path, initialValue, nil, ByteArraySerializer, ByteArrayDeserializer)
}

//  int

func IntSerializer(ctx *thriftext.ThreadCtx, val interface{}) ([]byte, error) {
	return []byte(strconv.Itoa(val.(int))), nil
}

func IntDeserializer(ctx *thriftext.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error) {
	return strconv.Atoi(string(raw))
}

func LoadIntWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error) {
	return LoadZkWatchedNode(client, path, nil, IntSerializer, IntDeserializer)
}

func NewIntWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, initialValue int) (*ZkWatchedNode, error) {
	serialized, err := IntSerializer(nil, initialValue)
	if err != nil {
		return nil, err
	}

	return NewZkWatchedNode(client, mode, path, serialized, nil, IntSerializer, IntDeserializer)
}

//	string

func StringSerializer(ctx *thriftext.ThreadCtx, val interface{}) ([]byte, error){
	return []byte(val.(string)), nil
}

func StringDeserializer(ctx *thriftext.ThreadCtx, raw []byte, constructor Constructor) (interface{}, error){
	return string(raw), nil
}

func LoadStringWatchedNode(client curator.CuratorFramework, path string) (*ZkWatchedNode, error) {
	return LoadZkWatchedNode(client, path, nil, StringSerializer, StringDeserializer)
}

func NewStringWatchedNode(client curator.CuratorFramework, mode curator.CreateMode, path string, initialValue string) (*ZkWatchedNode, error) {
	serialized, err := StringSerializer(nil, initialValue)
	if err != nil {
		return nil, err
	}
	return NewZkWatchedNode(client, mode, path, serialized, nil, StringSerializer, StringDeserializer)
}