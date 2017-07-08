package iface

import (
	"sync"
	"git.apache.org/thrift.git/lib/go/thrift"
)

type ThreadCtx struct {
	serializer   *thrift.TSerializer
	deserializer *thrift.TDeserializer

	serializeLock   *sync.Mutex
	deserializeLock *sync.Mutex
}

func NewThreadCtx() *ThreadCtx {
	
	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocol(serializer.Transport)

	deserializer := thrift.NewTDeserializer()
	deserializer.Protocol = thrift.NewTCompactProtocol(deserializer.Transport)

	return &ThreadCtx{
		serializer:      serializer,
		deserializer:    deserializer,
		serializeLock:   &sync.Mutex{},
		deserializeLock: &sync.Mutex{},
	}

}

type GetBytes func() ([]byte, error)

type SetBytes func(value []byte) error

func (p *ThreadCtx) ReadThrift(get GetBytes, emptyStruct thrift.TStruct) error {

	bytes, err := get()

	if err != nil {
		return err
	}

	return p.ReadThriftBytes(bytes, emptyStruct)
}

func (p *ThreadCtx) ReadThriftBytes(data []byte, emptyStruct thrift.TStruct) error {

	p.deserializeLock.Lock()
	defer p.deserializeLock.Unlock()

	deserErr := p.deserializer.Read(emptyStruct, data)
	if deserErr != nil {
		return deserErr
	}

	return nil
}

func (p *ThreadCtx) SetThrift(set SetBytes, tStruct thrift.TStruct) error {

	bytes, err := p.ToBytes(tStruct)
	if err != nil {
		return err
	}

	return set(bytes)
}

func (p *ThreadCtx) ToBytes(tStruct thrift.TStruct) ([]byte, error) {

	p.serializeLock.Lock()
	defer p.serializeLock.Unlock()

	bytes, err := p.serializer.Write(tStruct)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
