package thrift_services

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"sync"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

type MapPartitionServerHandler struct {
	mockData    map[string]string
	NumRequests int32
}

func NewPartitionServerHandler(mockData map[string]string) *MapPartitionServerHandler {
	return &MapPartitionServerHandler{mockData: mockData}
}

func (p *MapPartitionServerHandler) ClearRequestCounters() {
	p.NumRequests = 0
}

//	assume everything is in one domain for testing
func (p *MapPartitionServerHandler) Get(domain_id int32, key []byte) (r *hank.HankResponse, err error) {
	p.NumRequests++

	var response = hank.NewHankResponse()

	val, ok := p.mockData[string(key)]
	if ok {
		response.Value = []byte(val)
		response.NotFound = newFalse()
		response.Xception = nil
	} else {
		response.NotFound = newTrue()
	}

	return response, nil
}

func newFalse() *bool {
	b := false
	return &b
}

func newTrue() *bool {
	b := true
	return &b
}

func (p *MapPartitionServerHandler) GetBulk(domain_id int32, keys [][]byte) (r *hank.HankBulkResponse, err error) {

	var response = hank.NewHankBulkResponse()
	var responses = make([]*hank.HankResponse, 0)

	for _, element := range keys {
		v, _ := p.Get(0, element)
		responses = append(responses, v)
	}

	response.Responses = responses
	return response, nil

}

func Serve(
	handler hank.PartitionServer,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory,
	addr string) (*thrift.TSimpleServer, func()) {

	var transport, _ = thrift.NewTServerSocket(addr)

	fmt.Printf("%T\n", transport)
	processor := hank.NewPartitionServerProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Starting the simple server... on ", addr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		server.Serve()
		wg.Done()
	}(&wg)

	return server, func() {
		transport.Close()
		server.Stop()
		wg.Wait()
	}
}
