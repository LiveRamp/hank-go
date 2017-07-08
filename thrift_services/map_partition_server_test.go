package thrift_services

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

const PARTITION_SERVER_ADDRESS = "127.0.0.1:56783"

func toBytes(str string) (b []byte) {
	return []byte(str)
}

func TestMapPartitionServer(t *testing.T) {

	testData := make(map[string]string)

	testData["key1"] = "value1"
	testData["key2"] = "value2"
	testData["key3"] = "value3"

	handler := NewPartitionServerHandler(testData)

	//	set up simple mock thrift partition server
	_, close := Serve(
		handler,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		thrift.NewTCompactProtocolFactory(),
		PARTITION_SERVER_ADDRESS)


	time.Sleep(time.Second)

	var transport, _ = thrift.NewTSocket(PARTITION_SERVER_ADDRESS)
	transport.Open()

	framed := thrift.NewTFramedTransportMaxLength(transport, 16384000)


	client := hank.NewPartitionServerClientFactory(
		framed,
		thrift.NewTCompactProtocolFactory())

	result, _ := client.Get(0, toBytes("key1"))
	assert.Equal(t, toBytes("value1"), result.Value)

	transport.Close()
	close()

}
