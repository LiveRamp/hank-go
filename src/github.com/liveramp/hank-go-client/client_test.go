package HankGoClient

import (
	"testing"
	"fmt"
)

func TestClient(t *testing.T){

	c := HankSmartClient{}
	fmt.Println("Client is: ", c)

	//	set up simple thrift partition server

	//	point to it in zk

	//	client reads and connects to get value

	//	need to use hank's thrift definitions

}