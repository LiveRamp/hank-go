package main

import (
	"fmt"
	"os"
	"time"

	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/hank_client"
	"github.com/LiveRamp/hank-go-client/zk_coordinator"
)

func main() {
	argsWithoutProg := os.Args[1:]

	client := curator.NewClient(argsWithoutProg[0], curator.NewRetryNTimes(1, time.Second))

	startErr := client.Start()
	if startErr != nil {
		fmt.Println(startErr)
		return
	}

	coordinator, coordErr := zk_coordinator.NewZkCoordinator(client, "/hank/domains", "/hank/ring_groups", "/hank/domain_groups")
	if coordErr != nil {
		fmt.Println(startErr)
		return
	}

	options := hank_client.NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2)

	smartClient, clientErr := hank_client.New(coordinator, "spruce-aws", options)
	if clientErr != nil {
		fmt.Println(clientErr)
		return
	}

	fmt.Println(smartClient)

	time.Sleep(time.Hour)

}
