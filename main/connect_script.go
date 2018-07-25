package main

import (
	"os"
	"time"

	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/hank_client"
	"github.com/LiveRamp/hank-go-client/zk_coordinator"

	log "github.com/sirupsen/logrus"

)

func main() {
	argsWithoutProg := os.Args[1:]

	client := curator.NewClient(argsWithoutProg[0], curator.NewRetryNTimes(1, time.Second))

	startErr := client.Start()
	if startErr != nil {
		log.WithError(startErr).Error("error creating curator client")
		return
	}

	coordinator, coordErr := zk_coordinator.NewZkCoordinator(client, "/hank/domains", "/hank/ring_groups", "/hank/domain_groups")
	if coordErr != nil {
		log.WithError(coordErr).Error("error creating zk coordinator")
		return
	}

	options := hank_client.NewHankSmartClientOptions().
		SetNumConnectionsPerHost(2)

	smartClient, clientErr := hank_client.New(coordinator, "spruce-aws", options)
	if clientErr != nil {
		log.WithError(clientErr).Error("error creating smart client")
		return
	}

	time.Sleep(time.Hour)

}
