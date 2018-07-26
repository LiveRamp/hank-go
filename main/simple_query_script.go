package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/hank_client"
	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/thriftext"
	"github.com/LiveRamp/hank-go-client/zk_coordinator"

	log "github.com/sirupsen/logrus"
)

func main() {
	argsWithoutProg := os.Args[1:]

	client := curator.NewClient(argsWithoutProg[0], curator.NewRetryNTimes(1, time.Second))
	client.Start()

	ctx := thriftext.NewThreadCtx()

	coordinator, coordErr := zk_coordinator.NewZkCoordinator(client, "/hank/domains", "/hank/ring_groups", "/hank/domain_groups")
	if coordErr != nil {
		log.WithError(coordErr).Error("error creating coordinator")
		return
	}

	group := coordinator.GetRingGroup("spruce-aws")
	ring0 := group.GetRing(iface.RingID(0))

	hosts := ring0.GetHosts(ctx)
	host := hosts[0]

	conn, _ := hank_client.NewHostConnection(host, 100, 100, 1, 100, 100)

	domain := coordinator.GetDomain(argsWithoutProg[1])
	domainId := domain.GetId()

	log.WithField("domain", domain.GetName()).Info("querying domain")

	file, err := os.Open(argsWithoutProg[2])
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		bytes := scanner.Bytes()
		text := string(bytes)

		log.Info(fmt.Sprintf("Checking: %v", text))

		bytes, err := hex.DecodeString(strings.TrimSpace(text))
		if err != nil {
			log.WithError(err).Error("error decoding string")
			return
		}

		val, err := conn.Get(domainId, bytes, false)
		if err != nil {
			log.WithError(err).Error("error querying")
			return
		}

		if val.Value != nil {
			log.Info("Found value")
			encodeToString := hex.EncodeToString(val.Value)
			log.WithField("value", encodeToString).Info("found value")
		} else {
			log.Info("did not find value")
		}

	}

}
