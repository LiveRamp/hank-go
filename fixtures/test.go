package fixtures

import (
	"errors"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/curator-go/curator"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func WaitUntilOrFail(t *testing.T, expectTrue func() bool) error {

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	err := backoff.Retry(func() error {
		val := expectTrue()

		if !val {
			return errors.New("failed to evaluate true")
		}

		return nil

	}, backoffStrat)

	assert.True(t, err == nil)

	if err == nil {
		fmt.Println("Assertion success!")
	}

	return err
}

type logWriter struct {
	t *testing.T
	p string
}

func (lw logWriter) Write(b []byte) (int, error) {
	lw.t.Logf("%s%s", lw.p, string(b))
	return len(b), nil
}

func SetupZookeeper(t *testing.T) (*zk.TestCluster, curator.CuratorFramework) {
	cluster, _ := zk.StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
	cluster.StartAllServers()

	client := curator.NewClient("127.0.0.1:"+strconv.Itoa(cluster.Servers[0].Port), curator.NewRetryNTimes(1, time.Second))
	client.Start()

	return cluster, client
}

func TeardownZookeeper(cluster *zk.TestCluster, client curator.CuratorFramework) {
	fmt.Println("Tearing down zookeeper")
	client.ZookeeperClient().Close()
	cluster.StopAllServers()
}
