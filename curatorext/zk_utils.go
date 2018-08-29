package curatorext

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cenkalti/backoff"
	"github.com/curator-go/curator"

	"github.com/LiveRamp/hank-go-client/thriftext"

	log "github.com/sirupsen/logrus"
)

func WaitUntilOrErr(expectTrue func() bool) error {

	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = time.Second * 10

	err := backoff.Retry(func() error {
		val := expectTrue()

		if !val {
			return errors.New("failed to evaluate true")
		}

		return nil

	}, backoffStrat)

	if err == nil {
		log.Info("Assertion success!")
	}

	return err
}

func AssertEmpty(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists != nil {
		return errors.New("domain group already exists")
	}
	return nil
}

func AssertExists(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists == nil {
		return errors.New("domain group doesn't exist")
	}
	return nil
}

func CreateWithParents(client curator.CuratorFramework, mode curator.CreateMode, root string, data []byte) error {

	stat, err := client.CheckExists().ForPath(root)
	if err != nil{
		return err
	}

	if stat != nil {
		return errors.New(fmt.Sprintf("cannot create directory %v: already exists", root))
	}

	builder := client.Create().WithMode(mode).CreatingParentsIfNeeded()

	if data != nil {
		_, createErr := builder.ForPathWithData(root, data)
		return createErr
	} else {
		_, createErr := builder.ForPath(root)
		return createErr
	}

}

func LoadThrift(ctx *thriftext.ThreadCtx, path string, client curator.CuratorFramework, tStruct thrift.TStruct) error {
	data, err := client.GetData().ForPath(path)
	if err != nil {
		return err
	}

	readErr := ctx.ReadThriftBytes(data, tStruct)
	if readErr != nil {
		return readErr
	}

	return nil
}

func CreateEphemeralSequential(root string, framework curator.CuratorFramework) thriftext.SetBytes {
	return func(data []byte) (path string, err error) {
		return framework.Create().WithMode(curator.EPHEMERAL_SEQUENTIAL).ForPathWithData(root, data)
	}
}

func IsSubdirectory(root string, otherPath string) bool {

	cleanRoot := path.Clean(root)
	cleanRel := path.Clean(otherPath)

	if cleanRoot == cleanRel {
		return false
	}

	rel, _ := filepath.Rel(root, otherPath)
	return path.Join(cleanRoot, rel) == cleanRel

}
