package curatorext

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/curator-go/curator"
	"path"
	"path/filepath"
	"github.com/cenkalti/backoff"
	"time"
	"fmt"
	"github.com/bpodgursky/hank-go-client/iface"
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
		fmt.Println("Assertion success!")
	}

	return err
}

func AssertEmpty(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists != nil {
		return errors.New("Domain group already exists!")
	}
	return nil
}

func AssertExists(client curator.CuratorFramework, fullPath string) error {
	exists, _ := client.CheckExists().ForPath(fullPath)
	if exists == nil {
		return errors.New("Domain group doesn't exist!")
	}
	return nil
}

func CreateWithParents(client curator.CuratorFramework, mode curator.CreateMode, root string, data []byte) error {
	builder := client.Create().WithMode(mode).CreatingParentsIfNeeded()

	if data != nil {
		_, createErr := builder.ForPathWithData(root, data)
		return createErr
	} else {
		_, createErr := builder.ForPath(root)
		return createErr
	}

}

func SafeEnsureParents(client curator.CuratorFramework, mode curator.CreateMode, root string) error {

	parentExists, existsErr := client.CheckExists().ForPath(root)
	if existsErr != nil {
		return existsErr
	}

	if parentExists == nil {
		return CreateWithParents(client, mode, root, nil)
	}

	return nil
}

func LoadThrift(ctx *iface.ThreadCtx, path string, client curator.CuratorFramework, tStruct thrift.TStruct) error {
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

func CreateEphemeralSequential(root string, framework curator.CuratorFramework) iface.SetBytes {
	return func(data []byte) error {
		_, err := framework.Create().WithMode(curator.EPHEMERAL_SEQUENTIAL).ForPathWithData(root, data)
		return err
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
