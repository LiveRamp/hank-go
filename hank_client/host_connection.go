package hank_client

import (
	"errors"
	"fmt"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"

	"github.com/LiveRamp/hank-go-client/iface"
	"github.com/LiveRamp/hank-go-client/syncext"
	"github.com/LiveRamp/hank-go-client/thriftext"
	log "github.com/sirupsen/logrus"

)

type HostConnection struct {
	host      iface.Host
	hostState iface.HostState

	tryLockTimeoutMs             int32
	establishConnectionTimeoutMs int32
	establishConnectionRetries  int32
	queryTimeoutMs               int32
	bulkQueryTimeoutMs           int32

	socket *thrift.TSocket
	client *hank.PartitionServerClient

	ctx *thriftext.ThreadCtx

	lock *syncext.TimeoutMutex
}

func NewHostConnection(
	host iface.Host,
	tryLockTimeoutMs int32,
	establishConnectionTimeoutMs int32,
	establishConnectionRetries int32,
	queryTimeoutMs int32,
	bulkQueryTimeoutMs int32,
) (*HostConnection, error) {

	connection := HostConnection{
		host:                         host,
		tryLockTimeoutMs:             tryLockTimeoutMs,
		establishConnectionTimeoutMs: establishConnectionTimeoutMs,
		establishConnectionRetries:  establishConnectionRetries,
		queryTimeoutMs:               queryTimeoutMs,
		bulkQueryTimeoutMs:           bulkQueryTimeoutMs,
		lock:                         syncext.NewMutex(),
	}

	host.AddStateChangeListener(&connection)

	err := connection.OnDataChange(string(host.GetState()))

	if err != nil {
		return nil, err
	}

	return &connection, nil

}

func (p *HostConnection) Disconnect() error {

	var err error

	if p.socket != nil {
		err = p.socket.Close()
	} else {
		err = nil
	}

	p.socket = nil
	p.client = nil

	return err
}

func (p *HostConnection) IsServing() bool {
	return p.hostState == iface.HOST_SERVING
}

func (p *HostConnection) IsOffline() bool {
	return p.hostState == iface.HOST_OFFLINE
}

func (p *HostConnection) IsDisconnected() bool {
	return p.client == nil
}

func (p *HostConnection) TryImmediateLock() bool {
	return p.lock.TryLockNoWait()
}

func (p *HostConnection) TryLockWithTimeout() bool {

	if p.tryLockTimeoutMs == 0 {
		p.lock.Lock()
		return true
	}

	return p.lock.TryLock(time.Duration(p.tryLockTimeoutMs) * time.Millisecond)
}

func (p *HostConnection) Lock() {
	p.lock.Lock()
}

func (p *HostConnection) Unlock() {
	p.lock.Unlock()
}

func (p *HostConnection) Get(id iface.DomainID, key []byte, isLockHeld bool) (*hank.HankResponse, error) {

	if !isLockHeld {
		acquired := p.TryLockWithTimeout()
		if !acquired {
			return nil, errors.New("Exceeded timeout while trying to lock the host connection.")
		}
	}

	if !p.IsServing() && !p.IsOffline() {
		p.Unlock()
		return nil, errors.New("Connection to host is not available (host is not serving).")
	}

	if p.IsDisconnected() {
		err := p.connect()
		if err != nil {
			p.Disconnect()
			p.Unlock()
			return nil, err
		}
	}

	resp, err := p.client.Get(int32(id), key)

	if err != nil {
		p.Disconnect()
		p.Unlock()
		return nil, err
	} else if resp.IsSetXception() {
		log.WithField("exception", resp.Xception).Error("Exception from server")
		p.Disconnect()
		p.Unlock()
		return nil, errors.New("exception from server")
	}

	p.Unlock()
	return resp, nil

}

func (p *HostConnection) connect() error {

	p.socket, _ = thrift.NewTSocketTimeout(p.host.GetAddress().Print(), time.Duration(p.establishConnectionTimeoutMs*1e6))
	framed := thrift.NewTFramedTransportMaxLength(p.socket, 16384000)

	err := framed.Open()
	if err != nil {
		log.WithError(err).Error("error connecting to host")
		p.Disconnect()
		return err
	}

	p.client = hank.NewPartitionServerClientFactory(
		framed,
		thrift.NewTCompactProtocolFactory(),
	)

	err = p.socket.SetTimeout(time.Duration(p.queryTimeoutMs * 1e6))
	if err != nil {
		p.Disconnect()
		return err
	}

	return nil
}

func (p *HostConnection) OnDataChange(newVal interface{}) (err error) {

	if newVal == nil {
		newVal = string(iface.HOST_OFFLINE)
	}

	newState := iface.HostState(newVal.(string))

	p.Lock()
	defer p.Unlock()

	disconnectErr := p.Disconnect()
	if disconnectErr != nil {
		log.WithError(disconnectErr).Error("Error disconnecting")
	}

	if newState == iface.HOST_SERVING {

		tries := int32(0)

		hostName := p.host.GetAddress().Print()

		for tries <= p.establishConnectionRetries {

			err := p.connect()

			if err == nil {
				p.hostState = newState
				return nil
			} else {
				log.WithFields(log.Fields{
					"host":    hostName,
					"attempt": tries,
				}).WithError(err).Error("error connecting to host")
			}
		}

		msg := fmt.Sprintf("Failed to connect to host %v after %v retries.  Failing.", hostName, p.establishConnectionRetries)
		err := errors.New(msg)

		log.WithFields(log.Fields{
			"host":    hostName,
			"retries": p.establishConnectionRetries,
		}).WithError(err).Error(msg)

		return err

	}

	p.hostState = newState
	return nil

}
