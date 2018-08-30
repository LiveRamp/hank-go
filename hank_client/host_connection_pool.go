package hank_client

import (
	"encoding/hex"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"

	"github.com/LiveRamp/hank-go/iface"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

const NO_HASH = -1
const NO_SEED = -1

type IndexedHostConnection struct {
	connection *HostConnection
	hostIndex  int32
}

type ConnectionSet struct {
	connections             [][]*IndexedHostConnection
	previouslyUsedHostIndex int32
}

type HostConnectionPool struct {
	preferredPools *ConnectionSet
	otherPools     *ConnectionSet

	incrementLock *sync.Mutex
	random        *rand.Rand
}

func CreateHostConnectionPool(connections []*HostConnection, hostShuffleSeed int64, preferredHosts []string) (*HostConnectionPool, error) {

	asMap := make(map[string][]*HostConnection)

	if len(connections) == 0 {
		return nil, nil
	}

	for _, connection := range connections {
		address := connection.host.GetAddress().Print()

		connections := asMap[address]
		if connections == nil {
			connections = []*HostConnection{}
		}

		asMap[address] = append(connections, connection)

	}

	return NewHostConnectionPool(asMap, hostShuffleSeed, preferredHosts)

}

func shuffleStrings(slice []string, hostShuffleSeed int64) {
	seededRand := rand.New(rand.NewSource(hostShuffleSeed))

	n := len(slice)
	for i := n - 1; i > 0; i-- {
		j := seededRand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func shuffleConns(slice []*IndexedHostConnection, hostShuffleSeed int64) {
	seededRand := rand.New(rand.NewSource(hostShuffleSeed))

	n := len(slice)
	for i := n - 1; i > 0; i-- {
		j := seededRand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func NewHostConnectionPool(connectionsByHost map[string][]*HostConnection, hostShuffleSeed int64, preferredHosts []string) (*HostConnectionPool, error) {

	if len(connectionsByHost) == 0 {
		return nil, errors.New("Cannot create a HostConnectionPool with no connections")
	}

	shuffledHosts := []string{}
	for host := range connectionsByHost {
		shuffledHosts = append(shuffledHosts, host)
	}

	if hostShuffleSeed != NO_SEED {
		sort.Strings(shuffledHosts)
		shuffleStrings(shuffledHosts, hostShuffleSeed)
	} else {
		shuffleStrings(shuffledHosts, time.Now().Unix())
	}

	random := rand.New(rand.NewSource(time.Now().Unix()))

	preferredIndex := int32(0)
	otherIndex := int32(0)

	preferredSet := make(map[string]bool)
	for _, host := range preferredHosts {
		preferredSet[host] = true
	}

	preferred := &ConnectionSet{connections: [][]*IndexedHostConnection{}}
	other := &ConnectionSet{connections: [][]*IndexedHostConnection{}}

	for _, host := range shuffledHosts {
		if _, ok := preferredSet[host]; ok {
			preferred.connections = append(preferred.connections, buildConnections(connectionsByHost, preferredIndex, host))
			preferredIndex++
		} else {
			other.connections = append(other.connections, buildConnections(connectionsByHost, otherIndex, host))
			otherIndex++
		}
	}

	if len(preferred.connections) != 0 {
		preferred.previouslyUsedHostIndex = random.Int31n(int32(len(preferred.connections)))
	}

	if len(other.connections) != 0 {
		other.previouslyUsedHostIndex = random.Int31n(int32(len(other.connections)))
	}

	return &HostConnectionPool{
		preferred,
		other,
		&sync.Mutex{},
		random,
	}, nil

}

func buildConnections(connectionsByHost map[string][]*HostConnection, hostIndex int32, host string) []*IndexedHostConnection {

	connections := []*IndexedHostConnection{}
	for _, connection := range connectionsByHost[host] {
		connections = append(connections, &IndexedHostConnection{connection: connection, hostIndex: hostIndex})
	}

	shuffleConns(connections, time.Now().Unix())
	return connections

}

func (p *HostConnectionPool) getConnectionFromPools(pools *ConnectionSet, keyHash int32, connection *IndexedHostConnection) (*IndexedHostConnection, bool) {

	var conn *IndexedHostConnection
	var locked bool

	p.incrementLock.Lock()

	if connection == nil {
		if keyHash == NO_HASH {
			conn, locked = p.getConnectionToUse(pools)
		} else {
			conn, locked = p.getConnectionToUseForKey(pools, keyHash)
		}
	} else {
		conn, locked = p.getNextConnectionToUse(connection.hostIndex, pools.connections)
	}

	p.incrementLock.Unlock()
	return conn, locked

}

func (p *HostConnectionPool) getConnectionToUseForKey(pool *ConnectionSet, keyHash int32) (*IndexedHostConnection, bool) {
	return p.getNextConnectionToUse(int32(keyHash%int32(len(pool.connections))), pool.connections)
}

func (p *HostConnectionPool) getNextHostIndexToUse(previouslyUsedHostIndex int32, connections [][]*IndexedHostConnection) int32 {
	if previouslyUsedHostIndex >= int32(len(connections)-1) {
		return 0
	} else {
		return previouslyUsedHostIndex + 1
	}
}

func (p *HostConnectionPool) getNextConnectionToUse(previouslyUsedHostIndex int32, connections [][]*IndexedHostConnection) (*IndexedHostConnection, bool) {

	for tryId := 0; tryId < len(connections); tryId++ {

		previouslyUsedHostIndex = p.getNextHostIndexToUse(previouslyUsedHostIndex, connections)
		connectionList := connections[previouslyUsedHostIndex]

		// If a host has one unavaible connection, it is itself unavailable. Move on to the next host.

		for _, indexedConnection := range connectionList {
			if !indexedConnection.connection.IsServing() {
				break
			}

			if indexedConnection.connection.TryImmediateLock() {
				return indexedConnection, true
			}
		}
	}

	// Here, host index is back to the same host we started with (it looped over once)

	for tryId := 0; tryId < len(connections); tryId++ {

		previouslyUsedHostIndex = p.getNextHostIndexToUse(previouslyUsedHostIndex, connections)

		connectionList := connections[previouslyUsedHostIndex]

		// Pick a random connection for that host
		connectionAndIndex := connectionList[p.random.Intn(len(connectionList))]

		// If a host has one unavaible connection, it is itself unavailable.
		// Move on to the next host. Otherwise, return it.

		if connectionAndIndex.connection.IsServing() {
			// Note: here the returned connection is not locked.
			// Locking/unlocking it is not the responsibily of this method.
			return connectionAndIndex, false
		}
	}

	// Here, host index is back to the same host we started with (it looped over twice)

	// No random available connection was found, return a random connection that is not available.
	// This is a worst case scenario only. For example when hosts miss a Zookeeper heartbeat and report
	// offline when the Thrift partition server is actually still up. We then attempt to use an unavailable
	// connection opportunistically, until the system recovers.

	for tryId := 0; tryId < len(connections); tryId++ {

		previouslyUsedHostIndex = p.getNextHostIndexToUse(previouslyUsedHostIndex, connections)
		hostConnections := connections[previouslyUsedHostIndex]

		// Pick a random connection for that host, and use it only if it is offline
		hostConnection := hostConnections[p.random.Intn(len(hostConnections))]

		if hostConnection.connection.IsOffline() {
			return hostConnection, false
		}

	}

	// No available connection was found, return null
	return nil, false

}

func (p *HostConnectionPool) getConnectionToUse(set *ConnectionSet) (*IndexedHostConnection, bool) {
	result, locked := p.getNextConnectionToUse(set.previouslyUsedHostIndex, set.connections)

	if result != nil {
		set.previouslyUsedHostIndex = result.hostIndex
	}

	return result, locked
}

func newTrue() *bool {
	b := true
	return &b
}

func (p *HostConnectionPool) attemptQuery(connection *IndexedHostConnection, isLockHeld bool, domain iface.Domain, key []byte, numTries int32, maxNumTries int32) (*hank.HankResponse, error) {
	domainId := domain.GetId()

	if connection == nil {
		return nil, errors.Errorf("no connection available to domain %v", domain.GetName())
	} else {

		// Perform query

		resp, err := connection.connection.Get(domainId, key, isLockHeld)

		if resp != nil {
			return resp, nil
		} else {

			//	if we don't warn, we have no record of the error on retries
			log.WithFields(log.Fields{
				"host":          connection.connection.host.GetAddress(),
				"num_tries":     numTries,
				"max_num_tries": maxNumTries,
				"domain":        domain.GetName(),
				"key":           hex.EncodeToString(key),
			}).WithError(err).Debug("Failed to perform query, retrying")

			if numTries < maxNumTries {
				return nil, nil
			} else {
				return nil, errors.Errorf("failed to perform query. host: %v num_tries: %v domain: %v key: %v", connection.connection.host.GetAddress(), numTries, domain.GetName(), hex.EncodeToString(key))
			}
		}
	}
}

func (p *HostConnectionPool) Get(domain iface.Domain, key []byte, maxNumTries int32, keyHash int32) (*hank.HankResponse, error) {

	var indexedConnection *IndexedHostConnection
	var locked bool

	numPreferredTries := int32(0)
	numOtherTries := int32(0)

	for true {

		//	jump out if we don't have any more preferred hosts
		if numPreferredTries >= int32(len(p.preferredPools.connections)) {
			break
		}

		//	Either get a connection to an arbitrary host, or get a connection skipping the
		//	previous host used (since it failed)
		indexedConnection, locked = p.getConnectionFromPools(p.preferredPools, keyHash, indexedConnection)

		numPreferredTries++

		response, err := p.attemptQuery(indexedConnection, locked, domain, key, numPreferredTries, maxNumTries)

		if err != nil {
			return nil, err
		}

		if response != nil {
			return response, nil
		}

	}

	for true {

		indexedConnection, locked = p.getConnectionFromPools(p.otherPools, keyHash, indexedConnection)
		numOtherTries++

		resp, err := p.attemptQuery(indexedConnection, locked, domain, key, numPreferredTries+numOtherTries, maxNumTries)

		if err != nil {
			return nil, err
		}

		if resp != nil {
			return resp, nil
		}

	}

	//	Go, you are a stupid compiler, this is unreachable.
	return nil, nil
}

func (p *HostConnectionPool) GetConnections() []*HostConnection {

	all := []*HostConnection{}

	for _, conns := range p.preferredPools.connections {
		for _, conn := range conns {
			all = append(all, conn.connection)
		}
	}

	for _, conns := range p.otherPools.connections {
		for _, conn := range conns {
			all = append(all, conn.connection)
		}
	}

	return all
}

func (p *HostConnectionPool) GetConnectionLoad() (numConnections int64, numLockedConnections int64) {

	numConnections = 0
	numLockedConnections = 0

	for _, conns := range p.preferredPools.connections {
		for _, conn := range conns {
			if conn.connection.lock.TestIsLocked() {
				numLockedConnections++
			}
			numConnections++
		}
	}

	for _, conns := range p.otherPools.connections {
		for _, conn := range conns {
			if conn.connection.lock.TestIsLocked() {
				numLockedConnections++
			}
			numConnections++
		}
	}

	return numConnections, numLockedConnections

}
