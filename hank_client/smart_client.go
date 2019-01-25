package hank_client

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/hank/hank-core/src/main/go/hank"
	"github.com/karlseguin/ccache"

	"github.com/LiveRamp/hank-go/iface"
	"github.com/LiveRamp/hank-go/syncext"
	"github.com/LiveRamp/hank-go/thriftext"

	"regexp"

	"github.com/LiveRamp/hank-go/zk_coordinator"
	"github.com/pkg/errors"
)

const NumStatSamples = 3
const SampleSleepTime = time.Second
const StatInterval = time.Second * 30

//	we don't want to rebuild each time an updating host updates a version of a partition.  we'll catch it when
//	it switches back to active.
var AssignmentsRegex = regexp.MustCompile(fmt.Sprintf("hosts/[-0-9]+/%v", zk_coordinator.ASSIGNMENTS_PATH))
var SkipRebuildPaths = []*regexp.Regexp{AssignmentsRegex}

type RequestCounters struct {
	requests  int64
	cacheHits int64

	lock *sync.Mutex
}

type HankCacheResponse struct {
	*hank.HankResponse
	Cached bool
}


func NewRequestCounters() *RequestCounters {
	return &RequestCounters{
		0,
		0,
		&sync.Mutex{},
	}
}

func (p *RequestCounters) increment(requests int64, cacheHits int64) {
	p.lock.Lock()

	p.requests += requests
	p.cacheHits += cacheHits

	p.lock.Unlock()
}

func (p *RequestCounters) clear() (requests int64, cacheHits int64) {
	p.lock.Lock()

	requests = p.requests
	cacheHits = p.cacheHits

	p.requests = 0
	p.cacheHits = 0

	p.lock.Unlock()

	return requests, cacheHits
}

type HankSmartClient struct {
	coordinator iface.Coordinator
	ringGroup   iface.RingGroup

	options *hankSmartClientOptions

	serverToConnections       map[string]*HostConnectionPool
	domainToPartToConnections map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool
	connectionLock            *sync.Mutex
	stopping                  *bool

	clientId string

	//	mostly for testing
	numCacheRebuildTriggers     int
	numSkippedRebuildTriggers   int
	numCreatedConnections       int
	numClosedConnections        int
	numSuccessfulCacheRefreshes int

	cache    *ccache.Cache
	counters *RequestCounters

	cacheUpdateLock *syncext.SingleLockSemaphore
}

func New(
	coordinator iface.Coordinator,
	ringGroupName string,
	options *hankSmartClientOptions) (*HankSmartClient, error) {

	ringGroup := coordinator.GetRingGroup(ringGroupName)

	metadata, err := GetClientMetadata()

	if err != nil {
		log.WithError(err).Error("error fetching client metadata")
		return nil, err
	}

	ctx := thriftext.NewThreadCtx()
	id, registerErr := ringGroup.RegisterClient(ctx, metadata)

	if registerErr != nil {
		log.WithError(registerErr).Error("error registering client")
		return nil, registerErr
	}

	var cache *ccache.Cache

	if options.ResponseCacheEnabled {
		cache = ccache.New(ccache.Configure().MaxSize(int64(options.ResponseCacheNumItems)))
	}

	connectionCacheLock := syncext.NewSingleLockSemaphore()

	stopping := false

	client := &HankSmartClient{coordinator,
		ringGroup,
		options,
		make(map[string]*HostConnectionPool),
		make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool),
		&sync.Mutex{},
		&stopping,
		id,
		0,
		0,
		0,
		0,
		0,
		cache,
		NewRequestCounters(),
		connectionCacheLock,
	}

	//	if we can't build a proper cache when first instantiating the client, fail out hard.
	//	once you're running later, we don't want to do this
	err = client.updateConnectionCache(ctx)
	if err != nil {
		log.WithError(err).Error("error updating connection cache")
		return nil, err
	}

	go client.updateLoop(connectionCacheLock)
	go client.runtimeStatsLoop()

	ringGroup.AddListener(client)

	return client, nil
}

func (p *HankSmartClient) OnChange(path string) {

	for _, item := range SkipRebuildPaths {
		if len(item.FindStringSubmatch(path)) > 0 {
			p.numSkippedRebuildTriggers++
			return
		}
	}

	log.WithField("changed_path", path).Info("Triggering cache rebuild")
	p.numCacheRebuildTriggers++
	p.cacheUpdateLock.Release()

}

func (p *HankSmartClient) updateLoop(listenerLock *syncext.SingleLockSemaphore) {

	ctx := thriftext.NewThreadCtx()

	for true {

		listenerLock.Read()

		if *p.stopping {
			log.Info("exiting cache update routine")
			break
		}

		p.updateConnectionCache(ctx)
	}

}

func (p *HankSmartClient) getNumCacheRebuildTriggers() int {
	return p.numCacheRebuildTriggers
}

func (p *HankSmartClient) runtimeStatsLoop() {

	lastCheck := time.Now().UnixNano()

	for true {

		if *p.stopping {
			log.Info("exiting stats loop")
			break
		}

		time.Sleep(StatInterval)

		serverTotalConns := make(map[string]int64)
		serverLockedConns := make(map[string]int64)

		for i := 0; i < NumStatSamples; i++ {
			for server, conns := range p.serverToConnections {
				conns, lockedConns := conns.GetConnectionLoad()
				serverTotalConns[server] += conns
				serverLockedConns[server] += lockedConns
			}

			time.Sleep(SampleSleepTime)
		}

		for server, total := range serverTotalConns {
			locked := serverLockedConns[server]

			if locked > 0 {
				log.WithFields(log.Fields{
					"host":   server,
					"locked": locked,
					"total":  total,
				}).Debug("Client connection load")
			}

		}

		requests, cacheHits := p.counters.clear()
		now := time.Now().UnixNano()
		elapsed := float64(now-lastCheck) / float64(1e9)
		throughput := float64(requests) / elapsed
		cacheHitRate := float64(cacheHits) / float64(requests)

		if requests != 0 {

			log.WithFields(log.Fields{
				"throughput_qps": throughput,
				"cache_hit_rate": cacheHitRate,
			}).Debug("Client-side cache hit rate")

		}

		lastCheck = now

	}

}

func (p *HankSmartClient) Stop() {

	p.stopping = newTrue()
	p.cacheUpdateLock.Release()

	for _, value := range p.domainToPartToConnections {
		for _, connections := range value {
			for _, conns := range connections.otherPools.connections {
				for _, conn := range conns {
					conn.connection.Disconnect()
				}
			}
		}
	}

	p.ringGroup.DeregisterClient(&thriftext.ThreadCtx{}, p.clientId)

}

func newFalse() *bool {
	b := false
	return &b
}

func GetClientMetadata() (*hank.ClientMetadata, error) {

	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	metadata := hank.NewClientMetadata()
	metadata.Host = hostname
	metadata.ConnectedAt = time.Now().Unix() * int64(1000)
	metadata.Type = "GolangHankSmartClient"
	metadata.Version = "lolidk"

	return metadata, nil
}

func (p *HankSmartClient) updateConnectionCache(ctx *thriftext.ThreadCtx) error {
	log.Info("Loading Hank's smart client metadata cache and connections.")

	newServerToConnections := make(map[string]*HostConnectionPool)
	newDomainToPartitionToConnections := make(map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool)

	err := p.buildNewConnectionCache(ctx, newServerToConnections, newDomainToPartitionToConnections)

	if err != nil || len(newServerToConnections) == 0 {
		log.WithError(err).Error("Error building new connection cache")
		return err
	}

	oldServerToConnections := p.serverToConnections

	// Switch old cache for new cache
	p.connectionLock.Lock()
	p.serverToConnections = newServerToConnections
	p.domainToPartToConnections = newDomainToPartitionToConnections
	p.connectionLock.Unlock()

	for address, pool := range oldServerToConnections {
		if _, ok := p.serverToConnections[address]; !ok {
			log.WithField("address", address).Info("closing connections to server %v ", address)
			for _, conn := range pool.GetConnections() {
				conn.Disconnect()
				p.numClosedConnections++
			}
		}
	}

	p.numSuccessfulCacheRefreshes++

	return nil
}

func (p *HankSmartClient) Get(domainName string, key []byte) (*HankCacheResponse, error) {

	domain := p.coordinator.GetDomain(domainName)

	if domain == nil {
		return nil, errors.Errorf("domain %v not found", domainName)
	}

	return p.get(domain, key)
}

type Entry struct {
	domain iface.DomainID
	key    string
}

func (p *HankSmartClient) get(domain iface.Domain, key []byte) (*HankCacheResponse, error) {

	if key == nil {
		return nil, errors.New("null key")
	}

	if len(key) == 0 {
		return nil, errors.New("empty key")
	}

	domainID := domain.GetId()
	entry := strconv.Itoa(int(domainID)) + "-" + string(key)

	var val interface{}
	var cached = false

	if p.cache != nil {
		item := p.cache.Get(entry)
		if item != nil && !item.Expired() {
			val = item.Value()
			cached = true
		}
	}

	if val != nil {
		p.counters.increment(1, 1)
		var resp = &HankCacheResponse{
			val.(*hank.HankResponse),
			cached,
		}

		return resp, nil
	} else {
		p.counters.increment(1, 0)

		// Determine HostConnectionPool to use
		partitioner := domain.GetPartitioner()
		partition := partitioner.Partition(key, domain.GetNumParts())
		keyHash := partitioner.Partition(key, math.MaxInt32)

		p.connectionLock.Lock()
		parts := p.domainToPartToConnections[domainID]
		p.connectionLock.Unlock()

		if parts == nil {
			return nil, errors.Errorf("could not find domain %v in partition map", domain.GetName())
		}

		pool := parts[iface.PartitionID(partition)]

		if pool == nil {
			return nil, errors.Errorf("could not find list of hosts for domain %v partition %v", domain.GetName(), partition)
		}

		response, err := pool.Get(domain, key, p.options.QueryMaxNumTries, keyHash)

		if err != nil {
			return nil, err
		}

		if p.cache != nil && response != nil && (response.IsSetNotFound() || response.IsSetValue()) {
			p.cache.Set(entry, response, p.options.ResponseCacheExpiryTime)
		}

		if response.IsSetXception() {
			return nil, errors.Errorf("received exception from server: %v for domain: %v partition: %v key: %v ",
				response.Xception, domain.GetName(), partition, key)
		}


		return &HankCacheResponse{response, cached}, nil

	}

}

func (p *HankSmartClient) isPreferredHost(ctx *thriftext.ThreadCtx, host iface.Host) bool {

	flags := host.GetEnvironmentFlags(ctx)

	if flags != nil && p.options.PreferredHostEnvironment != nil {
		clientValue, ok := flags[p.options.PreferredHostEnvironment.Key]

		if ok && clientValue == p.options.PreferredHostEnvironment.Value {
			return true
		}

	}

	return false
}

func (p *HankSmartClient) buildNewConnectionCache(
	ctx *thriftext.ThreadCtx,
	newServerToConnections map[string]*HostConnectionPool,
	newDomainToPartitionToConnections map[iface.DomainID]map[iface.PartitionID]*HostConnectionPool) error {

	//  this is horrible looking, and I'd love to make a MultiMap, but I can't because Go is the short-bus of languages
	domainToPartToAddresses := make(map[iface.DomainID]map[iface.PartitionID][]*iface.PartitionServerAddress)

	var preferredHosts []string
	var err error

	rings := p.ringGroup.GetRings()

	log.WithField("num_rings", len(rings)).Info("Building new connection caches")

	for _, ring := range rings {
		hosts := ring.GetHosts(ctx)

		log.WithFields(log.Fields{
			"ring":      ring.GetNum(),
			"num_hosts": len(hosts),
		}).Info("Building connection cache for ring")

		for _, host := range hosts {
			hostAddress := host.GetAddress().Print()

			log.WithFields(log.Fields{
				"host": hostAddress,
			}).Info("Building cache for host")

			if p.isPreferredHost(ctx, host) {
				preferredHosts = append(preferredHosts, hostAddress)
			}

			address := host.GetAddress()

			log.WithFields(log.Fields{
				"host": hostAddress,
			}).Info("Loading partition metadata for host")

			for _, hostDomain := range host.GetAssignedDomains(ctx) {

				domain, err := hostDomain.GetDomain(ctx, p.coordinator)
				log.WithFields(log.Fields{
					"host":   hostAddress,
					"domain": domain.GetName(),
				}).Info("Found domain assigned to host")

				if err != nil {
					return err
				}

				domainId := domain.GetId()

				if domain == nil {
					return errors.New("Domain not found " + strconv.Itoa(int(domainId)))
				}

				partitionToAddresses := domainToPartToAddresses[domainId]

				if partitionToAddresses == nil {
					partitionToAddresses = make(map[iface.PartitionID][]*iface.PartitionServerAddress)
					domainToPartToAddresses[domainId] = partitionToAddresses
				}

				for _, partition := range hostDomain.GetPartitions() {

					if !partition.IsDeletable() {

						partNum := partition.GetPartitionNumber()
						hostAddresses := partitionToAddresses[partNum]
						if hostAddresses == nil {
							hostAddresses = []*iface.PartitionServerAddress{}
						}

						partitionToAddresses[partNum] = append(hostAddresses, address)

					}
				}
			}

			addressStr := address.Print()
			pool := p.serverToConnections[addressStr]
			opts := p.options

			if pool == nil {

				log.WithFields(log.Fields{
					"NumConnectionsPerHost":        opts.NumConnectionsPerHost,
					"TryLockTimeoutMs":             opts.TryLockTimeoutMs,
					"EstablishConnectionTimeoutMs": opts.EstablishConnectionTimeoutMs,
					"QueryTimeoutMs":               opts.QueryTimeoutMs,
					"host":                         hostAddress,
				}).Info("Establishing connections to host")

				var wg sync.WaitGroup
				wg.Add(int(opts.NumConnectionsPerHost))

				connections := make(chan *HostConnection, int(opts.NumConnectionsPerHost))

				for i := 0; i < int(opts.NumConnectionsPerHost); i++ {
					go func() {
						defer wg.Done()

						connection, err := NewHostConnection(
							host,
							opts.TryLockTimeoutMs,
							opts.EstablishConnectionTimeoutMs,
							opts.EstablishConnectionRetries,
							opts.QueryTimeoutMs,
							opts.BulkQueryTimeoutMs,
						)

						if err == nil {
							connections <- connection
						} else {
							log.WithError(err).Warn("error connecting to host")
						}

					}()
				}

				log.Info("Waiting for connections to complete")
				wg.Wait()

				p.numCreatedConnections += int(opts.NumConnectionsPerHost)

				close(connections)

				var hostConnections []*HostConnection

				for connection := range connections {
					hostConnections = append(hostConnections, connection)
					host.AddStateChangeListener(connection)
				}

				pool, err = CreateHostConnectionPool(hostConnections, NO_SEED, preferredHosts)

				if err != nil {
					return err
				}
			}

			if pool != nil {
				newServerToConnections[addressStr] = pool
			}
		}
	}

	for domainID, connections := range domainToPartToAddresses {

		partitionToConnections := make(map[iface.PartitionID]*HostConnectionPool)

		for partitionID, addresses := range connections {

			var connections []*HostConnection
			for _, address := range addresses {
				addr := address.Print()

				if newServerToConnections[addr] != nil {
					connections = append(connections, newServerToConnections[addr].GetConnections()...)
				}

			}

			servingConnections := countServingConnections(connections)

			if servingConnections < int(p.options.MinConnectionsPerPartition) {
				return errors.New(fmt.Sprintf("Could not establish %v connections to partition %v for domain %v", p.options.MinConnectionsPerPartition, partitionID, domainID))
			}

			partitionToConnections[partitionID], err = CreateHostConnectionPool(connections,
				getHostListShuffleSeed(domainID, partitionID),
				preferredHosts,
			)
		}

		newDomainToPartitionToConnections[domainID] = partitionToConnections

	}

	return nil
}

func countServingConnections(connections []*HostConnection) int {

	connected := 0
	for _, connection := range connections {
		if !connection.IsDisconnected() {
			connected++
		}
	}
	return connected
}

func getHostListShuffleSeed(domainId iface.DomainID, partitionId iface.PartitionID) int64 {
	return (int64(domainId) + 1) * (int64(partitionId) + 1)
}
