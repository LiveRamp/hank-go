package hank_client

import (
	"fmt"
	"github.com/bpodgursky/hank-go-client/fixtures"
	"github.com/bpodgursky/hank-go-client/iface"
	"github.com/curator-go/curator"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"github.com/bpodgursky/hank-go-client/zk_coordinator"
	"github.com/liveramp/hank/hank-core/src/main/go/hank"
)

func Exception() *hank.HankResponse {
	resp := &hank.HankResponse{}

	exception := &hank.HankException{}
	exception.InternalError = newStrRef("Internal Error")
	resp.Xception = exception

	return resp
}

func newStrRef(val string) *string {
	return &val
}

type CountingHandler struct {
	numGets          int
	numCompletedGets int

	internal InternalHandler
}

func (p *CountingHandler) Clear() {
	p.numGets = 0
	p.numCompletedGets = 0
}

func (p *CountingHandler) Get(domain_id int32, key []byte) (r *hank.HankResponse, err error) {

	p.numGets++
	response, err := p.internal.get(iface.DomainID(domain_id), key)

	if err == nil {
		p.numCompletedGets++
	} else {
		return nil, err
	}

	return response, nil

}
func (p *CountingHandler) GetBulk(domain_id int32, keys [][]byte) (r *hank.HankBulkResponse, err error) {
	return nil, nil
}

type InternalHandler interface {
	get(int iface.DomainID, key []byte) (*hank.HankResponse, error)
}

type ConstValue struct{ val *hank.HankResponse }

func (p *ConstValue) get(int iface.DomainID, key []byte) (*hank.HankResponse, error) {
	return p.val, nil
}

type FailingValue struct{}

func (p *FailingValue) get(int iface.DomainID, key []byte) (r *hank.HankResponse, err error) {
	return Exception(), nil
}

type HangingResponse struct{ lock *sync.Mutex }

func (p *HangingResponse) get(int iface.DomainID, key []byte) (r *hank.HankResponse, err error) {
	p.lock.Lock()
	return nil, nil
}

const Val1Str = "1"

func Val1() *hank.HankResponse {
	resp := &hank.HankResponse{}
	resp.Value = []byte(Val1Str)
	resp.NotFound = newFalse()
	return resp
}

func Key1() []byte {
	return []byte("1")
}

func setupHangingServerClient(t *testing.T, ctx *iface.ThreadCtx, client curator.CuratorFramework, i int, lock *sync.Mutex) (*CountingHandler, iface.Host, func(), *HostConnection) {
	handler := &CountingHandler{internal: &HangingResponse{lock: lock}}
	host, close, conn := setupServerClient(t, handler, ctx, client, i)
	return handler, host, close, conn
}

func setupCountingServerClient(t *testing.T, ctx *iface.ThreadCtx, client curator.CuratorFramework, i int) (*CountingHandler, iface.Host, func(), *HostConnection) {
	handler := &CountingHandler{internal: &ConstValue{val: Val1()}}
	host, close, conn := setupServerClient(t, handler, ctx, client, i)
	return handler, host, close, conn
}

func setupFailingServerClient(t *testing.T, ctx *iface.ThreadCtx, client curator.CuratorFramework, i int) (*CountingHandler, iface.Host, func(), *HostConnection) {
	handler := &CountingHandler{internal: &FailingValue{}}
	host, close, conn := setupServerClient(t, handler, ctx, client, i)
	return handler, host, close, conn
}

func setupServerClient(t *testing.T, server hank.PartitionServer, ctx *iface.ThreadCtx, client curator.CuratorFramework, i int) (iface.Host, func(), *HostConnection) {
	host, close := createHostServer(t, ctx, client, i, server)

	conn := NewHostConnection(host, 100, 100, 100, 100)
	fixtures.WaitUntilOrFail(t, func() bool {
		return conn.IsServing()
	})

	return host, func() { conn.Disconnect(); close() }, conn
}

func byAddress(connections []*HostConnection) map[string][]*HostConnection {
	hostConnections := make(map[string][]*HostConnection)

	for _, conn := range connections {
		addr := conn.host.GetAddress().Print()

		if _, ok := hostConnections[addr]; !ok {
			hostConnections[addr] = []*HostConnection{}
		}

		hostConnections[addr] = append(hostConnections[addr], conn)
	}

	return hostConnections
}

func setUpCoordinator(client curator.CuratorFramework) (*iface.ThreadCtx, iface.Domain) {
	ctx := iface.NewThreadCtx()
	cdr, _ := zk_coordinator.NewZkCoordinator(client,
		"/hank/domains",
		"/hank/ring_groups",
		"/hank/domain_groups",
	)
	domain, _ := cdr.AddDomain(ctx, "domain1", 1, "", "", "", nil)

	return ctx, domain
}

func TestBothUp(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	//	take one host down, expect all queries on the other

	host2.SetState(ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrFail(t, func() bool {
		return h2conn1.IsOffline()
	})

	handler1.Clear()
	handler2.Clear()

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 10, numHits)

	//	if both are down, give it a shot anyway

	host1.SetState(ctx, iface.HOST_OFFLINE)
	fixtures.WaitUntilOrFail(t, func() bool {
		return h1conn1.IsOffline()
	})

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestSimplePreferredPool(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, _, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{host1.GetAddress().Print()})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestBothPreferred(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{
		host1.GetAddress().Print(),
		host2.GetAddress().Print(),
	})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)

}

func TestPreferredFailing(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupFailingServerClient(t, ctx, client, 1)
	handler2, _, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{host1.GetAddress().Print()})

	//	with 2 attempts, everything should hit first server once, then the other

	numHits := queryKey(pool, domain, t, 10, 2, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 10, handler2.numGets)
	assert.Equal(t, 10, numHits)

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	//	with 1 attempt, everything should hit first server once and fail

	numHits = queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 0, handler2.numGets)
	assert.Equal(t, 0, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestPreferredFallback(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupFailingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)
	handler3, _, close3, h3conn1 := setupCountingServerClient(t, ctx, client, 3)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1, h3conn1}), NO_SEED, []string{
		host1.GetAddress().Print(),
		host2.GetAddress().Print(),
	})

	numHits := queryKey(pool, domain, t, 10, 2, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 10, handler2.numGets)
	assert.Equal(t, 0, handler3.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()
	close3()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestOneHankExceptions(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, _, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, _, close2, h2conn1 := setupFailingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{})

	numHits := queryKey(pool, domain, t, 10, 1, Val1Str)

	assert.Equal(t, 5, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 5, numHits)

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	numHits = queryKey(pool, domain, t, 10, 2, Val1Str)

	assert.Equal(t, 10, handler1.numGets)
	assert.Equal(t, 5, handler2.numGets)
	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestOneHanging(t *testing.T) {
	cluster, client := fixtures.SetupZookeeper(t)

	lock := sync.Mutex{}
	lock.Lock()

	ctx, domain := setUpCoordinator(client)

	handler1, _, close1, h1conn1 := setupHangingServerClient(t, ctx, client, 1, &lock)
	handler2, _, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	pool, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), NO_SEED, []string{})

	numHits := 0
	previousIface1NumGets := 0

	for i := 0; i < 10; i++ {
		fmt.Println("\n")
		val := pool.Get(domain, Key1(), 1, NO_HASH)
		if val.IsSetValue() {
			numHits++
		}

		//	looks confusing, but just making sure we hang until the retry succeeds
		if handler1.numGets != previousIface1NumGets {
			lock.Unlock()
			previousIface1NumGets = handler1.numGets
		}

	}

	fixtures.WaitUntilOrFail(t, func() bool {
		return handler1.numGets == 5 &&
			handler1.numCompletedGets == 5 &&
			handler2.numGets == 5 &&
			handler2.numCompletedGets == 5
	})

	assert.Equal(t, 5, numHits)

	handler1.Clear()
	handler2.Clear()
	numHits = 0

	for i := 0; i < 10; i++ {
		fmt.Println("\n")
		val := pool.Get(domain, Key1(), 2, NO_HASH)
		if val.IsSetValue() {
			numHits++
		}

		//	looks confusing, but just making sure we hang until the retry succeeds
		if handler1.numGets != previousIface1NumGets {
			lock.Unlock()
			previousIface1NumGets = handler1.numGets
		}

	}

	fixtures.WaitUntilOrFail(t, func() bool {
		return handler1.numGets == 5 &&
			handler1.numCompletedGets == 5 &&
			handler2.numGets == 10 &&
			handler2.numCompletedGets == 10
	})

	assert.Equal(t, 10, numHits)

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)
}

func TestConsistentHashing(t *testing.T) {

	cluster, client := fixtures.SetupZookeeper(t)

	ctx, domain := setUpCoordinator(client)

	handler1, host1, close1, h1conn1 := setupCountingServerClient(t, ctx, client, 1)
	handler2, host2, close2, h2conn1 := setupCountingServerClient(t, ctx, client, 2)

	for i := 0; i < 1024; i++ {

		numHits := 0

		poolA, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), int64(i), []string{
			host1.GetAddress().Print(),
			host2.GetAddress().Print(),
		})

		poolB, _ := NewHostConnectionPool(byAddress([]*HostConnection{h1conn1, h2conn1}), int64(i), []string{
			host1.GetAddress().Print(),
			host2.GetAddress().Print(),
		})

		keyHash := int64(42)

		for j := 0; j < 10; j++ {

			respA := poolA.Get(domain, []byte(Val1Str), 1, keyHash)
			respB := poolB.Get(domain, []byte(Val1Str), 1, keyHash)

			if respA.IsSetValue() && respB.IsSetValue() {
				numHits += 2
			}

		}

		allOne := (handler1.numGets == 20 && handler2.numGets == 0) || (handler1.numGets == 0 && handler2.numGets == 20)

		if !allOne {
			fmt.Println("\n")
			fmt.Println(handler1.numGets)
			fmt.Println(handler2.numGets)
		}

		assert.True(t, allOne)
		assert.True(t, numHits == 20)

		handler1.Clear()
		handler2.Clear()

		numHits = 0

	}

	close1()
	close2()

	fixtures.TeardownZookeeper(cluster, client)

}

func queryKey(pool *HostConnectionPool, domain iface.Domain, t *testing.T, times int, numTries int32, expected string) int {
	numHits := 0
	for i := 0; i < times; i++ {
		fmt.Println("\n")
		val := pool.Get(domain, Key1(), numTries, NO_HASH)
		if val.IsSetValue() {
			numHits++
		}
	}
	return numHits
}
