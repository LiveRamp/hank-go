package hank_client

import "time"

type EnvironmentValue struct {
	Key   string
	Value string
}

type hankSmartClientOptions struct {
	NumConnectionsPerHost int32

	TryLockTimeoutMs             int32
	EstablishConnectionTimeoutMs int32
	QueryTimeoutMs               int32
	BulkQueryTimeoutMs           int32
	PreferredHostEnvironment     *EnvironmentValue
	QueryMaxNumTries             int32

	ResponseCacheEnabled  bool
	ResponseCacheNumItems int32
	ResponseCacheExpiryTime time.Duration
}

func NewHankSmartClientOptions() *hankSmartClientOptions {
	return &hankSmartClientOptions{
		NumConnectionsPerHost:        int32(1),
		TryLockTimeoutMs:             int32(1000),
		EstablishConnectionTimeoutMs: int32(1000),
		QueryTimeoutMs:               int32(1000),
		BulkQueryTimeoutMs:           int32(1000),
		ResponseCacheNumItems:        int32(1000),
		ResponseCacheExpiryTime:      time.Hour,
	}
}

func (p *hankSmartClientOptions) SetResponseCacheExpiryTime(time time.Duration) *hankSmartClientOptions {
	p.ResponseCacheExpiryTime = time
	return p
}

func (p *hankSmartClientOptions) SetNumConnectionsPerHost(connections int32) *hankSmartClientOptions {
	p.NumConnectionsPerHost = connections
	return p
}

func (p *hankSmartClientOptions) SetTryLockTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.TryLockTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetEstablishConnectionTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.EstablishConnectionTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetQueryTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.QueryTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetBulkQueryTimeoutMs(timeout int32) *hankSmartClientOptions {
	p.BulkQueryTimeoutMs = timeout
	return p
}

func (p *hankSmartClientOptions) SetResponseCacheEnabled(enabled bool) *hankSmartClientOptions {
	p.ResponseCacheEnabled = enabled
	return p
}

func (p *hankSmartClientOptions) SetResponseCacheNumItems(items int32) *hankSmartClientOptions {
	p.ResponseCacheNumItems = items
	return p
}

func (p *hankSmartClientOptions) SetQueryMaxNumTries(tries int32) *hankSmartClientOptions {
	p.QueryMaxNumTries = tries
	return p
}

func (p *hankSmartClientOptions) SetPreferredEnvironment(env *EnvironmentValue) *hankSmartClientOptions {
	p.PreferredHostEnvironment = env
	return p
}
