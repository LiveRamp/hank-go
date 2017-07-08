package zk_coordinator

import (
	"github.com/aviddiviner/go-murmur"
)

type Murmur64Partitioner struct{}

const SEED = 645568

func abs64(val int64) int64{
	if val < 0 {
		return -val
	}
	return val
}

func (p *Murmur64Partitioner) Partition(key []byte, numPartitions int32) int64 {
	return abs64(int64(murmur.MurmurHash64A(key, SEED)) % int64(numPartitions))
}
