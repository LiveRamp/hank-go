package zk_coordinator

type Murmur64Partitioner struct{}

const SEED = 645568
const M = int64(-4132994306676758123)

func (p *Murmur64Partitioner) Partition(key []byte, numPartitions int32) int32 {
	return abs(int32(MurmurHash64(key, SEED)) % int32(numPartitions))
}

func abs(partition int32) int32{

	if partition < 0 {
		return -partition
	}
	return partition

}

//	this is probably * wrong * in a technical sense, but it matches Hank's Java implementation (hopefully)
func MurmurHash64(data []byte, seed int) (h int64) {

	r := 47

	len := len(data)
	hTemp := int64(uint64(seed) ^ uint64(int(len) * int(M)))

	remainder := len & 7

	end := len - remainder

	for i := 0; i < end; i += 8 {

		k := int64(int8(data[i+7]))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+6]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+5]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+4]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+3]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+2]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+1]))&0xff))
		k = k << 8

		k = int64(k | int64(int64(int8(data[i+0]))&0xff))
		k = k * M
		k = k ^ int64(uint64(k)>>uint64(r))
		k = k * M

		hTemp  = int64(uint64(hTemp) ^ uint64(k))

		hTemp = M * hTemp
	}

	switch remainder {
	case 7:
		hTemp = int64(uint64(hTemp) ^ uint64(uint64(uint8(data[end+6]) & uint8(0xff)) << uint(48)))
		fallthrough
	case 6:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end+5]) & uint8(0xff)) << uint(40))))
		fallthrough
	case 5:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end+4]) & uint8(0xff)) << uint(32))))
		fallthrough
	case 4:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end+3]) & uint8(0xff)) << uint(24))))
		fallthrough
	case 3:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end+2]) & uint8(0xff)) << uint(16))))
		fallthrough
	case 2:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end+1]) & uint8(0xff)) << uint(8))))
		fallthrough
	case 1:
		hTemp = int64(uint64(hTemp) ^ uint64(int64(uint64(uint8(data[end]) & uint8(0xff)))))
		hTemp = hTemp * M
	}

	hTemp = hTemp ^ int64(uint64(hTemp) >> uint(r))
	hTemp = int64(hTemp) * M
	hTemp = hTemp ^ int64(uint64(hTemp) >> uint64(r))

	return int64(hTemp)
}
