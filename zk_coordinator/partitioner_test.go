package zk_coordinator

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartitioner(t *testing.T) {

	partitioner := &Murmur64Partitioner{}
	bytes, _ := hex.DecodeString("6760db3664815e8114622a615cf491f5837c60e23862fe97ef2fb72f05281d07")
	partition := partitioner.Partition(bytes, 1024)
	assert.Equal(t, int32(765), partition)

	bytes = []byte("9q34poifj[9qvjafjqpfewajpf")
	partition = partitioner.Partition(bytes, 1024)
	assert.Equal(t, int32(1020), partition)

	bytes = []byte("f304pf4ijq3f90[qpefjqfieoawfjaws")
	partition = partitioner.Partition(bytes, 1024)
	assert.Equal(t, int32(193), partition)

}

func TestMurmur64(t *testing.T) {

	bytes, _ := hex.DecodeString("6760db3664815e8114622a615cf491f5837c60e23862fe97ef2fb72f05281d07")
	assert.Equal(t, int64(6343921159570942211), MurmurHash64(bytes, 645568))

	bytes = []byte("test12345")
	assert.Equal(t, int64(-7808825886808697493), MurmurHash64(bytes, 645568))

	bytes = []byte("934;wkfq3lfq")
	assert.Equal(t, int64(1582694255121429424), MurmurHash64(bytes, 645568))

	bytes = []byte("%#G%GVWC$WGDSSasdfaeaf4393q4u0ajerjeiaer")
	assert.Equal(t, int64(1912501296182416924), MurmurHash64(bytes, 645568))

	bytes = []byte("%#G%0[u8w34goijaviaevalkdscasdfjalks   ")
	assert.Equal(t, int64(435888642032577352), MurmurHash64(bytes, 645568))
}
