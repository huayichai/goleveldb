package goleveldb

import (
	"math"
	"math/rand"
	"testing"
)

func Test_UVarint_basic(t *testing.T) {
	var a uint32 = math.MaxUint32
	buf1 := make([]byte, 10)
	len := EncodeUVarint32(buf1, a)
	if len != 5 {
		t.Fatal("EncodeUVarint32 failed")
	}
	b, len := DecodeUVarint32(buf1)
	if len != 5 || b != a {
		t.Fatal("DecodeUVarint32 failed")
	}

	var c uint64 = math.MaxUint64
	buf2 := make([]byte, 10)
	len = EncodeUVarint64(buf2, c)
	if len != 10 {
		t.Fatal("EncodeUVarint64 failed")
	}
	d, len := DecodeUVarint64(buf2)
	if len != 10 || d != c {
		t.Fatal("DecodeUVarint32 failed")
	}
}

func Test_Varint_random(t *testing.T) {
	n := 1000
	buf := make([]byte, n*5)
	var data []uint32

	var offset uint32 = 0
	for i := 0; i < n; i++ {
		k := rand.Uint32()
		data = append(data, k)
		l := EncodeUVarint32(buf[offset:], k)
		offset += l
	}
	offset = 0
	for i := 0; i < n; i++ {
		v, l := DecodeUVarint32(buf[offset:])
		if v != data[i] {
			t.Fatal("UVarint32 Encode or Decode failed")
		}
		offset += l
	}
}
