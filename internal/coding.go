package internal

import "encoding/binary"

func EncodeFixed32(dst []byte, value uint32) {
	binary.LittleEndian.PutUint32(dst, value)
}

func EncodeFixed64(dst []byte, value uint64) {
	binary.LittleEndian.PutUint64(dst, value)
}

func DecodeFixed32(src []byte) uint32 {
	return binary.LittleEndian.Uint32(src)
}

func DecodeFixed64(src []byte) uint64 {
	return binary.LittleEndian.Uint64(src)
}
