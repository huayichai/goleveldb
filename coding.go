package goleveldb

import "encoding/binary"

// Encode uint32 into varint32.
// Return the encoded length.
func EncodeUVarint32(dst []byte, value uint32) uint32 {
	return uint32(binary.PutUvarint(dst, uint64(value)))
}

// Decode uint32 from varint32.
// Return the decoded value and encoded length.
func DecodeUVarint32(src []byte) (uint32, uint32) {
	value, len := binary.Uvarint(src)
	return uint32(value), uint32(len)
}

// Encode uint64 into varint64.
// Return the encoded length.
func EncodeUVarint64(dst []byte, value uint64) uint32 {
	return uint32(binary.PutUvarint(dst, value))
}

// Decode uint64 from varint64.
// Return the decoded value and encoded length.
func DecodeUVarint64(src []byte) (uint64, uint32) {
	value, len := binary.Uvarint(src)
	return value, uint32(len)
}

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
