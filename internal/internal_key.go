package internal

import (
	"bytes"
	"encoding/binary"
	"io"
)

func EncodeTo(w io.Writer, data any) error {
	return binary.Write(w, binary.LittleEndian, data)
}

func EncodeInternalKVEntry(key, value []byte) []byte {
	key_len := len(key)
	value_len := len(value)
	size := (key_len + value_len + 8)
	code := make([]byte, size)
	copy(code[0:key_len], key)
	copy(code[key_len:key_len+value_len], value)
	binary.LittleEndian.PutUint32(code[key_len+value_len:], uint32(key_len))
	binary.LittleEndian.PutUint32(code[key_len+value_len+4:], uint32(value_len))
	return code
}

func DecodeInternalKVEntryFrom(data []byte) ([]byte, []byte) {
	len := len(data)
	key_len := binary.LittleEndian.Uint32(data[len-8:])
	value_len := binary.LittleEndian.Uint32(data[len-4:])
	key_code := data[0:key_len]
	value_code := data[key_len : key_len+value_len]
	return key_code, value_code
}

func DecodeInternalKeyFrom(data []byte) []byte {
	len := len(data)
	key_len := binary.LittleEndian.Uint32(data[len-8:])
	return data[0:key_len]
}

func DecodeInternalValueFrom(data []byte) []byte {
	len := len(data)
	key_len := binary.LittleEndian.Uint32(data[len-8:])
	value_len := binary.LittleEndian.Uint32(data[len-4:])
	return data[key_len : key_len+value_len]
}

func PutLengthPrefixedSlice(value []byte) []byte {
	size := uint32(len(value))
	p := make([]byte, 4+size)
	binary.LittleEndian.PutUint32(p, size)
	copy(p[4:], value)
	return p
}

func GetLengthPrefixedSlice(input []byte) ([]byte, uint32) {
	size := binary.LittleEndian.Uint32(input[0:4])
	value_begin_offset := 4
	value_end_offset := value_begin_offset + int(size)
	return input[value_begin_offset:value_end_offset], 4 + size
}

func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
