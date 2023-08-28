package sstable

import (
	"bytes"
	"encoding/binary"

	"github.com/huayichai/goleveldb/internal"
)

const (
	keySizeOffset   uint32 = 0
	valueSizeOffset uint32 = 4
	dataOffset      uint32 = 8
)

func DecodeEntryFrom(data []byte, offset uint32) (uint32, []byte, []byte) {
	key_size := binary.LittleEndian.Uint32(data[(offset + keySizeOffset):])
	value_size := binary.LittleEndian.Uint32(data[(offset + valueSizeOffset):])
	key_offset := offset + dataOffset
	value_offset := offset + dataOffset + key_size
	return key_size + value_size + 8, data[key_offset:value_offset], data[value_offset : value_offset+value_size]
}

type BlockBuilder struct {
	buffer  bytes.Buffer // Destination buffer
	counter uint32       // Number of entries in block
}

func (blockBuilder *BlockBuilder) Add(key string, value string) {
	// | key_size(4 byte) | value_size(4 byte) | key | value |
	key_size := int32(len(key))
	value_size := int32(len(value))
	internal.EncodeTo(&blockBuilder.buffer, key_size)
	internal.EncodeTo(&blockBuilder.buffer, value_size)
	internal.EncodeTo(&blockBuilder.buffer, []byte(key))
	internal.EncodeTo(&blockBuilder.buffer, []byte(value))
	blockBuilder.counter++
}

func (blockBuilder *BlockBuilder) Finish() []byte {
	// internal.EncodeTo(&blockBuilder.buffer, blockBuilder.counter)
	return blockBuilder.buffer.Bytes()
}

func (blockBuilder *BlockBuilder) CurrentSizeEstimate() uint32 {
	return uint32(blockBuilder.buffer.Len())
}

func (blockBuilder *BlockBuilder) Reset() {
	blockBuilder.counter = 0
	blockBuilder.buffer.Reset()
}

func (blockBuilder *BlockBuilder) Empty() bool {
	return blockBuilder.CurrentSizeEstimate() == 0
}
