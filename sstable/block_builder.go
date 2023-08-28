package sstable

import (
	"bytes"

	"github.com/huayichai/goleveldb/internal"
)

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
	internal.EncodeTo(&blockBuilder.buffer, blockBuilder.counter)
	return blockBuilder.buffer.Bytes()
}

func (blockBuilder *BlockBuilder) CurrentSizeEstimate() uint32 {
	return uint32(blockBuilder.buffer.Len())
}
