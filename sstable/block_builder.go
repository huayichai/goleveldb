package sstable

import (
	"github.com/huayichai/goleveldb/internal"
)

// return code_len, internal_key, value
func DecodeEntryFrom(data []byte, offset uint32) (uint32, internal.InternalKey, []byte) {
	// | internal_key_size(4B) | value_size(4B) | internal_key | value |
	internal_key_size := internal.DecodeFixed32(data[offset:])
	value_size := internal.DecodeFixed32(data[offset+4:])
	internal_key_offset := offset + 8
	value_offset := internal_key_offset + internal_key_size

	code_len := 8 + internal_key_size + value_size
	internal_key := internal.InternalKey(data[internal_key_offset:value_offset])
	value := data[value_offset : value_offset+value_size]
	return code_len, internal_key, value
}

type BlockBuilder struct {
	buffer  []byte // Destination buffer
	counter uint32 // Number of entries in block
}

func (blockBuilder *BlockBuilder) Add(key internal.InternalKey, value []byte) {
	// | internal_key_size(4 byte) | value_size(4 byte) | internal_key | value |
	p := make([]byte, 8)
	key_size := uint32(len(key))
	value_size := uint32(len(value))
	internal.EncodeFixed32(p, key_size)
	internal.EncodeFixed32(p[4:8], value_size)
	blockBuilder.buffer = append(blockBuilder.buffer, p...)
	blockBuilder.buffer = append(blockBuilder.buffer, []byte(key)...)
	blockBuilder.buffer = append(blockBuilder.buffer, value...)
	blockBuilder.counter++
}

func (blockBuilder *BlockBuilder) Finish() []byte {
	// internal.EncodeTo(&blockBuilder.buffer, blockBuilder.counter)
	return blockBuilder.buffer
}

func (blockBuilder *BlockBuilder) CurrentSizeEstimate() uint32 {
	return uint32(len(blockBuilder.buffer))
}

func (blockBuilder *BlockBuilder) Reset() {
	blockBuilder.counter = 0
	blockBuilder.buffer = blockBuilder.buffer[0:0]
}

func (blockBuilder *BlockBuilder) Empty() bool {
	return blockBuilder.CurrentSizeEstimate() == 0
}
