package sstable

import (
	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
)

type TableBuilder struct {
	options           *internal.Options
	file              log.WritableFile
	status            error
	offset            uint64
	dataBlockBuilder  BlockBuilder
	indexBlockBuilder BlockBuilder
	pendingIndexEntry bool
	pendingHandle     BlockHandle
	lastKey           internal.InternalKey
}

func NewTableBuilder(options *internal.Options, file log.WritableFile) *TableBuilder {
	return &TableBuilder{
		options:           options,
		file:              file,
		offset:            0,
		pendingIndexEntry: false,
	}
}

// Add entry to data block
// If the the data block exceeds the threshold, flush and insert an index in the index block.
func (builder *TableBuilder) Add(key internal.InternalKey, value []byte) {
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.lastKey, builder.pendingHandle.EncodeTo())
		builder.pendingIndexEntry = false
	}

	builder.dataBlockBuilder.Add(key, value)

	if builder.dataBlockBuilder.CurrentSizeEstimate() >= builder.options.BlockSize {
		builder.flush()
	}

	builder.lastKey = key
}

func (builder *TableBuilder) flush() {
	if builder.dataBlockBuilder.Empty() {
		return
	}
	builder.pendingHandle = builder.writeBlock(&builder.dataBlockBuilder)
	if builder.status == nil {
		builder.pendingIndexEntry = true
		builder.file.Flush()
	}
}

func (builder *TableBuilder) writeBlock(blockBuilder *BlockBuilder) BlockHandle {
	blockContent := blockBuilder.Finish()
	blockSize := len(blockContent)

	var handle BlockHandle
	handle.Offset = builder.offset
	handle.Size = uint64(blockSize)
	builder.offset += uint64(blockSize)

	builder.status = builder.file.Append(string(blockContent))

	blockBuilder.Reset()
	return handle
}

func (builder *TableBuilder) Finish() {
	builder.flush()

	// Write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.lastKey, builder.pendingHandle.EncodeTo())
		builder.pendingIndexEntry = false
	}
	indexBlockHandle := builder.writeBlock(&builder.indexBlockBuilder)

	// write footer block
	footer := Footer{IndexBlockHandle: indexBlockHandle}
	builder.status = builder.file.Append(footer.EncodeTo())

	// close sstable
	builder.file.Close()
}

func (builder *TableBuilder) FileSize() uint64 {
	return builder.offset
}
