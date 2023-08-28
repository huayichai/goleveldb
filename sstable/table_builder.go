package sstable

import (
	"github.com/huayichai/goleveldb/db"
	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
)

type TableBuilder struct {
	options           *internal.Options
	file              log.WritableFile
	status            db.Status
	offset            uint64
	dataBlockBuilder  BlockBuilder
	indexBlockBuilder BlockBuilder
	pendingIndexEntry bool
	pendingHandle     BlockHandle
}

func NewTableBuilder(options *internal.Options, file log.WritableFile) *TableBuilder {
	return &TableBuilder{
		options:           options,
		file:              file,
		offset:            0,
		pendingIndexEntry: false,
	}
}

func (builder *TableBuilder) Add(key, value string) {
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(key, builder.pendingHandle.EncodeTo())
		builder.pendingIndexEntry = false
	}

	builder.dataBlockBuilder.Add(key, value)

	if builder.dataBlockBuilder.CurrentSizeEstimate() >= builder.options.BlockSize {
		builder.flush()
	}
}

func (builder *TableBuilder) flush() {
	builder.pendingHandle = builder.writeBlock(&builder.dataBlockBuilder)
	if builder.status.OK() {
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
	return handle
}

func (builder *TableBuilder) Finish() {
	builder.flush()
}
