package sstable

import (
	"github.com/huayichai/goleveldb/db"
	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
)

type SSTable struct {
	file       log.RandomAccessFile
	footer     Footer
	indexBlock *Block
	dataBlock  *Block
}

func OpenSSTable(file log.RandomAccessFile, size uint64) (*SSTable, db.Status) {
	var table SSTable
	table.file = file

	// Read the footer
	footer_data, status := table.file.Read(size-uint64(KEncodedLength), uint32(KEncodedLength))
	if !status.OK() {
		return nil, status
	}
	table.footer.DecodeFrom(footer_data)

	// Read the index block
	table.indexBlock, status = table.readBlock(table.footer.IndexBlockHandle)
	if !status.OK() {
		return nil, status
	}

	// Read the data block
	table.dataBlock, status = table.readBlock(BlockHandle{Offset: 0, Size: table.footer.IndexBlockHandle.Offset})
	if !status.OK() {
		return nil, status
	}

	return &table, status
}

func (table *SSTable) Get(key []byte) ([]byte, db.Status) {
	cur := uint32(0)
	for cur < table.dataBlock.Size {
		n, cur_key, value := DecodeEntryFrom(table.dataBlock.Data, cur)
		cmp := internal.Compare(cur_key, key)
		if cmp == 0 {
			return value, db.StatusOK()
		} else if cmp > 0 {
			return nil, db.StatusNotFound("")
		}
		cur += n
	}
	return nil, db.StatusNotFound("")
}

func (table *SSTable) readBlock(blockHandle BlockHandle) (*Block, db.Status) {
	var block Block
	var status db.Status
	block.Size = uint32(blockHandle.Size)
	block.Data, status = table.file.Read(blockHandle.Offset, uint32(blockHandle.Size))
	if !status.OK() {
		return nil, status
	}
	return &block, status
}
