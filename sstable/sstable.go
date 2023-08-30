package sstable

import (
	"encoding/binary"

	"github.com/huayichai/goleveldb/log"
)

type SSTable struct {
	file       log.RandomAccessFile
	footer     Footer
	indexBlock *Block
	dataBlock  *Block
}

func OpenSSTable(file log.RandomAccessFile, size uint64) (*SSTable, error) {
	var table SSTable
	table.file = file

	// Read the footer
	footer_data, err := table.file.Read(size-uint64(KEncodedLength), uint32(KEncodedLength))
	if err != nil {
		return nil, err
	}
	table.footer.DecodeFrom(footer_data)

	// Read the index block
	table.indexBlock, err = NewBlock(table.file, table.footer.IndexBlockHandle)
	if err != nil {
		return nil, err
	}

	// Read the data block
	table.dataBlock, err = NewBlock(table.file, BlockHandle{Offset: 0, Size: table.footer.IndexBlockHandle.Offset})
	if err != nil {
		return nil, err
	}

	return &table, nil
}

// Firstly, locate the block according to the index block,
// and then search by sequential traversal.
func (table *SSTable) Get(key []byte) ([]byte, error) {
	// search in index block
	_, pos_bytes, err := table.indexBlock.Get(0, key)
	if err != nil {
		return nil, err
	}
	offset := binary.LittleEndian.Uint32(pos_bytes)

	// search in data block
	_, v, err := table.dataBlock.Get(offset, key)
	return v, err
}
