package sstable

import (
	"github.com/huayichai/goleveldb/internal"
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
func (table *SSTable) Get(key internal.InternalKey) ([]byte, error) {
	// search in index block
	_, pos_bytes, err := table.indexBlock.Get(0, key)
	if err != nil {
		return nil, err
	}
	offset := internal.DecodeFixed32(pos_bytes)

	// search in data block
	_, v, err := table.dataBlock.Get(offset, key)
	return v, err
}

type SSTableIterator struct {
	dataBlock *Block
	data_iter *BlockIterator
}

func NewSSTableIterator(table *SSTable) *SSTableIterator {
	var iter SSTableIterator
	iter.dataBlock = table.dataBlock
	iter.data_iter = NewBlockIterator(table.dataBlock)
	return &iter
}

func (iter *SSTableIterator) Valid() bool {
	return iter.data_iter.Valid()
}

func (iter *SSTableIterator) SeekToFirst() {
	iter.data_iter.SeekToFirst()
}

func (iter *SSTableIterator) SeekToLast() {
	iter.data_iter.SeekToLast()
}

func (iter *SSTableIterator) Seek(target interface{}) {
	iter.data_iter.Seek(target)
}

func (iter *SSTableIterator) Next() {
	iter.data_iter.Next()
}

func (iter *SSTableIterator) Prev() {
	iter.data_iter.Prev()
}

func (iter *SSTableIterator) Key() []byte {
	return iter.data_iter.Key()
}

func (iter *SSTableIterator) Value() []byte {
	return iter.data_iter.Value()
}
