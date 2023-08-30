package sstable

import (
	"fmt"

	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
)

type Block struct {
	Data []byte
	Size uint32
}

// Read block data from file
func NewBlock(file log.RandomAccessFile, blockHandle BlockHandle) (*Block, error) {
	var block Block
	var err error
	block.Size = uint32(blockHandle.Size)
	block.Data, err = file.Read(blockHandle.Offset, uint32(blockHandle.Size))
	if err != nil {
		return nil, err
	}
	return &block, err
}

func (block *Block) Get(offset uint32, key []byte) ([]byte, []byte, error) {
	cur_offset := offset
	for cur_offset < block.Size {
		n, cur_key, value := DecodeEntryFrom(block.Data, cur_offset)
		cmp := internal.Compare(cur_key, key)
		if cmp >= 0 {
			return cur_key, value, nil
		}
		cur_offset += n
	}
	return nil, nil, fmt.Errorf("%s", "Not Found")
}

type BlockIterator struct {
	data    []byte
	current uint32 // current_ is offset in data of current entry.
	key     []byte
	value   []byte
}

func NewBlockIterator(block *Block) *BlockIterator {
	var iter BlockIterator
	iter.data = block.Data
	iter.current = 0
	iter.SeekToFirst()
	return &iter
}

func (iter *BlockIterator) Valid() bool {
	return iter.current < uint32(len(iter.data))
}

func (iter *BlockIterator) SeekToFirst() {
	iter.current = 0
	_, iter.key, iter.value = DecodeEntryFrom(iter.data, 0)
}

func (iter *BlockIterator) SeekToLast() {
	panic("BlockIterator.SeekToLast() Unimplement!")
}

func (iter *BlockIterator) Seek(target interface{}) {
	for iter.Valid() && internal.Compare(target.([]byte), iter.key) < 0 {
		iter.Next()
	}
}

func (iter *BlockIterator) Next() {
	if !iter.Valid() {
		return
	}

	// next key-value entry offset
	iter.current += uint32(8 + len(iter.key) + len(iter.value))

	// decode from bytes
	_, iter.key, iter.value = DecodeEntryFrom(iter.data, iter.current)
}

func (iter *BlockIterator) Prev() {
	panic("BlockIterator.Prev() Unimplement!")
}

func (iter *BlockIterator) Key() []byte {
	return iter.key
}

func (iter *BlockIterator) Value() []byte {
	return iter.value
}

var Iterator = (*BlockIterator)(nil)
