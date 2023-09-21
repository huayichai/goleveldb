package goleveldb

import (
	"encoding/binary"
)

// Decode SSTable Entry from [offset:]byte
// return decode_len, internal_key, value
func decodeSSTableEntryFrom(data []byte, offset uint32) (uint32, InternalKey, []byte) {
	// | internal_key_size(1~5B) | value_size(1~5B) | internal_key | value |
	internal_key_size, l1 := DecodeUVarint32(data[offset:])
	value_size, l2 := DecodeUVarint32(data[offset+uint32(l1):])
	internal_key_offset := offset + uint32(l1+l2)
	value_offset := internal_key_offset + internal_key_size

	decode_len := uint32(l1+l2) + internal_key_size + value_size
	internal_key := InternalKey(data[internal_key_offset:value_offset])
	value := data[value_offset : value_offset+value_size]
	return decode_len, internal_key, value
}

// blockHandle describes the position of block in sstable
type blockHandle struct {
	offset uint64
	size   uint64
}

func (handle *blockHandle) encodeTo() []byte {
	p := make([]byte, 16)
	EncodeFixed64(p, handle.offset)
	EncodeFixed64(p[8:], handle.size)
	return p
}

func (handle *blockHandle) decodeFrom(buf []byte) {
	handle.offset = DecodeFixed64(buf)
	handle.size = DecodeFixed64(buf[8:])
}

// footer at the end of sstable
type footer struct {
	metaIndexHandle  blockHandle
	indexblockHandle blockHandle
}

const (
	kFooterEncodedLength int = 32
)

func (f *footer) encodeTo() []byte {
	p := make([]byte, kFooterEncodedLength)
	binary.LittleEndian.PutUint64(p[0:8], f.metaIndexHandle.offset)
	binary.LittleEndian.PutUint64(p[8:16], f.metaIndexHandle.size)
	binary.LittleEndian.PutUint64(p[16:24], f.indexblockHandle.offset)
	binary.LittleEndian.PutUint64(p[24:32], f.indexblockHandle.size)
	return p
}

func (f *footer) decodeFrom(data []byte) {
	f.metaIndexHandle.offset = binary.LittleEndian.Uint64(data[0:8])
	f.metaIndexHandle.size = binary.LittleEndian.Uint64(data[8:16])
	f.indexblockHandle.offset = binary.LittleEndian.Uint64(data[16:24])
	f.indexblockHandle.size = binary.LittleEndian.Uint64(data[24:32])
}

type block struct {
	data       []byte
	restarts   []byte
	n_restarts uint32
}

func newBlock(buf []byte) *block {
	var b block
	buf_len := len(buf)
	restart_end := buf_len - 4
	b.n_restarts = DecodeFixed32(buf[restart_end:])
	restart_begin := restart_end - int(b.n_restarts)*4
	b.restarts = buf[restart_begin:restart_end]
	b.data = buf[0:restart_begin]
	return &b
}

// Get the first key that greater or equal than lookup_key
func (b *block) get(key InternalKey) ([]byte, []byte, error) {
	iter := newBlockIterator(b)
	iter.Seek(key)
	if iter.Valid() {
		return iter.key, iter.value, nil
	} else {
		return nil, nil, ErrKeyNotFound
	}
}

func (b *block) getRestartPoint(index uint32) uint32 {
	return DecodeFixed32(b.restarts[index*4:])
}

// Decode entry from offset
// Entry -> | shared(1~5B) | non_shared(1~5B) | valye_len(1~5B) | key | value |
// Return shared, non_shared, value_len, k, v, encode_len
func (b *block) decodeEntry(offset uint32) (uint32, uint32, uint32, []byte, []byte, uint32) {
	tmp := offset
	shared, l1 := DecodeUVarint32(b.data[offset:])
	offset += l1
	non_shared, l2 := DecodeUVarint32(b.data[offset:])
	offset += l2
	value_len, l3 := DecodeUVarint32(b.data[offset:])
	offset += l3
	k := b.data[offset : offset+non_shared]
	offset += non_shared
	v := b.data[offset : offset+value_len]
	offset += value_len
	return shared, non_shared, value_len, k, v, offset - tmp
}

type blockIterator struct {
	b          *block
	cur_offset uint32 // the scan offset in current restart region
	key        InternalKey
	value      []byte
}

func newBlockIterator(b *block) *blockIterator {
	var iter blockIterator
	iter.b = b
	iter.key = nil
	return &iter
}

func (iter *blockIterator) Valid() bool {
	return iter.key != nil
}

func (iter *blockIterator) SeekToFirst() {
	iter.cur_offset = 0
	iter.Next()
}

func (iter *blockIterator) SeekToRestartPoint(index uint32) {
	iter.cur_offset = iter.b.getRestartPoint(index)
	iter.Next()
}

// Seek the first key that graeter ot equal than target
// If all keys in block less than target, return the last key value.
func (iter *blockIterator) Seek(target interface{}) {
	// binary search by resarts
	left := uint32(0)
	right := iter.b.n_restarts - 1
	for left < right {
		mid := (left + right + 1) / 2
		region_offset := iter.b.getRestartPoint(mid)
		_, _, _, k, _, _ := iter.b.decodeEntry(region_offset)
		// use Compare not InternalKeyCompare due to indexblock not store internalkey
		cmp := Compare(k, target.(InternalKey).ExtractUserKey())
		if cmp < 0 { // k < key
			left = mid
		} else if cmp > 0 { // k > key
			right = mid - 1
		} else {
			break
		}
	}

	iter.SeekToRestartPoint(left)

	for ; iter.nextValid(); iter.Next() {
		if Compare(iter.key, target.(InternalKey).ExtractUserKey()) >= 0 {
			return
		}
	}
}

func (iter *blockIterator) nextValid() bool {
	return iter.cur_offset < uint32(len(iter.b.data))
}

func (iter *blockIterator) Next() {
	if !iter.nextValid() {
		iter.key = nil
		iter.value = nil
		return
	}
	var encode_len uint32
	iter.key, iter.value, encode_len = iter.parseNextEntry()
	iter.cur_offset += encode_len
}

// Return next_key, value, encode_len
func (iter *blockIterator) parseNextEntry() (InternalKey, []byte, uint32) {
	shared, non_shared, _, k, v, encode_len := iter.b.decodeEntry(iter.cur_offset)
	next_key := make([]byte, shared+non_shared)
	if shared == 0 { // restart
		next_key = k
	} else {
		copy(next_key[0:shared], iter.key[0:shared])
		copy(next_key[shared:shared+non_shared], k[0:non_shared])
	}
	next_value := v
	return next_key, next_value, encode_len
}

func (iter *blockIterator) Key() []byte {
	return iter.key
}

func (iter *blockIterator) Value() []byte {
	return iter.value
}

var _ Iterator = (*blockIterator)(nil)

type sstable struct {
	footer     footer
	indexblock *block // the offset of block in datablocks
	datablocks []byte
}

func openSSTable(filepath string) (*sstable, error) {
	var table sstable
	file, err := NewLinuxFile(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	size := uint64(file.Size())

	// Read the footer
	footer_data, err := file.Read(size-uint64(kFooterEncodedLength), uint32(kFooterEncodedLength))
	if err != nil {
		return nil, err
	}
	table.footer.decodeFrom(footer_data)

	// Read all data blocks buf
	table.datablocks, err = file.Read(0, uint32(table.footer.indexblockHandle.offset))
	if err != nil {
		return nil, err
	}

	// Read index block buf
	index_block_buf, err := file.Read(table.footer.indexblockHandle.offset, uint32(table.footer.indexblockHandle.size))
	if err != nil {
		return nil, err
	}

	// Construct index block
	table.indexblock = newBlock(index_block_buf)

	return &table, nil
}

// Firstly, locate the block according to the index block,
// and then search by sequential traversal.
func (table *sstable) get(key InternalKey) ([]byte, error) {
	iter := newSSTableIterator(table)
	iter.Seek(key)
	k := InternalKey(iter.Key())
	if UserKeyCompare(k.ExtractUserKey(), key.ExtractUserKey()) != 0 {
		return nil, ErrKeyNotFound
	} else if k.ExtractValueType() == KTypeDeletion {
		return nil, errKeyDeleted
	}
	return iter.Value(), nil
}

type sstableIterator struct {
	table            *sstable
	index_block_iter *blockIterator
	data_block_iter  *blockIterator
}

func newSSTableIterator(table *sstable) *sstableIterator {
	var iter sstableIterator
	iter.table = table
	return &iter
}

func (iter *sstableIterator) Valid() bool {
	if iter.data_block_iter.Valid() {
		return true
	}
	iter.nextDataBlock()
	return iter.data_block_iter.Valid()
}

func (iter *sstableIterator) SeekToFirst() {
	if iter.index_block_iter == nil {
		iter.index_block_iter = newBlockIterator(iter.table.indexblock)
	}
	iter.index_block_iter.SeekToFirst()
	var handle blockHandle
	handle.decodeFrom(iter.index_block_iter.value)
	block_data := newBlock(iter.table.datablocks[handle.offset : handle.offset+handle.size])
	iter.data_block_iter = newBlockIterator(block_data)
	iter.data_block_iter.SeekToFirst()
}

func (iter *sstableIterator) Seek(target interface{}) {
	if iter.index_block_iter == nil {
		iter.index_block_iter = newBlockIterator(iter.table.indexblock)
	}

	var handle blockHandle
	iter.index_block_iter.Seek(target)
	handle.decodeFrom(iter.index_block_iter.value)
	iter.parseDataBlock(&handle)

	iter.data_block_iter.Seek(target)
}

func (iter *sstableIterator) Next() {
	if iter.data_block_iter.Valid() {
		iter.data_block_iter.Next()
	} else {
		iter.nextDataBlock()
	}
}

func (iter *sstableIterator) nextDataBlock() {
	if iter.index_block_iter.Valid() {
		iter.index_block_iter.Next()
		if iter.index_block_iter.Valid() {
			var handle blockHandle
			handle.decodeFrom(iter.index_block_iter.value)
			iter.parseDataBlock(&handle)
			iter.data_block_iter.SeekToFirst()
		}
	}
}

func (iter *sstableIterator) parseDataBlock(handle *blockHandle) {
	block_data := iter.table.datablocks[handle.offset : handle.offset+handle.size]
	iter.data_block_iter = newBlockIterator(newBlock(block_data))
}

func (iter *sstableIterator) Key() []byte {
	return iter.data_block_iter.key
}

func (iter *sstableIterator) Value() []byte {
	return iter.data_block_iter.value
}

var _ Iterator = (*sstableIterator)(nil)
