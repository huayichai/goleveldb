package goleveldb

import (
	"encoding/binary"
)

// Decode SSTable Entry from [offset:]byte
// return decode_len, internal_key, value
func decodeSSTableEntryFrom(data []byte, offset uint32) (uint32, InternalKey, []byte) {
	// | internal_key_size(4B) | value_size(4B) | internal_key | value |
	internal_key_size := DecodeFixed32(data[offset:])
	value_size := DecodeFixed32(data[offset+4:])
	internal_key_offset := offset + 8
	value_offset := internal_key_offset + internal_key_size

	decode_len := 8 + internal_key_size + value_size
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

// tableBuilder build the sstable
type tableBuilder struct {
	options           *Options
	file              WritableFile
	status            error
	offset            uint64
	dataBlockBuilder  blockBuilder
	indexBlockBuilder blockBuilder
	pendingIndexEntry bool
	pendingHandle     blockHandle
	lastKey           InternalKey
}

func newTableBuilder(options *Options, file WritableFile) *tableBuilder {
	return &tableBuilder{
		options:           options,
		file:              file,
		offset:            0,
		pendingIndexEntry: false,
	}
}

// Add entry to data block
// If the the data block exceeds the threshold, flush and insert an index in the index block.
func (builder *tableBuilder) add(key InternalKey, value []byte) {
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.add(builder.lastKey, builder.pendingHandle.encodeTo())
		builder.pendingIndexEntry = false
	}

	builder.dataBlockBuilder.add(key, value)

	if builder.dataBlockBuilder.currentSizeEstimate() >= builder.options.BlockSize {
		builder.flush()
	}

	builder.lastKey = key
}

func (builder *tableBuilder) flush() {
	if builder.dataBlockBuilder.empty() {
		return
	}
	builder.pendingHandle = builder.writeblock(&builder.dataBlockBuilder)
	if builder.status == nil {
		builder.pendingIndexEntry = true
		builder.file.Sync()
	}
}

func (builder *tableBuilder) writeblock(blockBuilder *blockBuilder) blockHandle {
	blockContent := blockBuilder.finish()
	blockSize := len(blockContent)

	var handle blockHandle
	handle.offset = builder.offset
	handle.size = uint64(blockSize)
	builder.offset += uint64(blockSize)

	builder.status = builder.file.Append(string(blockContent))

	blockBuilder.reset()
	return handle
}

func (builder *tableBuilder) finish() {
	builder.flush()

	// Write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.add(builder.lastKey, builder.pendingHandle.encodeTo())
		builder.pendingIndexEntry = false
	}
	indexblockHandle := builder.writeblock(&builder.indexBlockBuilder)

	// write footer block
	footer := footer{indexblockHandle: indexblockHandle}
	builder.status = builder.file.Append(footer.encodeTo())

	// close sstable
	builder.file.Close()
}

func (builder *tableBuilder) fileSize() uint64 {
	return builder.offset
}

// blockBuilder builds the block in sstable
// block contains datablock and indexblock
type blockBuilder struct {
	buffer  []byte // Destination buffer
	counter uint32 // Number of entries in block
}

func (builder *blockBuilder) add(key InternalKey, value []byte) {
	// | internal_key_size(4 byte) | value_size(4 byte) | internal_key | value |
	p := make([]byte, 8)
	key_size := uint32(len(key))
	value_size := uint32(len(value))
	EncodeFixed32(p, key_size)
	EncodeFixed32(p[4:8], value_size)
	builder.buffer = append(builder.buffer, p...)
	builder.buffer = append(builder.buffer, []byte(key)...)
	builder.buffer = append(builder.buffer, value...)
	builder.counter++
}

func (builder *blockBuilder) finish() []byte {
	return builder.buffer
}

func (builder *blockBuilder) currentSizeEstimate() uint32 {
	return uint32(len(builder.buffer))
}

func (builder *blockBuilder) reset() {
	builder.counter = 0
	builder.buffer = builder.buffer[0:0]
}

func (builder *blockBuilder) empty() bool {
	return builder.currentSizeEstimate() == 0
}

// footer at the end of sstable
type footer struct {
	metaIndexHandle  blockHandle
	indexblockHandle blockHandle
}

const (
	kEncodedLength int = 32
)

func (f *footer) encodeTo() string {
	p := make([]byte, kEncodedLength)
	binary.LittleEndian.PutUint64(p[0:8], f.metaIndexHandle.offset)
	binary.LittleEndian.PutUint64(p[8:16], f.metaIndexHandle.size)
	binary.LittleEndian.PutUint64(p[16:24], f.indexblockHandle.offset)
	binary.LittleEndian.PutUint64(p[24:32], f.indexblockHandle.size)
	return string(p)
}

func (f *footer) decodeFrom(data []byte) {
	f.metaIndexHandle.offset = binary.LittleEndian.Uint64(data[0:8])
	f.metaIndexHandle.size = binary.LittleEndian.Uint64(data[8:16])
	f.indexblockHandle.offset = binary.LittleEndian.Uint64(data[16:24])
	f.indexblockHandle.size = binary.LittleEndian.Uint64(data[24:32])
}

type block struct {
	data []byte
	size uint32
}

// Read block data from file
func newBlock(file RandomAccessFile, blockHandle blockHandle) (*block, error) {
	var b block
	var err error
	b.size = uint32(blockHandle.size)
	b.data, err = file.Read(blockHandle.offset, uint32(blockHandle.size))
	if err != nil {
		return nil, err
	}
	return &b, err
}

func (b *block) get(offset uint32, key InternalKey) ([]byte, []byte, error) {
	cur_offset := offset
	for cur_offset < b.size {
		n, cur_key, value := decodeSSTableEntryFrom(b.data, cur_offset)
		cmp := InternalKeyCompare(cur_key, key)
		if cmp >= 0 {
			return cur_key, value, nil
		}
		cur_offset += n
	}
	return nil, nil, ErrKeyNotFound
}

type blockIterator struct {
	data             []byte
	current          uint32 // current_ is offset in data of current entry.
	key              InternalKey
	value            []byte
	current_code_len uint32 // current kv entry len
}

func newBlockIterator(b *block) *blockIterator {
	var iter blockIterator
	iter.data = b.data
	iter.current = 0
	iter.SeekToFirst()
	return &iter
}

func (iter *blockIterator) Valid() bool {
	return iter.current < uint32(len(iter.data))
}

func (iter *blockIterator) SeekToFirst() {
	iter.current = 0
	iter.current_code_len, iter.key, iter.value = decodeSSTableEntryFrom(iter.data, 0)
}

func (iter *blockIterator) SeekToLast() {
	panic("blockIterator.SeekToLast() Unimplement!")
}

func (iter *blockIterator) Seek(target interface{}) {
	for iter.Valid() && InternalKeyCompare(target.(InternalKey), iter.key) < 0 {
		iter.Next()
	}
}

func (iter *blockIterator) Next() {
	if !iter.Valid() {
		return
	}

	// next key-value entry offset
	iter.current += iter.current_code_len

	// decode from bytes
	iter.current_code_len, iter.key, iter.value = decodeSSTableEntryFrom(iter.data, iter.current)
}

func (iter *blockIterator) Prev() {
	panic("blockIterator.Prev() Unimplement!")
}

func (iter *blockIterator) Key() []byte {
	return iter.key
}

func (iter *blockIterator) Value() []byte {
	return iter.value
}

var _ Iterator = (*blockIterator)(nil)

type sstable struct {
	file       RandomAccessFile
	footer     footer
	indexblock *block
	datablock  *block
}

func openSSTable(file RandomAccessFile, size uint64) (*sstable, error) {
	var table sstable
	table.file = file

	// Read the footer
	footer_data, err := table.file.Read(size-uint64(kEncodedLength), uint32(kEncodedLength))
	if err != nil {
		return nil, err
	}
	table.footer.decodeFrom(footer_data)

	// Read the index block
	table.indexblock, err = newBlock(table.file, table.footer.indexblockHandle)
	if err != nil {
		return nil, err
	}

	// Read the data block
	table.datablock, err = newBlock(table.file, blockHandle{offset: 0, size: table.footer.indexblockHandle.offset})
	if err != nil {
		return nil, err
	}

	return &table, nil
}

// Firstly, locate the block according to the index block,
// and then search by sequential traversal.
func (table *sstable) get(key InternalKey) ([]byte, error) {
	// search in index block
	_, pos_bytes, err := table.indexblock.get(0, key)
	if err != nil {
		return nil, err
	}
	offset := DecodeFixed32(pos_bytes)

	// search in data block
	_, v, err := table.datablock.get(offset, key)
	return v, err
}

type sstableIterator struct {
	datablock *block
	data_iter *blockIterator
}

func newSSTableIterator(table *sstable) *sstableIterator {
	var iter sstableIterator
	iter.datablock = table.datablock
	iter.data_iter = newBlockIterator(table.datablock)
	return &iter
}

func (iter *sstableIterator) Valid() bool {
	return iter.data_iter.Valid()
}

func (iter *sstableIterator) SeekToFirst() {
	iter.data_iter.SeekToFirst()
}

func (iter *sstableIterator) SeekToLast() {
	iter.data_iter.SeekToLast()
}

func (iter *sstableIterator) Seek(target interface{}) {
	iter.data_iter.Seek(target)
}

func (iter *sstableIterator) Next() {
	iter.data_iter.Next()
}

func (iter *sstableIterator) Prev() {
	iter.data_iter.Prev()
}

func (iter *sstableIterator) Key() []byte {
	return iter.data_iter.Key()
}

func (iter *sstableIterator) Value() []byte {
	return iter.data_iter.Value()
}
