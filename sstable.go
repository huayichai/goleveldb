package goleveldb

import (
	"encoding/binary"
	"fmt"
)

type TableBuilder struct {
	options           *Options
	file              WritableFile
	status            error
	offset            uint64
	dataBlockBuilder  BlockBuilder
	indexBlockBuilder BlockBuilder
	pendingIndexEntry bool
	pendingHandle     BlockHandle
	lastKey           InternalKey
}

func NewTableBuilder(options *Options, file WritableFile) *TableBuilder {
	return &TableBuilder{
		options:           options,
		file:              file,
		offset:            0,
		pendingIndexEntry: false,
	}
}

// Add entry to data block
// If the the data block exceeds the threshold, flush and insert an index in the index block.
func (builder *TableBuilder) Add(key InternalKey, value []byte) {
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

// return code_len, internal_key, value
func DecodeEntryFrom(data []byte, offset uint32) (uint32, InternalKey, []byte) {
	// | internal_key_size(4B) | value_size(4B) | internal_key | value |
	internal_key_size := DecodeFixed32(data[offset:])
	value_size := DecodeFixed32(data[offset+4:])
	internal_key_offset := offset + 8
	value_offset := internal_key_offset + internal_key_size

	code_len := 8 + internal_key_size + value_size
	internal_key := InternalKey(data[internal_key_offset:value_offset])
	value := data[value_offset : value_offset+value_size]
	return code_len, internal_key, value
}

type BlockBuilder struct {
	buffer  []byte // Destination buffer
	counter uint32 // Number of entries in block
}

func (blockBuilder *BlockBuilder) Add(key InternalKey, value []byte) {
	// | internal_key_size(4 byte) | value_size(4 byte) | internal_key | value |
	p := make([]byte, 8)
	key_size := uint32(len(key))
	value_size := uint32(len(value))
	EncodeFixed32(p, key_size)
	EncodeFixed32(p[4:8], value_size)
	blockBuilder.buffer = append(blockBuilder.buffer, p...)
	blockBuilder.buffer = append(blockBuilder.buffer, []byte(key)...)
	blockBuilder.buffer = append(blockBuilder.buffer, value...)
	blockBuilder.counter++
}

func (blockBuilder *BlockBuilder) Finish() []byte {
	// EncodeTo(&blockBuilder.buffer, blockBuilder.counter)
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

type BlockHandle struct {
	Offset uint64
	Size   uint64
}

func (handle *BlockHandle) EncodeTo() []byte {
	p := make([]byte, 16)
	EncodeFixed64(p, handle.Offset)
	EncodeFixed64(p[8:], handle.Size)
	return p
}

type Footer struct {
	MetaIndexHandle  BlockHandle
	IndexBlockHandle BlockHandle
}

const (
	KEncodedLength int = 32
)

func (footer *Footer) EncodeTo() string {
	p := make([]byte, KEncodedLength)
	binary.LittleEndian.PutUint64(p[0:8], footer.MetaIndexHandle.Offset)
	binary.LittleEndian.PutUint64(p[8:16], footer.MetaIndexHandle.Size)
	binary.LittleEndian.PutUint64(p[16:24], footer.IndexBlockHandle.Offset)
	binary.LittleEndian.PutUint64(p[24:32], footer.IndexBlockHandle.Size)
	return string(p)
}

func (footer *Footer) DecodeFrom(data []byte) {
	// buf := bytes.NewBuffer(data)
	// binary.Read(buf, binary.LittleEndian, &footer.MetaIndexHandle.Offset)
	// binary.Read(buf, binary.LittleEndian, &footer.MetaIndexHandle.Size)
	// binary.Read(buf, binary.LittleEndian, &footer.IndexBlockHandle.Offset)
	// binary.Read(buf, binary.LittleEndian, &footer.IndexBlockHandle.Size)
	footer.MetaIndexHandle.Offset = binary.LittleEndian.Uint64(data[0:8])
	footer.MetaIndexHandle.Size = binary.LittleEndian.Uint64(data[8:16])
	footer.IndexBlockHandle.Offset = binary.LittleEndian.Uint64(data[16:24])
	footer.IndexBlockHandle.Size = binary.LittleEndian.Uint64(data[24:32])
}

type Block struct {
	Data []byte
	Size uint32
}

// Read block data from file
func NewBlock(file RandomAccessFile, blockHandle BlockHandle) (*Block, error) {
	var block Block
	var err error
	block.Size = uint32(blockHandle.Size)
	block.Data, err = file.Read(blockHandle.Offset, uint32(blockHandle.Size))
	if err != nil {
		return nil, err
	}
	return &block, err
}

func (block *Block) Get(offset uint32, key InternalKey) ([]byte, []byte, error) {
	cur_offset := offset
	for cur_offset < block.Size {
		n, cur_key, value := DecodeEntryFrom(block.Data, cur_offset)
		cmp := InternalKeyCompare(cur_key, key)
		if cmp >= 0 {
			return cur_key, value, nil
		}
		cur_offset += n
	}
	return nil, nil, fmt.Errorf("%s", "Not Found")
}

type BlockIterator struct {
	data             []byte
	current          uint32 // current_ is offset in data of current entry.
	key              InternalKey
	value            []byte
	current_code_len uint32 // current kv entry len
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
	iter.current_code_len, iter.key, iter.value = DecodeEntryFrom(iter.data, 0)
}

func (iter *BlockIterator) SeekToLast() {
	panic("BlockIterator.SeekToLast() Unimplement!")
}

func (iter *BlockIterator) Seek(target interface{}) {
	for iter.Valid() && InternalKeyCompare(target.(InternalKey), iter.key) < 0 {
		iter.Next()
	}
}

func (iter *BlockIterator) Next() {
	if !iter.Valid() {
		return
	}

	// next key-value entry offset
	iter.current += iter.current_code_len

	// decode from bytes
	iter.current_code_len, iter.key, iter.value = DecodeEntryFrom(iter.data, iter.current)
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

var _ Iterator = (*BlockIterator)(nil)

type SSTable struct {
	file       RandomAccessFile
	footer     Footer
	indexBlock *Block
	dataBlock  *Block
}

func OpenSSTable(file RandomAccessFile, size uint64) (*SSTable, error) {
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
func (table *SSTable) Get(key InternalKey) ([]byte, error) {
	// search in index block
	_, pos_bytes, err := table.indexBlock.Get(0, key)
	if err != nil {
		return nil, err
	}
	offset := DecodeFixed32(pos_bytes)

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
