package goleveldb

// tableBuilder build the sstable
type tableBuilder struct {
	options           *Options
	file              WritableFile
	status            error
	offset            uint64
	dataBlockBuilder  *blockBuilder
	indexBlockBuilder *blockBuilder
	pendingIndexEntry bool
	pendingHandle     blockHandle
	lastKey           InternalKey
}

func newTableBuilder(options *Options, file WritableFile) *tableBuilder {
	return &tableBuilder{
		options:           options,
		file:              file,
		offset:            0,
		dataBlockBuilder:  newBlockBuilder(options.BlockRestartInterval),
		indexBlockBuilder: newBlockBuilder(1),
		pendingIndexEntry: false,
	}
}

// Add entry to data block
// If the the data block exceeds the threshold, flush and insert an index in the index block.
func (builder *tableBuilder) add(key InternalKey, value []byte) {
	if builder.pendingIndexEntry {
		sep := FindShortestSeparator(builder.lastKey, key) // all keys before key is less than sep
		handle := builder.pendingHandle.encodeTo()
		builder.indexBlockBuilder.add(sep, handle)
		builder.pendingIndexEntry = false
	}

	builder.lastKey = key
	builder.dataBlockBuilder.add(key, value)

	if builder.dataBlockBuilder.currentSizeEstimate() >= builder.options.BlockSize {
		builder.flush()
	}
}

func (builder *tableBuilder) flush() {
	if builder.dataBlockBuilder.empty() {
		return
	}
	builder.pendingHandle = builder.writeblock(builder.dataBlockBuilder)
	if builder.status == nil {
		builder.pendingIndexEntry = true
		if builder.options.Sync {
			builder.file.Sync()
		}
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
		handle := builder.pendingHandle.encodeTo()
		builder.indexBlockBuilder.add(builder.lastKey, handle)
		builder.pendingIndexEntry = false
	}
	indexblockHandle := builder.writeblock(builder.indexBlockBuilder)

	// write footer block
	footer := footer{indexblockHandle: indexblockHandle}
	builder.status = builder.file.Append(string(footer.encodeTo()))

	// flush disk
	builder.file.Sync()

	// close sstable
	builder.file.Close()
}

func (builder *tableBuilder) fileSize() uint64 {
	return builder.offset
}

// blockBuilder builds the block in sstable
// block contains datablock and indexblock
//
// An entry for a particular key-value pair has the form:
//
//	shared_bytes: varint32
//	unshared_bytes: varint32
//	value_length: varint32
//	key_delta: char[unshared_bytes]
//	value: char[value_length]
//
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//
//	restarts: uint32[num_restarts]
//	num_restarts: uint32
//
// restarts[i] contains the offset within the block of the ith restart point.
type blockBuilder struct {
	blockRestartInterval uint32
	buffer               []byte   // Destination buffer
	restarts             []uint32 // Restart points
	counter              uint32   // Number of entries in block
	lastInternalKey      InternalKey
}

func newBlockBuilder(interval uint32) *blockBuilder {
	var builder blockBuilder
	builder.blockRestartInterval = interval
	builder.restarts = append(builder.restarts, 0)
	builder.counter = 0
	return &builder
}

func (builder *blockBuilder) add(key InternalKey, value []byte) {
	shared := 0
	if builder.counter < builder.blockRestartInterval {
		// See how much sharing to do with previous string
		min_ken := min(len(key), len(builder.lastInternalKey))
		for (shared < min_ken) && (builder.lastInternalKey[shared] == key[shared]) {
			shared++
		}
	} else {
		// Restart compression
		builder.restarts = append(builder.restarts, uint32(len(builder.buffer)))
		builder.counter = 0
	}
	non_shared := len(key) - shared

	// Add "<shared><non_shared><value_size>" to buffer
	p := make([]byte, 15)
	var offset uint32 = 0
	offset += EncodeUVarint32(p[offset:], uint32(shared))
	offset += EncodeUVarint32(p[offset:], uint32(non_shared))
	offset += EncodeUVarint32(p[offset:], uint32(len(value)))
	builder.buffer = append(builder.buffer, p[0:offset]...)

	// Add key's delta to buffer followed by value
	builder.buffer = append(builder.buffer, []byte(key)[shared:]...)
	builder.buffer = append(builder.buffer, value...)

	// Update state
	builder.lastInternalKey = key
	builder.counter++
}

func (builder *blockBuilder) finish() []byte {
	n_restars := len(builder.restarts)
	p := make([]byte, n_restars*4+4)
	offset := 0
	for i := 0; i < n_restars; i++ {
		EncodeFixed32(p[offset:], builder.restarts[i])
		offset += 4
	}
	EncodeFixed32(p[offset:], uint32(n_restars))
	builder.buffer = append(builder.buffer, p...)
	return builder.buffer
}

func (builder *blockBuilder) currentSizeEstimate() uint32 {
	return uint32(len(builder.buffer) + // Raw data buffer
		len(builder.restarts)*4 + // Restart array
		4) // Restart array length
}

func (builder *blockBuilder) reset() {
	builder.buffer = builder.buffer[0:0]
	builder.restarts = builder.restarts[0:0]
	builder.restarts = append(builder.restarts, 0)
	builder.counter = 0
	builder.lastInternalKey = []byte{}
}

func (builder *blockBuilder) empty() bool {
	return builder.currentSizeEstimate() == 0
}
