package wal

type RecordType uint8

const (
	// Zero is reserved for preallocated files
	kZeroType RecordType = 0

	kFullType = 1

	// For fragments
	kFirstType  = 2
	kMiddleType = 3
	kLastType   = 4
)

const kBlockSize uint32 = 32 * 1024

// const kBlockSize uint32 = 32

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const kHeaderSize uint32 = 4 + 2 + 1
