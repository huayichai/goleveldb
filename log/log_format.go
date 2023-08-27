package log

type RecordType uint8

const (
	// Zero is reserved for preallocated files
	KZeroType RecordType = 0

	KFullType = 1

	// For fragments
	KFirstType  = 2
	KMiddleType = 3
	KLastType   = 4
)

const KBlockSize int = 32 * 1024

const KHeaderSize int = 4 + 2 + 1
