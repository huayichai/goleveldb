package goleveldb

type MemTable interface {
	Add(seq SequenceNumber, valueType ValueType, key, value []byte)
	Get(key LookupKey) ([]byte, bool)
	ApproximateMemoryUsage() uint64
	Iterator() Iterator
	GetLogPath() string
}

func NewMemTable(logPath string) MemTable {
	return NewSkipListTable(logPath)
}
