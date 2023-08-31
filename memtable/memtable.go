package memtable

import (
	"github.com/huayichai/goleveldb/internal"
)

type MemTable interface {
	Add(seq internal.SequenceNumber, valueType internal.ValueType, key, value []byte)
	Get(key internal.LookupKey) ([]byte, bool)
	ApproximateMemoryUsage() uint64
	Iterator() internal.Iterator
}

func NewMemTable() MemTable {
	return NewSkipListTable()
}
