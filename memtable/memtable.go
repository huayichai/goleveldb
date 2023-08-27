package memtable

import (
	"github.com/huayichai/goleveldb/internal"
)

type MemTable interface {
	Add(valueType internal.ValueType, key, value string)
	Get(key string) (string, bool)
	ApproximateMemoryUsage() uint64
}

func NewMemTable() MemTable {
	return NewMapTable()
}
