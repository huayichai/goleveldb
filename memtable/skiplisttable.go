package memtable

import (
	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/skiplist"
)

type SkipListTable struct {
	table       *skiplist.SkipList
	memoryUsage uint64
}

func NewSkipListTable() *SkipListTable {
	var memtable SkipListTable
	memtable.table = skiplist.New()
	memtable.memoryUsage = 0
	return &memtable
}

func (sk *SkipListTable) Add(valueType internal.ValueType, key, value string) {
	internalKey := internal.EncodeInternalKVEntry([]byte(key), []byte(value))
	sk.table.Insert(internalKey)
	sk.memoryUsage += uint64(len(internalKey))
}

func (sk *SkipListTable) Get(key string) (string, bool) {
	lookup_key := []byte(key)
	iter := sk.table.NewIterator()
	iter.Seek(lookup_key)
	if iter.Valid() {
		if internal.Compare(iter.Key(), lookup_key) == 0 {
			return string(iter.Value()), true
		}
	}
	return "", false
}

func (sk *SkipListTable) ApproximateMemoryUsage() uint64 {
	return sk.memoryUsage
}

func (sk *SkipListTable) Iterator() internal.Iterator {
	return sk.table.NewIterator()
}

var _ MemTable = (*SkipListTable)(nil)
