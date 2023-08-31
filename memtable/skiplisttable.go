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

func (sk *SkipListTable) Add(seq internal.SequenceNumber, valueType internal.ValueType, key, value []byte) {
	// construct memtable key
	memkey := internal.NewMemTableKey(seq, valueType, key, value)
	// insert into skiplist
	sk.table.Insert([]byte(memkey))
	sk.memoryUsage += uint64(len(memkey))
}

func (sk *SkipListTable) Get(key internal.LookupKey) ([]byte, bool) {
	iter := sk.table.NewIterator()
	iter.Seek([]byte(key))
	if iter.Valid() {
		memkey := internal.MemTableKey(iter.Key())
		internal_key := memkey.ExtractInternalKey()
		if internal.UserKeyCompare(internal_key.ExtractUserKey(), key.ExtractUserKey()) == 0 {
			// deleted
			if internal_key.ExtractValueType() == internal.KTypeDeletion {
				return nil, false
			}
			// extract value
			return memkey.ExtractValue(), true
		}
	}
	return nil, false
}

func (sk *SkipListTable) ApproximateMemoryUsage() uint64 {
	return sk.memoryUsage
}

func (sk *SkipListTable) Iterator() internal.Iterator {
	return sk.table.NewIterator()
}

var _ MemTable = (*SkipListTable)(nil)
