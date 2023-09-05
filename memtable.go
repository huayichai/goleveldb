package goleveldb

type memTable struct {
	table       *SkipList
	memoryUsage uint64
	logPath     string
}

func newMemTable(logPath string) *memTable {
	var memtable memTable
	memtable.table = New()
	memtable.memoryUsage = 0
	memtable.logPath = logPath
	return &memtable
}

func (mem *memTable) add(seq SequenceNumber, valueType ValueType, key, value []byte) {
	// construct memtable key
	memkey := NewKVEntry(seq, valueType, key, value)
	// insert into memiplist
	mem.table.Insert([]byte(memkey))
	mem.memoryUsage += uint64(len(memkey))
}

func (mem *memTable) get(key LookupKey) ([]byte, bool) {
	iter := mem.table.NewIterator()
	iter.Seek([]byte(key))
	if iter.Valid() {
		memkey := KVEntry(iter.Key())
		internal_key := memkey.ExtractInternalKey()
		if UserKeyCompare(internal_key.ExtractUserKey(), key.ExtractUserKey()) == 0 {
			// deleted
			if internal_key.ExtractValueType() == KTypeDeletion {
				return nil, false
			}
			// extract value
			return memkey.ExtractValue(), true
		}
	}
	return nil, false
}

func (mem *memTable) approximateMemoryUsage() uint64 {
	return mem.memoryUsage
}

func (mem *memTable) iterator() Iterator {
	return mem.table.NewIterator()
}

func (mem *memTable) getLogPath() string {
	return mem.logPath
}
