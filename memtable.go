package goleveldb

type MemTable struct {
	table       *SkipList
	memoryUsage uint64
	logPath     string
}

func NewMemTable(logPath string) *MemTable {
	var memtable MemTable
	memtable.table = New()
	memtable.memoryUsage = 0
	memtable.logPath = logPath
	return &memtable
}

func (mem *MemTable) Add(seq SequenceNumber, valueType ValueType, key, value []byte) {
	// construct memtable key
	memkey := NewKVEntry(seq, valueType, key, value)
	// insert into memiplist
	mem.table.Insert([]byte(memkey))
	mem.memoryUsage += uint64(len(memkey))
}

func (mem *MemTable) Get(key LookupKey) ([]byte, bool) {
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

func (mem *MemTable) ApproximateMemoryUsage() uint64 {
	return mem.memoryUsage
}

func (mem *MemTable) Iterator() Iterator {
	return mem.table.NewIterator()
}

func (mem *MemTable) GetLogPath() string {
	return mem.logPath
}
