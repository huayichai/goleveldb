package goleveldb

type SkipListTable struct {
	table       *SkipList
	memoryUsage uint64
	logPath     string
}

func NewSkipListTable(logPath string) *SkipListTable {
	var memtable SkipListTable
	memtable.table = New()
	memtable.memoryUsage = 0
	memtable.logPath = logPath
	return &memtable
}

func (sk *SkipListTable) Add(seq SequenceNumber, valueType ValueType, key, value []byte) {
	// construct memtable key
	memkey := NewKVEntry(seq, valueType, key, value)
	// insert into skiplist
	sk.table.Insert([]byte(memkey))
	sk.memoryUsage += uint64(len(memkey))
}

func (sk *SkipListTable) Get(key LookupKey) ([]byte, bool) {
	iter := sk.table.NewIterator()
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

func (sk *SkipListTable) ApproximateMemoryUsage() uint64 {
	return sk.memoryUsage
}

func (sk *SkipListTable) Iterator() Iterator {
	return sk.table.NewIterator()
}

func (sk *SkipListTable) GetLogPath() string {
	return sk.logPath
}

var _ MemTable = (*SkipListTable)(nil)
