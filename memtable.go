package goleveldb

type memTable struct {
	table       *SkipList
	memoryUsage uint64
	logPath     string
}

func newMemTable(logPath string) *memTable {
	var memtable memTable
	memtable.table = newSkipList()
	memtable.memoryUsage = 0
	memtable.logPath = logPath
	return &memtable
}

func (mem *memTable) add(seq SequenceNumber, valueType ValueType, key, value []byte) {
	// construct internal key
	internal_key := NewInternalKey(key, seq, valueType)
	// insert into memiplist
	mem.table.Insert(internal_key, value)
	mem.memoryUsage += uint64(len(internal_key) + len(value))
}

// Return value, status
// status = ErrKeyNotFound means key not in memtable
// status = errKeyDeleted  means key was been deleted
// status = nil            menas find key and return value
func (mem *memTable) get(key InternalKey) ([]byte, error) {
	iter := mem.table.NewIterator()
	iter.Seek(key)
	if iter.Valid() {
		lookuped_key := InternalKey(iter.Key())
		if UserKeyCompare(lookuped_key.ExtractUserKey(), key.ExtractUserKey()) == 0 {
			if lookuped_key.ExtractValueType() == KTypeDeletion {
				return nil, errKeyDeleted
			} else {
				return iter.Value(), nil
			}
		}
	}
	return nil, ErrKeyNotFound
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
