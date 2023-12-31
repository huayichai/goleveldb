package goleveldb

type Iterator interface {
	Valid() bool
	SeekToFirst()
	Seek(target interface{})
	Next()
	Key() []byte
	Value() []byte
}

type sortedLevelIterator struct {
	list []Iterator // sorted iterator
	pos  uint32     // current iterator position
}

func newSortedLevelIterator(list []Iterator) *sortedLevelIterator {
	var iter sortedLevelIterator
	iter.list = list
	iter.pos = 0
	return &iter
}

func (iter *sortedLevelIterator) Valid() bool {
	if iter.pos >= uint32(len(iter.list)) {
		return false
	}
	return iter.list[iter.pos].Valid()
}

func (iter *sortedLevelIterator) SeekToFirst() {
	if iter.pos >= uint32(len(iter.list)) {
		return
	}
	iter.list[0].SeekToFirst()
}

func (iter *sortedLevelIterator) Seek(target interface{}) {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].Seek(target)
		if InternalKeyCompare(iter.list[i].Key(), target.(InternalKey)) >= 0 {
			iter.pos = uint32(i)
			break
		}
	}
}

func (iter *sortedLevelIterator) Next() {
	level_num := uint32(len(iter.list))
	if iter.pos >= level_num {
		return
	} else if iter.list[iter.pos].Valid() {
		iter.list[iter.pos].Next()
	} else {
		iter.pos++
		if iter.pos < level_num {
			iter.list[iter.pos].SeekToFirst()
		}
	}
}

func (iter *sortedLevelIterator) Key() []byte {
	return iter.list[iter.pos].Key()
}

func (iter *sortedLevelIterator) Value() []byte {
	return iter.list[iter.pos].Value()
}

var _ Iterator = (*sortedLevelIterator)(nil)

type mergeIterator struct {
	list    []Iterator
	current Iterator
}

func newMergeIterator(list [][]Iterator) *mergeIterator {
	var iter mergeIterator
	for i := 0; i < len(list); i++ {
		iter.list = append(iter.list, newSortedLevelIterator(list[i]))
	}
	return &iter
}

func (iter *mergeIterator) Valid() bool {
	return iter.current != nil && iter.current.Valid()
}

func (iter *mergeIterator) Key() []byte {
	return iter.current.Key()
}

func (iter *mergeIterator) Value() []byte {
	return iter.current.Value()
}

func (iter *mergeIterator) Next() {
	if iter.current != nil {
		iter.current.Next()
	}
	iter.findSmallest()
}

func (iter *mergeIterator) SeekToFirst() {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].SeekToFirst()
	}
	iter.findSmallest()
}

func (iter *mergeIterator) Seek(target interface{}) {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].Seek(target)
	}
	iter.findSmallest()
}

func (iter *mergeIterator) findSmallest() {
	var smallest Iterator = nil
	var smallest_key []byte
	for i := 0; i < len(iter.list); i++ {
		if iter.list[i].Valid() {
			if smallest == nil {
				smallest = iter.list[i]
				smallest_key = smallest.Key()
				continue
			}
			i_key := iter.list[i].Key()
			if InternalKeyCompare(smallest_key, i_key) > 0 {
				smallest = iter.list[i]
				smallest_key = i_key
			}
		}
	}
	iter.current = smallest
}

var _ Iterator = (*mergeIterator)(nil)

// Responsible for remove the deleted or duplicated item in iterator
type deduplicationIterator struct {
	input                Iterator
	prev_userkey         UserKey
	prev_userkey_deleted bool
}

func newDeduplicationIterator(input Iterator) *deduplicationIterator {
	var iter deduplicationIterator
	iter.input = input
	iter.prev_userkey = []byte("")
	iter.prev_userkey_deleted = false
	return &iter
}

func (iter *deduplicationIterator) Valid() bool {
	return iter.input.Valid()
}

func (iter *deduplicationIterator) SeekToFirst() {
	iter.input.SeekToFirst()
	key := InternalKey(iter.Key())
	iter.prev_userkey = key.ExtractUserKey()
	iter.prev_userkey_deleted = key.ExtractValueType() == KTypeDeletion
	if iter.prev_userkey_deleted {
		iter.nextExist()
	}
}

func (iter *deduplicationIterator) Next() {
	if iter.Valid() {
		iter.nextExist()
	}
}

// find next exist entry
func (iter *deduplicationIterator) nextExist() {
	var key InternalKey
	iter.input.Next()
	for iter.Valid() {
		key = iter.input.Key()
		if UserKeyCompare(key.ExtractUserKey(), iter.prev_userkey) == 0 {
			iter.input.Next()
		} else {
			iter.prev_userkey = key.ExtractUserKey()
			iter.prev_userkey_deleted = key.ExtractValueType() == KTypeDeletion
			if !iter.prev_userkey_deleted {
				break
			}
			iter.input.Next()
		}
	}
}

func (iter *deduplicationIterator) Seek(target interface{}) {
	iter.input.Seek(target)
}

func (iter *deduplicationIterator) Key() []byte {
	return iter.input.Key()
}

func (iter *deduplicationIterator) Value() []byte {
	return iter.input.Value()
}

var _ Iterator = (*deduplicationIterator)(nil)
