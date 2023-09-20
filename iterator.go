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
	list                []Iterator // sorted iterator
	pos                 uint32     // current iterator position
	cur_userkey         UserKey    // current user key
	cur_userkey_deleted bool       // whether cur key is deleted
}

func newSortedLevelIterator(list []Iterator) *sortedLevelIterator {
	var iter sortedLevelIterator
	iter.list = list
	iter.pos = 0
	iter.cur_userkey = []byte("")
	iter.cur_userkey_deleted = false
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

func (iter *sortedLevelIterator) Seek(target interface{}) {}

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
	list                 []Iterator
	current              Iterator
	prev_userkey         UserKey // prev user key
	prev_userkey_deleted bool    // whether prev key is deleted
}

func newMergeIterator(list [][]Iterator) *mergeIterator {
	var iter mergeIterator
	for i := 0; i < len(list); i++ {
		iter.list = append(iter.list, newSortedLevelIterator(list[i]))
	}
	iter.prev_userkey = []byte("")
	iter.prev_userkey_deleted = false
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

func (iter *mergeIterator) Seek(target interface{}) {}

func (iter *mergeIterator) findSmallest() {
	var smallest Iterator = nil
	for i := 0; i < len(iter.list); i++ {
		if iter.list[i].Valid() {
			if smallest == nil {
				smallest = iter.list[i]
			} else if InternalKeyCompare(smallest.Key(), iter.list[i].Key()) > 0 {
				smallest = iter.list[i]
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
	iter.SeekToFirst()
	iter.nextExist()
}

func (iter *deduplicationIterator) Next() {
	if iter.Valid() {
		iter.nextExist()
	}
}

func (iter *deduplicationIterator) nextExist() {
	var key InternalKey
	for {
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

func (iter *deduplicationIterator) Seek(target interface{}) {}

func (iter *deduplicationIterator) Key() []byte {
	return iter.input.Key()
}

func (iter *deduplicationIterator) Value() []byte {
	return iter.input.Value()
}

var _ Iterator = (*deduplicationIterator)(nil)
