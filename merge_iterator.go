package goleveldb

type mergeIterator struct {
	list    []*sstableIterator
	current *sstableIterator
}

func newMergeIterator(list []*sstableIterator) *mergeIterator {
	var iter mergeIterator
	iter.list = list
	return &iter
}

func (iter *mergeIterator) valid() bool {
	return iter.current != nil && iter.current.Valid()
}

func (iter *mergeIterator) internalKey() InternalKey {
	return iter.current.Key()
}

func (iter *mergeIterator) value() []byte {
	return iter.current.Value()
}

func (iter *mergeIterator) next() {
	if iter.current != nil {
		iter.current.Next()
	}
	iter.findSmallest()
}

func (iter *mergeIterator) seekToFirst() {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].SeekToFirst()
	}
	iter.findSmallest()
}

func (iter *mergeIterator) findSmallest() {
	var smallest *sstableIterator = nil
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
