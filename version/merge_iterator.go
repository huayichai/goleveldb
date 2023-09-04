package version

import (
	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/sstable"
)

type MergeIterator struct {
	list    []*sstable.SSTableIterator
	current *sstable.SSTableIterator
}

func NewMergeIterator(list []*sstable.SSTableIterator) *MergeIterator {
	var iter MergeIterator
	iter.list = list
	return &iter
}

func (iter *MergeIterator) Valid() bool {
	return iter.current != nil && iter.current.Valid()
}

func (iter *MergeIterator) InternalKey() internal.InternalKey {
	return iter.current.Key()
}

func (iter *MergeIterator) Value() []byte {
	return iter.current.Value()
}

func (iter *MergeIterator) Next() {
	if iter.current != nil {
		iter.current.Next()
	}
	iter.findSmallest()
}

func (iter *MergeIterator) SeekToFirst() {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].SeekToFirst()
	}
	iter.findSmallest()
}

func (iter *MergeIterator) findSmallest() {
	var smallest *sstable.SSTableIterator = nil
	for i := 0; i < len(iter.list); i++ {
		if iter.list[i].Valid() {
			if smallest == nil {
				smallest = iter.list[i]
			} else if internal.InternalKeyCompare(smallest.Key(), iter.list[i].Key()) > 0 {
				smallest = iter.list[i]
			}
		}
	}
	iter.current = smallest
}
