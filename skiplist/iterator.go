package skiplist

import "github.com/huayichai/goleveldb/internal"

type Iterator struct {
	list *SkipList
	node *Node
}

// Returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.node != nil
}

// Returns the key at the current position.
// REQUIRES: Valid()
func (it *Iterator) Key() []byte {
	code := it.node.key.([]byte)
	return internal.DecodeInternalKeyFrom(code)
}

func (it *Iterator) Value() []byte {
	code := it.node.key.([]byte)
	return internal.DecodeInternalValueFrom(code)
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.getNext(0)
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findLessThan(it.node.key)
	if it.node == it.list.head {
		it.node = nil
	}
}

// Advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.getNext(0)
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findlast()
	if it.node == it.list.head {
		it.node = nil
	}
}

var _ internal.Iterator = (*Iterator)(nil)
