package skiplist

type SkipListIterator struct {
	list *SkipList
	node *Node
}

// Returns true iff the iterator is positioned at a valid node.
func (it *SkipListIterator) Valid() bool {
	return it.node != nil
}

// Returns the key at the current position.
// REQUIRES: Valid()
func (it *SkipListIterator) Key() []byte {
	return it.node.Key
}

func (it *SkipListIterator) Value() []byte {
	return it.node.Value
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *SkipListIterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.Next()
}

// Advance to the first entry with a key >= target
func (it *SkipListIterator) Seek(target interface{}) {
	it.node = it.list.Find(target.([]byte))
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SkipListIterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.Next()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SkipListIterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.tail
}
