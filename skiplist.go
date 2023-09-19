package goleveldb

import (
	"math/rand"
	"sync"
)

type Node struct {
	key  interface{}
	next []*Node
}

func newNode(key interface{}, height int) *Node {
	x := new(Node)
	x.key = key
	x.next = make([]*Node, height)

	return x
}
func (node *Node) getNext(level int) *Node {
	return node.next[level]
}

func (node *Node) setNext(level int, x *Node) {
	node.next[level] = x
}

const (
	kMaxHeight = 12
	kBranching = 4
)

type SkipList struct {
	maxHeight int
	head      *Node
	mu        sync.RWMutex
}

func New() *SkipList {
	var skiplist SkipList
	skiplist.head = newNode(nil, kMaxHeight)
	skiplist.maxHeight = 1
	return &skiplist
}

func (list *SkipList) Insert(key interface{}) {
	list.mu.Lock()
	defer list.mu.Unlock()

	_, prev := list.findGreaterOrEqual(key)
	height := list.randomHeight()
	if height > list.maxHeight {
		for i := list.maxHeight; i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}
	x := newNode(key, height)
	for i := 0; i < height; i++ {
		x.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, x)
	}
}

func (list *SkipList) Contains(key interface{}) bool {
	list.mu.RLock()
	defer list.mu.RUnlock()
	x, _ := list.findGreaterOrEqual(key)
	if x != nil && LookupKeyCompare(x.key.([]byte), key.([]byte)) == 0 {
		return true
	}
	return false
}

func (list *SkipList) NewIterator() *SkipListIterator {
	var it SkipListIterator
	it.list = list
	return &it
}

func (list *SkipList) randomHeight() int {
	height := 1
	for height < kMaxHeight && (rand.Intn(kBranching) == 0) {
		height++
	}
	return height
}

func (list *SkipList) findGreaterOrEqual(key interface{}) (*Node, [kMaxHeight]*Node) {
	var prev [kMaxHeight]*Node
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.getNext(level)
		if list.keyIsAfterNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func (list *SkipList) findLessThan(key interface{}) *Node {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.getNext(level)
		if next == nil || LookupKeyCompare(next.key.([]byte), key.([]byte)) >= 0 {

			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}
func (list *SkipList) findlast() *Node {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}

func (list *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (LookupKeyCompare(n.key.([]byte), key.([]byte)) < 0)
}

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
	return it.node.key.([]byte)
}

func (it *SkipListIterator) Value() []byte {
	return it.node.key.([]byte)
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *SkipListIterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.getNext(0)
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *SkipListIterator) Prev() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findLessThan(it.node.key)
	if it.node == it.list.head {
		it.node = nil
	}
}

// Advance to the first entry with a key >= target
func (it *SkipListIterator) Seek(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SkipListIterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.getNext(0)
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SkipListIterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findlast()
	if it.node == it.list.head {
		it.node = nil
	}
}

var _ Iterator = (*SkipListIterator)(nil)
