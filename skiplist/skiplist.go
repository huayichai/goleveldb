package skiplist

import (
	"math/rand"
	"sync"

	"github.com/huayichai/goleveldb/internal"
)

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
	if x != nil && internal.LookupKeyCompare(x.key.([]byte), key.([]byte)) == 0 {
		return true
	}
	return false
}

func (list *SkipList) NewIterator() *Iterator {
	var it Iterator
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
	return nil, prev
}

func (list *SkipList) findLessThan(key interface{}) *Node {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.getNext(level)
		if next == nil || internal.LookupKeyCompare(next.key.([]byte), key.([]byte)) >= 0 {

			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
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
	return nil
}

func (list *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (internal.LookupKeyCompare(n.key.([]byte), key.([]byte)) < 0)
}
