package skiplist

import (
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultMaxLevel = 48
)

type SkipList struct {
	head      *Node // not store any data
	tail      *Node
	maxHeight int
	nums      int64

	compare Comparable
	rand    *rand.Rand
	mu      sync.RWMutex
}

func New(compare Comparable) *SkipList {
	var list SkipList
	list.head = newNode(DefaultMaxLevel, nil, nil)
	list.tail = nil
	list.maxHeight = 1
	list.nums = 0

	list.compare = compare
	list.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	return &list
}

func (list *SkipList) Insert(key, value interface{}) *Node {
	list.mu.Lock()
	defer list.mu.Unlock()
	isEqual, cur, prev := list.findGreaterOrEqual(key)

	if isEqual {
		cur.Value = value
		return cur
	}

	height := list.randHeight()
	if list.maxHeight < height {
		for i := list.maxHeight; i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}
	node := newNode(height, key, value)
	for i := 0; i < height; i++ {
		node.setNext(i, prev[i].GetLevel(i))
		prev[i].setNext(i, node)
	}

	if node.Next() == nil {
		list.tail = node
	}

	list.nums++
	return node
}

func (list *SkipList) Delete(key interface{}) {
	list.mu.Lock()
	defer list.mu.Unlock()
	node, prev := list.findFirstLessThan(key)
	del_node := node.Next()
	if del_node == nil || list.compare(del_node.Key, key) != 0 {
		return
	}
	h := min(list.maxHeight, len(del_node.levels))
	for i := 0; i < h; i++ {
		prev[i].setNext(i, del_node.GetLevel(i))
	}

	if del_node.Next() == nil {
		list.tail = node
	}
	list.nums--
}

// Get returns an node with the key.
// If the key is not found, returns nil.
func (list *SkipList) Get(key interface{}) *Node {
	list.mu.RLock()
	defer list.mu.RUnlock()
	isEqual, cur, _ := list.findGreaterOrEqual(key)
	if isEqual {
		return cur
	} else {
		return nil
	}
}

// Find returns the first node that is greater or equal to key.
func (list *SkipList) Find(key interface{}) *Node {
	list.mu.RLock()
	defer list.mu.RUnlock()
	_, cur, _ := list.findGreaterOrEqual(key)
	return cur
}

func (list *SkipList) Front() *Node {
	list.mu.RLock()
	defer list.mu.RUnlock()
	return list.head.Next()
}

func (list *SkipList) Back() *Node {
	list.mu.RLock()
	defer list.mu.RUnlock()
	return list.tail
}

func (list *SkipList) DataNum() int64 {
	return list.nums
}

// findGreaterOrEqual returns the first node that greater or equal to key
// @return isEqual, node, prev_node_list
func (list *SkipList) findGreaterOrEqual(key interface{}) (bool, *Node, []*Node) {
	prev := make([]*Node, DefaultMaxLevel)
	cur := list.head
	level := list.maxHeight - 1
	for {
		next := cur.GetLevel(level)
		if next != nil && list.compare(key, next.Key) >= 0 {
			cur = next
		} else {
			prev[level] = cur
			if level == 0 {
				if cur.Key == nil || list.compare(key, cur.Key) > 0 {
					return false, next, prev
				} else {
					return true, cur, prev
				}
			} else {
				level--
			}
		}
	}
}

func (list *SkipList) findFirstLessThan(key interface{}) (*Node, []*Node) {
	prev := make([]*Node, DefaultMaxLevel)
	cur := list.head
	level := list.maxHeight - 1
	for {
		next := cur.GetLevel(level)
		if next != nil && list.compare(key, next.Key) > 0 {
			cur = next
		} else {
			prev[level] = cur
			if level == 0 {
				return cur, prev
			} else {
				level--
			}
		}
	}
}

func (list *SkipList) randHeight() int {
	estimated := DefaultMaxLevel
	const prob = 1 << 30 // Half of 2^31.
	rand := list.rand
	i := 1
	for ; i < estimated; i++ {
		if rand.Int31() < prob {
			break
		}
	}
	return i
}
