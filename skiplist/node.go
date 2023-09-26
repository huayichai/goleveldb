package skiplist

type Node struct {
	Key    interface{}
	Value  interface{}
	levels []*Node
}

func newNode(level int, key, value interface{}) *Node {
	var node Node
	node.levels = make([]*Node, level)
	node.Key = key
	node.Value = value
	return &node
}

func (node *Node) Next() *Node {
	return node.GetLevel(0)
}

func (node *Node) GetLevel(level int) *Node {
	if level < 0 || len(node.levels) <= level {
		return nil
	}
	return node.levels[level]
}

func (node *Node) setNext(level int, next *Node) {
	node.levels[level] = next
}
