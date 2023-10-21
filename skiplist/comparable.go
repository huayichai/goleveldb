package skiplist

type Comparable func(lhs, rhs []byte) int

type GreaterThanFunc Comparable
type LessThanFunc Comparable
