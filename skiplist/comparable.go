package skiplist

type Comparable func(lhs, rhs interface{}) int

type GreaterThanFunc Comparable
type LessThanFunc Comparable
