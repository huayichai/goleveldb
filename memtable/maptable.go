package memtable

import (
	"github.com/huayichai/goleveldb/internal"
)

type MapTable struct {
	m           map[string]string
	memoryUsage uint64
}

func NewMapTable() *MapTable {
	return &MapTable{
		m:           make(map[string]string),
		memoryUsage: 0,
	}
}

func (mt *MapTable) Add(valueType internal.ValueType, key, value string) {
	mt.m[key] = value
	mt.memoryUsage += uint64(len(key) + len(value))
}

func (mt *MapTable) Get(key string) (string, bool) {
	v, ok := mt.m[key]
	return v, ok
}

func (mt *MapTable) ApproximateMemoryUsage() uint64 {
	return mt.memoryUsage
}

func (mt *MapTable) GetMap() map[string]string {
	return mt.m
}

// var _ MemTable = (*MapTable)(nil)
