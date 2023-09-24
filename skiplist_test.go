package goleveldb

import (
	"fmt"
	"testing"
)

func Test_Basic(t *testing.T) {
	test_num := 100
	index := newSkipList()
	for i := 0; i < test_num; i++ {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		index.Insert(key, key)
	}

	for i := 0; i < test_num; i++ {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		iter := index.NewIterator()
		iter.SeekToFirst()
		iter.Seek(key)
		if !iter.Valid() {
			t.Fatalf("SkipList failed! Expect %s, but get nil\n", key)
		}
		k := iter.Key()
		if InternalKeyCompare(key, k) != 0 {
			t.Fatalf("SkipList failed! Expect %s, but get %s\n", key, k)
		}
	}
}
