package goleveldb

import (
	"fmt"
	"testing"
)

type outputIterator struct {
	data [][]byte
	pos  int
}

func newOutputIterator(data [][]byte) *outputIterator {
	return &outputIterator{
		data: data,
	}
}

func (iter *outputIterator) Valid() bool {
	return iter.pos < len(iter.data)
}

func (iter *outputIterator) SeekToFirst() {
	iter.pos = 0
}

func (iter *outputIterator) Seek(target interface{}) {}

func (iter *outputIterator) Next() {
	if iter.Valid() {
		iter.pos++
	}
}

func (iter *outputIterator) Key() []byte {
	return iter.data[iter.pos]
}

func (iter *outputIterator) Value() []byte {
	return iter.data[iter.pos]
}

var _ Iterator = (*outputIterator)(nil)

func Test_level(t *testing.T) {
	data1 := [][]byte{}
	data2 := [][]byte{}
	data3 := [][]byte{}
	test_num := 10000
	for i := 0; i < test_num; i++ {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		if i < test_num/3 {
			data1 = append(data1, key)
		} else if i < 2*test_num/3 {
			data2 = append(data2, key)
		} else {
			data3 = append(data3, key)
		}
	}

	iter := newSortedLevelIterator([]Iterator{newOutputIterator(data1), newOutputIterator(data2), newOutputIterator(data3)})

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		k := iter.Key()
		if Compare(k, key) != 0 {
			t.Fatalf("level iterator failed! Expect %s, but get %s\n", key, k)
		}
		i++
	}
}

func Test_merge(t *testing.T) {
	data1 := [][]byte{}
	data2 := [][]byte{}
	data3 := [][]byte{}
	for i := 0; i < 10000; i++ {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		if i%3 == 0 {
			data1 = append(data1, key)
		} else if i%3 == 1 {
			data2 = append(data2, key)
		} else {
			data3 = append(data3, key)
		}
	}

	i1 := []Iterator{newOutputIterator(data1)}
	i2 := []Iterator{newOutputIterator(data2)}
	i3 := []Iterator{newOutputIterator(data3)}
	list := [][]Iterator{i1, i2, i3}

	mergeIter := newMergeIterator(list)

	i := 0
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		k := mergeIter.Key()
		if Compare(k, key) != 0 {
			t.Fatalf("merge failed! Expect %s, but get %s\n", key, k)
		}
		i++
	}
}

func Test_deduplication(t *testing.T) {
	data := [][]byte{}
	test_num := 10000
	for i := 0; i < test_num; i++ {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		data = append(data, key)
	}
	iter := newDeduplicationIterator(newOutputIterator(data))

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
		k := iter.Key()
		if Compare(k, key) != 0 {
			t.Fatalf("deduplication iterator failed! Expect %s, but get %s\n", key, k)
		}
		i++
	}
}
