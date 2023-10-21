package goleveldb

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
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

func TestConcurrentInsertThroughput(t *testing.T) {
	test_num := 100000
	thread_num := 10

	// generate test data
	key_arrays := make([]int, 0)
	for i := 0; i < test_num; i++ {
		key_arrays = append(key_arrays, i)
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(key_arrays), func(i, j int) {
		key_arrays[i], key_arrays[j] = key_arrays[j], key_arrays[i]
	})

	list := newSkipList()
	var wg sync.WaitGroup
	insert_func := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			key := NewInternalKey([]byte(fmt.Sprintf("%06dtest", i)), SequenceNumber(i), KTypeValue)
			value := []byte(fmt.Sprintf("value%06d", key_arrays[i]))
			list.Insert(key, value)
		}
	}

	batch_num := test_num / thread_num
	startTime := time.Now()
	for i := 0; i < thread_num; i++ {
		wg.Add(1)
		start := batch_num * i
		end := batch_num * (i + 1)
		if end > test_num {
			end = test_num
		}
		go insert_func(start, end)
	}
	wg.Wait()
	insertTime := time.Since(startTime) / time.Millisecond                      // ms
	insertThroughput := int64(float64(test_num) / float64(insertTime) * 1000.0) // QPS
	fmt.Printf("SkipList %d thread insert test.", thread_num)
	fmt.Printf("Insert entrys num: %d, throughput: %d\n", test_num, insertThroughput)

	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		iter := list.NewIterator()
		iter.Seek(key)

		if !iter.Valid() || InternalKeyCompare(iter.Key(), value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, iter.Key())
		}
	}
}
