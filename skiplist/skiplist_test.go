package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func bytes_compare(lhs, rhs []byte) int {
	return bytes.Compare(lhs, rhs)
}

func TestBasic(t *testing.T) {
	list := New(bytes_compare)

	list.Delete([]byte("000"))

	test_num := 100
	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		list.Insert(key, value)
	}
	for i := 2*test_num - 1; i >= test_num; i-- {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		list.Insert(key, value)
	}

	for i := 0; i < 2*test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		v := list.Get(key).Value
		if v == nil || bytes_compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v)
		}
	}

	for i := 0; i < 2*test_num; i += 2 {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i+1))
		list.Delete(key)
		node := list.Get(key)
		if node != nil {
			t.Fatalf("Delete key %s failed!\n", key)
		}
		v := list.Find(key).Value
		if v == nil || bytes_compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v)
		}
	}
}

func TestFrontBack(t *testing.T) {
	list := New(bytes_compare)

	f1, b1 := list.Front(), list.Back()
	if f1 != nil || b1 != nil {
		t.Fatal("Front() or Back() failed!")
	}

	test_num := 100
	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		list.Insert(key, value)
	}
	f2, b2 := list.Front().Key, list.Back().Key
	if bytes_compare(f2, []byte(fmt.Sprintf("key%06d", 0))) != 0 || bytes_compare(b2, []byte(fmt.Sprintf("key%06d", 99))) != 0 {
		t.Fatal("Front() or Back() failed!")
	}

	list.Delete([]byte(fmt.Sprintf("key%06d", 0)))
	list.Delete([]byte(fmt.Sprintf("key%06d", 99)))
	f3, b3 := list.Front().Key, list.Back().Key
	if bytes_compare(f3, []byte(fmt.Sprintf("key%06d", 1))) != 0 || bytes_compare(b3, []byte(fmt.Sprintf("key%06d", 98))) != 0 {
		t.Fatal("Front() or Back() failed!")
	}
}

func TestConcurrentInsert(t *testing.T) {
	list := New(bytes_compare)

	thread_num := 10
	test_num := 20000

	var wg sync.WaitGroup
	insert_func := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			key := []byte(fmt.Sprintf("key%06d", i))
			value := []byte(fmt.Sprintf("value%06d", i))
			list.Insert(key, value)
		}
	}

	batch_num := test_num / thread_num
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

	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		v := list.Get(key).Value
		if v == nil || bytes_compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v)
		}
	}
}

func TestInsertThroughput(t *testing.T) {
	test_num := 100000

	// generate test data
	key_arrays := make([]int, 0)
	for i := 0; i < test_num; i++ {
		key_arrays = append(key_arrays, i)
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(key_arrays), func(i, j int) {
		key_arrays[i], key_arrays[j] = key_arrays[j], key_arrays[i]
	})

	// new skiplist instance
	list := New(bytes_compare)

	// insert
	startTime := time.Now()
	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", key_arrays[i]))
		value := []byte(fmt.Sprintf("value%06d", key_arrays[i]))
		list.Insert(key, value)
	}
	insertTime := time.Since(startTime) / time.Millisecond                      // ms
	insertThroughput := int64(float64(test_num) / float64(insertTime) * 1000.0) // QPS
	fmt.Println("SkipList single thread insert test.")
	fmt.Printf("Insert entrys num: %d, throughput: %d\n", test_num, insertThroughput)

	// check
	for i := 0; i < test_num; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		v := list.Get(key).Value
		if v == nil || bytes_compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v)
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

	list := New(bytes_compare)
	var wg sync.WaitGroup
	insert_func := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			key := []byte(fmt.Sprintf("key%06d", key_arrays[i]))
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
		v := list.Get(key).Value
		if v == nil || bytes_compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v)
		}
	}
}
