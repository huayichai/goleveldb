package skiplist

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func bytes_compare(lhs, rhs interface{}) int {
	return bytes.Compare(lhs.([]byte), rhs.([]byte))
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
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v.([]byte))
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
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v.([]byte))
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

func TestConcurrencyInsert(t *testing.T) {
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
			t.Fatalf("Get key %s failed! Expect %s, but %s\n", key, value, v.([]byte))
		}
	}
}
