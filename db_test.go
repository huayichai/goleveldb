package goleveldb

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func openDB() (*DB, func()) {
	path := "/tmp/goleveldb-mydb"
	os.RemoveAll(path)
	options := DefaultOptions()
	options.DirPath = path
	options.BlockSize = 1024
	options.MemTableSize = 1024 * 64

	var err error
	db, err := Open(*options)
	if err != nil {
		panic(err)
	}
	return db, func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func TestDB_Basic(t *testing.T) {
	db, destroy := openDB()
	defer destroy()

	test_num := 10000

	for i := 0; i < test_num; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		db.Put([]byte(key), []byte(value))
	}

	for i := 0; i < test_num; i += 2 {
		key := fmt.Sprintf("%06dtest", i)
		db.Delete([]byte(key))
	}

	for i := 0; i < test_num; i++ {
		key := fmt.Sprintf("%06dtest", i)
		v, err := db.Get([]byte(key))
		if i%2 == 0 {
			if err != ErrKeyNotFound {
				t.Fatalf("key %s should be deleted", key)
			}
		} else {
			if err != nil {
				t.Fatalf("lookup: %s err. %s\n", key, err.Error())
			}
			value := fmt.Sprintf("value%06d", i)
			if value != string(v) {
				t.Fatalf("Expect: %s, but get %s\n", value, v)
			}
		}
	}
}

func TestDB_Scan(t *testing.T) {
	db, destroy := openDB()
	defer destroy()

	test_num := 10000
	for i := 0; i < test_num; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		db.Put([]byte(key), []byte(value))
	}

	iter, _ := db.Scan([]byte(fmt.Sprintf("%06dtest", 10)))
	for i := 10; i < 9990; i++ {
		if !iter.Valid() {
			t.Fatalf("Scan %s failed\n", fmt.Sprintf("%06dtest", i))
		}
		v := iter.Value()
		if Compare(v, []byte(fmt.Sprintf("value%06d", i))) != 0 {
			t.Fatalf("Scan %s failed\n", fmt.Sprintf("%06dtest", i))
		}
		iter.Next()
	}
}

func TestDB_Recover(t *testing.T) {
	path := "/tmp/goleveldb-mydb"
	os.RemoveAll(path)
	option := DefaultOptions()
	option.DirPath = path
	option.BlockSize = 1024
	option.MemTableSize = 1024 * 64

	db, _ := Open(*option)
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		db.Put([]byte(key), []byte(value))
	}
	db.Close()
	db = nil

	time.Sleep(time.Millisecond * time.Duration(100))

	db, _ = Open(*option)
	for i := 5000; i < 10000; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		db.Put([]byte(key), []byte(value))
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		v, _ := db.Get([]byte(key))
		if value != string(v) {
			t.Fatalf("Expect: %s, but get %s\n", key, v)
		}
	}
	os.RemoveAll(path)
}

func TestDB_concurrent_put(t *testing.T) {
	db, destroy := openDB()
	defer destroy()

	thread_num := 2
	test_num := 20000

	var wg sync.WaitGroup
	put_insert := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Nanosecond)
			key := []byte(fmt.Sprintf("%06dtest", i))
			value := []byte(fmt.Sprintf("value%06d", i))
			db.Put(key, value)
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
		go put_insert(start, end)
	}
	wg.Wait()

	db.PrintLevelInfo()

	for i := 99999; i < test_num; i++ {
		key := []byte(fmt.Sprintf("%06dtest", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		v, _ := db.Get(key)
		if Compare(v, value) != 0 {
			t.Fatalf("Get key %s failed! Expect %s, but get %s\n", key, value, v)
		}
	}
}
