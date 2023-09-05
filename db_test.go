package goleveldb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDB1(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/mydb"
	os.RemoveAll(path)
	option := DefaultOptions()
	option.DirPath = path
	option.BlockSize = 1024
	option.MemTableSize = 1024 * 64

	db, _ := Open(*option)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		db.Put([]byte(key), []byte(value))
	}

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%06dtest", i)
		value := fmt.Sprintf("value%06d", i)
		v, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("lookup: %s err. %s\n", key, err.Error())
		}
		if value != string(v) {
			t.Fatalf("Expect: %s, but get %s\n", key, v)
		}
	}

	db.Close()
	os.RemoveAll(path)
}

func TestDB_Recover(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/RecoverDB"
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
