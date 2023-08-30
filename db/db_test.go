package db

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/huayichai/goleveldb/internal"
)

func TestDB1(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/mydb"
	os.RemoveAll(path)
	option := internal.NewOptions()
	option.BlockSize = 128
	option.Write_buffer_size = 1024

	db := Open(*option, path)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%3dtest", i)
		value := fmt.Sprintf("value%3d", i)
		db.Put(key, value)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%3dtest", i)
		value := fmt.Sprintf("value%3d", i)
		if i == 5 {
			i = 5
		}
		v, _ := db.Get(key)
		if value != v {
			t.Fatalf("Expect: %s, but get %s\n", key, v)
		}
	}
	os.RemoveAll(path)
}

func TestDB_CloseRecover(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/RecoverDB"
	os.RemoveAll(path)
	option := internal.NewOptions()
	option.BlockSize = 128
	option.Write_buffer_size = 1024
	db := Open(*option, path)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%3dtest", i)
		value := fmt.Sprintf("value%3d", i)
		db.Put(key, value)
	}
	db.Close()
	db = nil

	time.Sleep(time.Millisecond * time.Duration(100))

	db = Open(*option, path)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%3dtest", i)
		value := fmt.Sprintf("value%3d", i)
		if i == 5 {
			i = 5
		}
		v, _ := db.Get(key)
		if value != v {
			t.Fatalf("Expect: %s, but get %s\n", key, v)
		}
	}
	os.RemoveAll(path)
}
