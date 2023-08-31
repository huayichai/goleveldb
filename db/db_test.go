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
		key := fmt.Sprintf("%03dtest", i)
		value := fmt.Sprintf("value%03d", i)
		db.Put([]byte(key), []byte(value))
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03dtest", i)
		value := fmt.Sprintf("value%03d", i)
		v, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("lookup: %s err. %s\n", key, err.Error())
		}
		if value != string(v) {
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
		key := fmt.Sprintf("%03dtest", i)
		value := fmt.Sprintf("value%03d", i)
		db.Put([]byte(key), []byte(value))
	}
	db.Close()
	db = nil

	time.Sleep(time.Millisecond * time.Duration(100))

	db = Open(*option, path)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03dtest", i)
		value := fmt.Sprintf("value%03d", i)
		if i == 5 {
			i = 5
		}
		v, _ := db.Get([]byte(key))
		if value != string(v) {
			t.Fatalf("Expect: %s, but get %s\n", key, v)
		}
	}
	os.RemoveAll(path)
}
