package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/huayichai/goleveldb/internal"
)

// func TestConstruct(t *testing.T) {
// 	option := internal.NewOptions()
// 	db := Open(*option, "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/mydb")
// 	err := db.Put("name", "huayichai")
// 	if err != nil {
// 		fmt.Println("put error")
// 	}

// 	v, err := db.Get("name")
// 	if err != nil || v != "huayichai" {
// 		fmt.Println("get error")
// 	}
// }

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
}
