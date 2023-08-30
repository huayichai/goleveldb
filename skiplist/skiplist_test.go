package skiplist

import (
	"fmt"
	"testing"

	"github.com/huayichai/goleveldb/internal"
)

func Test_SkipList1(t *testing.T) {
	skiplist := New()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%3dtest", i)
		value := fmt.Sprintf("value%3d", i)
		i_kv := internal.EncodeInternalKVEntry([]byte(key), []byte(value))
		skiplist.Insert(i_kv)
	}

	iter := skiplist.NewIterator()
	i := 0
	for ; iter.Valid(); iter.Next() {
		valid_key := []byte(fmt.Sprintf("%3dtest", i))
		valid_value := []byte(fmt.Sprintf("value%3d", i))
		k, v := iter.Key(), iter.Value()
		if internal.Compare(valid_key, k) != 0 || internal.Compare(valid_value, v) != 0 {
			t.Fatal("skiplist get failed")
		}
	}
}

func Test_SkipList2(t *testing.T) {
	skiplist := New()

	skiplist.Insert(internal.EncodeInternalKVEntry([]byte("49test"), []byte("value49")))
	iter := skiplist.NewIterator()
	iter.Seek([]byte("0test"))
	if !iter.Valid() {
		t.Fatal("")
	}
	if internal.Compare(iter.Value(), []byte("value49")) != 0 {
		t.Fatal("")
	}
}
