package skiplist

import (
	"fmt"
	"testing"

	"github.com/huayichai/goleveldb/internal"
)

func Test_SkipList1(t *testing.T) {
	skiplist := New()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03dtest", i)
		value := fmt.Sprintf("value%03d", i)
		memkey := internal.NewMemTableKey(internal.SequenceNumber(i), internal.KTypeValue, []byte(key), []byte(value))
		skiplist.Insert([]byte(memkey))
	}

	iter := skiplist.NewIterator()
	iter.SeekToFirst()
	i := 0
	for ; iter.Valid(); iter.Next() {
		valid_key := []byte(fmt.Sprintf("%03dtest", i))
		valid_value := []byte(fmt.Sprintf("value%03d", i))
		memkey := internal.MemTableKey(iter.Key())
		k, v := memkey.ExtractInternalKey().ExtractUserKey(), memkey.ExtractValue()
		if internal.UserKeyCompare(valid_key, k) != 0 || internal.UserKeyCompare(valid_value, v) != 0 {
			t.Fatal("skiplist get failed")
		}
		i++
	}
}

func Test_SkipList2(t *testing.T) {
	skiplist := New()

	memkey := internal.NewMemTableKey(internal.SequenceNumber(0), internal.KTypeValue, []byte("49test"), []byte("value49"))
	skiplist.Insert([]byte(memkey))
	iter := skiplist.NewIterator()

	lookup_key := internal.NewLookupKey([]byte("0test"), 0)
	iter.Seek([]byte(lookup_key))
	if !iter.Valid() {
		t.Fatal("")
	}

	r := internal.MemTableKey(iter.Key())

	if internal.UserKeyCompare(r.ExtractValue(), []byte("value49")) != 0 {
		t.Fatal("")
	}
}
