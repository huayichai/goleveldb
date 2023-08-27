package memtable

import (
	"fmt"
	"testing"

	"github.com/huayichai/goleveldb/internal"
)

func Test_MapTable(t *testing.T) {
	memtable := NewMapTable()
	memtable.Add(internal.KTypeValue, "name", "huayichai")
	v, ok := memtable.Get("name")
	if !ok || v != "huayichai" {
		fmt.Printf("MapTable Get Error")
	}
}
