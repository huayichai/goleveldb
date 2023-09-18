package goleveldb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func createDir(dirPath string) error {
	_, err := os.Stat(dirPath)
	if err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, 0755); err != nil {
			return err
		}
	}
	return nil
}

func Test_SSTable_PutGet(t *testing.T) {
	options := DefaultOptions()
	options.BlockSize = 128
	options.BlockRestartInterval = 4
	options.DirPath = "/tmp/golevel-sstable"
	os.RemoveAll(options.DirPath)
	if err := createDir(options.DirPath); err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(options.DirPath)
	}()
	file, err := NewLinuxFile(sstableFileName(options.DirPath, 1))
	if err != nil {
		panic(err)
	}
	test_num := 500
	builder := newTableBuilder(options, file)
	for i := 0; i < test_num; i++ {
		i_k := NewInternalKey([]byte(fmt.Sprintf("key%04d", i)), SequenceNumber(i), KTypeValue)
		builder.add(i_k, []byte(fmt.Sprintf("v%d", i)))
	}
	builder.finish()

	table, _ := openSSTable(sstableFileName(options.DirPath, 1))
	for i := 0; i < test_num; i++ {
		i_k := NewInternalKey([]byte(fmt.Sprintf("key%04d", i)), SequenceNumber(i), KTypeValue)
		v, _ := table.get(i_k)
		if !bytes.Equal(v, []byte(fmt.Sprintf("v%d", i))) {
			t.Fatalf("lookup key%04d failed\n", i)
		}
	}

	iter := newSSTableIterator(table)
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := NewInternalKey([]byte(fmt.Sprintf("key%04d", i)), SequenceNumber(i), KTypeValue)
		v := []byte(fmt.Sprintf("v%d", i))
		if InternalKeyCompare(iter.Key(), k) != 0 || Compare(iter.Value(), v) != 0 {
			t.Fatalf("scan key%04d failed\n", i)
		}
		i++
	}
}
