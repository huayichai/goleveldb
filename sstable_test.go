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

func Test_SSTable_GetDiffVersion(t *testing.T) {
	options := DefaultOptions()
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
	builder := newTableBuilder(options, file)
	for i := 9; i >= 0; i-- {
		i_k := NewInternalKey([]byte("key"), SequenceNumber(i), KTypeValue)
		builder.add(i_k, []byte(fmt.Sprintf("v%d", i)))
	}
	builder.finish()

	table, _ := openSSTable(sstableFileName(options.DirPath, 1))
	for i := 0; i < 10; i++ {
		i_k := NewInternalKey([]byte("key"), SequenceNumber(i), KTypeValue)
		v, _ := table.get(i_k)
		if !bytes.Equal(v, []byte(fmt.Sprintf("v%d", i))) {
			t.Fatalf("lookup key failed\n")
		}
	}
}
