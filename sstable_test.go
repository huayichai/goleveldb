package goleveldb

import (
	"bytes"
	"os"
	"testing"
)

func TestSSTableBuild(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/file0"
	os.Remove(path)
	file, _ := NewLinuxFile(path)
	defer file.Close()
	options := NewOptions()
	options.BlockSize = 16
	builder := newTableBuilder(options, file)

	builder.add([]byte("a"), []byte("valuea"))
	builder.add([]byte("b"), []byte("valueb"))
	builder.add([]byte("c"), []byte("valuec"))
	builder.add([]byte("d"), []byte("valued"))
	builder.finish()
}

func TestSSTableRead(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/file0"
	file, _ := NewLinuxFile(path)
	table, _ := openSSTable(file, uint64(file.Size()))

	va, _ := table.get([]byte("a"))
	if bytes.Compare(va, []byte("valuea")) != 0 {
		t.Fatalf("failed get key a")
	}

	vb, _ := table.get([]byte("b"))
	if bytes.Compare(vb, []byte("valueb")) != 0 {
		t.Fatalf("failed get key b")
	}

	vc, _ := table.get([]byte("c"))
	if bytes.Compare(vc, []byte("valuec")) != 0 {
		t.Fatalf("failed get key c")
	}

	vd, _ := table.get([]byte("d"))
	if bytes.Compare(vd, []byte("valued")) != 0 {
		t.Fatalf("failed get key d")
	}

	file.Close()
	os.Remove(path)
}

func TestSSTable1(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/file1"
	os.Remove(path)
	// create sstable
	file, _ := NewLinuxFile(path)
	options := NewOptions()
	options.BlockSize = 16
	builder := newTableBuilder(options, file)
	// add key value
	builder.add([]byte("1name"), []byte("huayichai"))
	builder.add([]byte("2school"), []byte("nju"))
	builder.add([]byte("3age"), []byte("23"))
	builder.add([]byte("4gender"), []byte("male"))
	builder.finish()
	file.Close()

	// lookup sstable
	file, _ = NewLinuxFile(path)
	table, _ := openSSTable(file, uint64(file.Size()))

	v, _ := table.get([]byte("1name"))
	if bytes.Compare(v, []byte("huayichai")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.get([]byte("2school"))
	if bytes.Compare(v, []byte("nju")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.get([]byte("3age"))
	if bytes.Compare(v, []byte("23")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.get([]byte("4gender"))
	if bytes.Compare(v, []byte("male")) != 0 {
		t.Fatalf("failed get key")
	}

	_, s := table.get([]byte("not_exist_key"))
	if s == nil {
		t.Fatalf("failed get key")
	}

	file.Close()
	os.Remove(path)
}
