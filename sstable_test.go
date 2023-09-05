package goleveldb

import (
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
	builder := NewTableBuilder(options, file)

	builder.Add([]byte("a"), []byte("valuea"))
	builder.Add([]byte("b"), []byte("valueb"))
	builder.Add([]byte("c"), []byte("valuec"))
	builder.Add([]byte("d"), []byte("valued"))
	builder.Finish()
}

func TestSSTableRead(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/file0"
	file, _ := NewLinuxFile(path)
	table, _ := OpenSSTable(file, uint64(file.Size()))

	va, _ := table.Get([]byte("a"))
	if Compare(va, []byte("valuea")) != 0 {
		t.Fatalf("failed get key a")
	}

	vb, _ := table.Get([]byte("b"))
	if Compare(vb, []byte("valueb")) != 0 {
		t.Fatalf("failed get key b")
	}

	vc, _ := table.Get([]byte("c"))
	if Compare(vc, []byte("valuec")) != 0 {
		t.Fatalf("failed get key c")
	}

	vd, _ := table.Get([]byte("d"))
	if Compare(vd, []byte("valued")) != 0 {
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
	builder := NewTableBuilder(options, file)
	// add key value
	builder.Add([]byte("1name"), []byte("huayichai"))
	builder.Add([]byte("2school"), []byte("nju"))
	builder.Add([]byte("3age"), []byte("23"))
	builder.Add([]byte("4gender"), []byte("male"))
	builder.Finish()
	file.Close()

	// lookup sstable
	file, _ = NewLinuxFile(path)
	table, _ := OpenSSTable(file, uint64(file.Size()))

	v, _ := table.Get([]byte("1name"))
	if Compare(v, []byte("huayichai")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.Get([]byte("2school"))
	if Compare(v, []byte("nju")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.Get([]byte("3age"))
	if Compare(v, []byte("23")) != 0 {
		t.Fatalf("failed get key")
	}

	v, _ = table.Get([]byte("4gender"))
	if Compare(v, []byte("male")) != 0 {
		t.Fatalf("failed get key")
	}

	_, s := table.Get([]byte("not_exist_key"))
	if s == nil {
		t.Fatalf("failed get key")
	}

	file.Close()
	os.Remove(path)
}
