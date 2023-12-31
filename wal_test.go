package goleveldb

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"
)

func GenerateRandomBytes(l int) []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := make([]byte, l)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result[i] = bytes[r.Intn(len(bytes))]
	}
	return result
}

func GenerateSameBytes(l int, b byte) []byte {
	result := make([]byte, l)
	for i := 0; i < l; i++ {
		result[i] = b
	}
	return result
}

func Test_wal1(t *testing.T) {
	path := "/tmp/goleveldb-wal"
	os.RemoveAll(path)
	file, err := NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}

	record_a := GenerateSameBytes(10, byte('a'))
	record_b := GenerateSameBytes(40, byte('b'))
	record_c := GenerateSameBytes(4, byte('c'))
	records := [][]byte{record_a, record_b, record_c}

	// write log
	log_writer := newWALWriter(file, false)
	for i := 0; i < len(records); i++ {
		err = log_writer.addRecord(records[i])
		if err != nil {
			t.Fatal("log_writer add record failed")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	// read log
	file, err = NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}
	log_reader := newWALReader(file)
	for i := 0; i < len(records); i++ {
		v, err := log_reader.readRecord()
		if err != nil {
			t.Fatal("log_reader read record failed")
		}
		if !bytes.Equal(records[i], v) {
			t.Fatal("log_reader read false record")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	os.RemoveAll(path)
}

func Test_wal2(t *testing.T) {
	path := "/tmp/goleveldb-wal"
	os.RemoveAll(path)
	file, err := NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}

	record_a := GenerateRandomBytes(1000)
	record_b := GenerateRandomBytes(97270)
	record_c := GenerateRandomBytes(8000)
	records := [][]byte{record_a, record_b, record_c}

	// write log
	log_writer := newWALWriter(file, false)
	for i := 0; i < len(records); i++ {
		err = log_writer.addRecord(records[i])
		if err != nil {
			t.Fatal("log_writer add record failed")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	// read log
	file, err = NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}
	log_reader := newWALReader(file)
	for i := 0; i < len(records); i++ {
		v, err := log_reader.readRecord()
		if err != nil {
			t.Fatal("log_reader read record failed")
		}
		if !bytes.Equal(records[i], v) {
			t.Fatal("log_reader read false record")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	os.RemoveAll(path)
}
