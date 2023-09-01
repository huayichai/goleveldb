package wal

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
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
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/wal"
	os.RemoveAll(path)
	file, err := log.NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}

	record_a := GenerateSameBytes(10, byte('a'))
	record_b := GenerateSameBytes(40, byte('b'))
	record_c := GenerateSameBytes(4, byte('c'))
	records := [][]byte{record_a, record_b, record_c}

	// write log
	log_writer := NewLogWriter(file)
	for i := 0; i < len(records); i++ {
		err = log_writer.AddRecord(records[i])
		if err != nil {
			t.Fatal("log_writer add record failed")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	// read log
	file, err = log.NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}
	log_reader := NewLogReader(file)
	for i := 0; i < len(records); i++ {
		v, err := log_reader.ReadRecord()
		if err != nil {
			t.Fatal("log_reader read record failed")
		}
		if internal.Compare(records[i], v) != 0 {
			t.Fatal("log_reader read false record")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	os.RemoveAll(path)
}

func Test_wal2(t *testing.T) {
	path := "/home/ubuntu/huayichai/MyToyCode/goleveldb/data/wal"
	os.RemoveAll(path)
	file, err := log.NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}

	record_a := GenerateRandomBytes(1000)
	record_b := GenerateRandomBytes(97270)
	record_c := GenerateRandomBytes(8000)
	records := [][]byte{record_a, record_b, record_c}

	// write log
	log_writer := NewLogWriter(file)
	for i := 0; i < len(records); i++ {
		err = log_writer.AddRecord(records[i])
		if err != nil {
			t.Fatal("log_writer add record failed")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	// read log
	file, err = log.NewLinuxFile(path)
	if err != nil {
		t.Fatal("create writable file failed")
	}
	log_reader := NewLogReader(file)
	for i := 0; i < len(records); i++ {
		v, err := log_reader.ReadRecord()
		if err != nil {
			t.Fatal("log_reader read record failed")
		}
		if internal.Compare(records[i], v) != 0 {
			t.Fatal("log_reader read false record")
		}
	}
	file.Close()
	file = nil
	log_writer = nil

	os.RemoveAll(path)
}
