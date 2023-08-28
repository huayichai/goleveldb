package db

import (
	"fmt"

	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/memtable"
)

type DB struct {
	// Constant after construction
	dbname string // As root dir name
	option internal.Options

	mem memtable.MemTable // Memtable
	imm memtable.MemTable // Memtable being compacted
}

func Open(option internal.Options, name string) *DB {
	return &DB{
		dbname: name,
		mem:    memtable.NewMemTable(),
		imm:    nil,
	}
}

func (db *DB) Close() {

}

func (db *DB) Put(key, value string) Status {
	db.mem.Add(internal.KTypeValue, key, value)
	return StatusOK()
}

func (db *DB) Get(key string) (string, Status) {
	v, ok := db.mem.Get(key)
	if ok {
		return v, StatusOK()
	} else {
		return v, StatusNotFound(fmt.Sprintf("key: %s not exist", key))
	}
}

func (db *DB) makeRoomForWrite() Status {
	for {
		if db.mem.ApproximateMemoryUsage() < uint64(db.option.Write_buffer_size) {
			// There is room in current memtable
			return StatusOK()
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			db.imm = db.mem
			db.mem = memtable.NewMemTable()
		}
	}
}
