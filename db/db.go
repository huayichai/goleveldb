package db

import (
	"os"
	"sync"
	"time"

	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
	"github.com/huayichai/goleveldb/memtable"
	"github.com/huayichai/goleveldb/sstable"
	"github.com/huayichai/goleveldb/version"
)

type DB struct {
	// Constant after construction
	dbname string // As root dir name
	option internal.Options

	mem memtable.MemTable // Memtable
	imm memtable.MemTable // Memtable being compacted

	current *version.Version

	mu   sync.Mutex
	cond *sync.Cond
}

func Open(option internal.Options, name string) *DB {
	var db DB
	db.dbname = name
	db.option = option
	db.mem = memtable.NewMemTable()
	db.imm = nil
	db.cond = sync.NewCond(&db.mu)
	db.current = version.NewVersion(db.dbname)

	// create db dir
	_, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(name, 0755)
			if err != nil {
				panic("create dir fialed")
			}
		}
	}

	return &db
}

func (db *DB) Close() {

}

func (db *DB) Put(key, value string) error {
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}
	db.mem.Add(internal.KTypeValue, key, value)
	return nil
}

func (db *DB) Get(key string) (string, error) {
	v, ok := db.mem.Get(key)
	if ok {
		return v, nil
	}
	if db.imm != nil {
		v, ok = db.imm.Get(key)
		if ok {
			return v, nil
		}
	}

	value, err := db.current.Get([]byte(key))
	return string(value), err
}

func (db *DB) makeRoomForWrite() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for {
		if db.current.NumLevelFiles(0) >= internal.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1) * time.Second)
			db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() < uint64(db.option.Write_buffer_size) {
			// There is room in current memtable
			return nil
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			db.cond.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			db.imm = db.mem
			db.mem = memtable.NewMemTable()
			db.backgroundCompaction()
		}
	}
}

func (db *DB) writeLevel0Table(imm memtable.MemTable, ver *version.Version) error {
	// FileMetaData
	var meta version.FileMetaData
	meta.Number = ver.NextFileNumber

	// file
	ver.NextFileNumber++
	filename := internal.SSTableFileName(db.dbname, meta.Number)
	file, err := log.NewLinuxFile(filename)
	if err != nil {
		return err
	}

	// sstable build
	builder := sstable.NewTableBuilder(&db.option, file)

	iter := imm.Iterator()
	iter.SeekToFirst()
	if iter.Valid() {
		meta.Smallest = iter.Key()
		for ; iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()
			meta.Largest = key
			builder.Add(string(key), string(value))
		}
		builder.Finish()
		meta.FileSize = builder.FileSize()
	}

	// add meta to version
	ver.AddFile(0, &meta)

	return nil
}
