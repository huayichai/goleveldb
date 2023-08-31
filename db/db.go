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
	db.Recover()
	return &db
}

func (db *DB) Put(key, value []byte) error {
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	db.mu.Lock()
	seq := db.current.LastSequence
	db.current.LastSequence++
	db.mu.Unlock()

	db.mem.Add(seq, internal.KTypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	snapshot := db.current.LastSequence
	mem := db.mem
	imm := db.imm
	current := db.current
	db.mu.Unlock()

	lookup_key := internal.NewLookupKey(key, snapshot)
	v, ok := mem.Get(lookup_key)
	if ok {
		return v, nil
	}
	if imm != nil {
		v, ok = imm.Get(lookup_key)
		if ok {
			return v, nil
		}
	}

	value, err := current.Get(lookup_key.ExtractInternalKey())
	return value, err
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
			go db.backgroundCompaction()
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
		memkey := internal.MemTableKey(iter.Key())
		meta.Smallest = memkey.ExtractInternalKey().ExtractUserKey()
		for ; iter.Valid(); iter.Next() {
			memkey = internal.MemTableKey(iter.Key())
			internal_key := memkey.ExtractInternalKey()
			value := memkey.ExtractValue()
			meta.Largest = internal_key.ExtractUserKey()
			builder.Add(internal_key, value)
		}
		builder.Finish()
		meta.FileSize = builder.FileSize()
	}

	// add meta to version
	ver.AddFile(0, &meta)

	return nil
}

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// save mem, imm
	for db.imm != nil {
		db.cond.Wait()
	}
	db.writeLevel0Table(db.mem, db.current)

	// save version
	err := db.saveManifestFile()
	if err != nil {
		panic("SaveManifestFile failed")
	}
}

func (db *DB) Recover() {
	_, err := os.Stat(db.dbname)
	// db not exist
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(db.dbname, 0755)
		if err != nil {
			panic("Create db fialed")
		}
		db.current = version.NewVersion(db.dbname)
		return
	} else { // recover from last close
		db.current = version.NewVersion(db.dbname)
		file, err := log.NewLinuxFile(internal.ManifestFileName(db.dbname))
		if err != nil {
			panic("Recover failed")
		}
		data, err := file.Read(0, uint32(file.Size()))
		if err != nil {
			panic("Recover failed")
		}
		db.current.DecodeFrom(data)
	}
}

func (db *DB) saveManifestFile() error {
	manifestContent := db.current.EncodeTo()
	file, err := log.NewLinuxFile(internal.ManifestFileName(db.dbname))
	if err != nil {
		return err
	}
	file.Append(string(manifestContent))
	file.Flush()
	file.Close()
	return nil
}
