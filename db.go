package goleveldb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var Debug bool = false

type DB struct {
	// Constant after construction
	option Options

	mem *memTable // Memtable
	imm *memTable // Memtable being compacted

	cache *tableCache

	currentLogFileNumber uint64
	logWriter            *walWriter

	current *version

	immExistCh chan bool
	dbCloseCh  chan bool

	muCompaction sync.Mutex

	mu sync.Mutex
}

func Open(option Options) (*DB, error) {
	var db DB
	var err error
	db.option = option
	db.immExistCh = make(chan bool, 1)
	db.dbCloseCh = make(chan bool, 1)

	// init TableCache
	db.cache, err = newTableCache(&db.option)
	if err != nil {
		return nil, err
	}

	// recover from last close
	if err = db.Recover(); err != nil {
		return nil, err
	}

	if db.mem == nil {
		if err = db.switchToNewMemTable(); err != nil {
			return nil, err
		}
	}

	go db.backgroundCompaction()

	return &db, nil
}

func (db *DB) Put(key, value []byte) error {
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	db.mu.Lock()
	seq := db.current.lastSequence
	db.current.lastSequence++
	db.mu.Unlock()

	// write ahead log
	if err := db.logWriter.addRecord([]byte(NewKVEntry(seq, KTypeValue, key, value))); err != nil {
		return err
	}

	// insert into memtable
	db.mem.add(seq, KTypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	snapshot := db.current.lastSequence
	mem := db.mem
	imm := db.imm
	current := db.current
	db.mu.Unlock()

	internal_key := NewInternalKey(key, snapshot, KTypeValue)
	v, status := mem.get(internal_key)
	if status == nil {
		return v, nil
	} else if status == errKeyDeleted {
		return nil, ErrKeyNotFound
	} else if imm != nil {
		v, status = imm.get(internal_key)
		if status == nil {
			return v, nil
		} else if status == errKeyDeleted {
			return nil, ErrKeyNotFound
		}
	}

	db.muCompaction.Lock()
	defer db.muCompaction.Unlock()
	value, err := current.get(internal_key)
	return value, err
}

func (db *DB) Scan(key []byte) (Iterator, error) {
	snapshot := db.current.lastSequence
	internal_key := NewInternalKey(key, snapshot, KTypeValue)

	var list [][]Iterator
	db.muCompaction.Lock()
	defer db.muCompaction.Unlock()

	var l1 []Iterator
	if db.mem != nil {
		l1 = append(l1, db.mem.iterator())
		list = append(list, l1)
	}

	var l2 []Iterator
	if db.imm != nil {
		l2 = append(l2, db.imm.iterator())
		list = append(list, l2)
	}

	for i := 0; i < len(db.current.files); i++ {
		level_num := len(db.current.files[i])
		if level_num == 0 {
			break
		}
		if i == 0 {
			for j := 0; j < level_num; j++ {
				table, err := db.cache.getTable(db.current.files[i][j].number)
				if err != nil {
					return nil, err
				}
				var tmp []Iterator
				tmp = append(tmp, newSSTableIterator(table))
				list = append(list, tmp)
			}
		} else {
			var tmp []Iterator
			for j := 0; j < level_num; j++ {
				table, err := db.cache.getTable(db.current.files[i][j].number)
				if err != nil {
					return nil, err
				}
				tmp = append(tmp, newSSTableIterator(table))

			}
			list = append(list, tmp)
		}
	}

	iter := newDeduplicationIterator(newMergeIterator(list))

	iter.Seek(internal_key)
	return iter, nil
}

func (db *DB) Delete(key []byte) error {
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	db.mu.Lock()
	seq := db.current.lastSequence
	db.current.lastSequence++
	db.mu.Unlock()

	if err := db.logWriter.addRecord([]byte(NewKVEntry(seq, KTypeDeletion, key, []byte{}))); err != nil {
		return err
	}
	db.mem.add(seq, KTypeDeletion, key, []byte{})
	return nil
}

func (db *DB) makeRoomForWrite() error {
	for {
		if db.current.numLevelFiles(0) >= L0_SlowdownWritesTrigger {
			time.Sleep(time.Duration(1) * time.Second)
		} else if db.mem.approximateMemoryUsage() < uint64(db.option.MemTableSize) {
			// There is room in current memtable
			return nil
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			time.Sleep(time.Duration(100+rand.Intn(100)) * time.Nanosecond)
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			db.mu.Lock()
			db.muCompaction.Lock()
			if db.imm == nil {
				if err := db.switchToNewMemTable(); err != nil {
					db.muCompaction.Unlock()
					db.mu.Unlock()
					return err
				}
				db.immExistCh <- true // notify background compaction
			}
			db.muCompaction.Unlock()
			db.mu.Unlock()
		}
	}
}

func (db *DB) writeLevel0Table(imm *memTable, ver *version) error {
	// FileMetaData
	var meta fileMetaData
	meta.number = ver.nextFileNumber

	// file
	ver.nextFileNumber++
	filename := sstableFileName(db.option.DirPath, meta.number)
	file, err := NewLinuxFile(filename)
	if err != nil {
		return err
	}

	// sstable build
	builder := newTableBuilder(&db.option, file)

	iter := imm.iterator()
	iter.SeekToFirst()
	if iter.Valid() {
		internal_key := InternalKey(iter.Key())
		meta.smallest = internal_key
		for ; iter.Valid(); iter.Next() {
			internal_key = InternalKey(iter.Key())
			value := iter.Value()
			meta.largest = internal_key
			builder.add(internal_key, value)
		}
		builder.finish()
		meta.fileSize = builder.fileSize()
	}

	// add meta to version
	ver.addFile(0, &meta)
	return nil
}

func (db *DB) Close() error {
	db.muCompaction.Lock()
	db.mu.Lock()
	defer db.mu.Unlock()
	defer db.muCompaction.Unlock()

	db.dbCloseCh <- true

	// save version
	if err := db.saveManifestFile(); err != nil {
		return err
	}

	// close wal file
	if err := db.logWriter.close(); err != nil {
		return err
	}

	fmt.Print("DB close successfully! Bye~")
	return nil
}

func (db *DB) Recover() error {
	db.mem, db.imm = nil, nil
	dbpath := db.option.DirPath
	_, err := os.Stat(dbpath)
	// db not exist
	if err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(dbpath, 0755); err != nil {
			return err
		}
		db.current = newVersion(db.cache)
		return nil
	} else { // recover from last close
		db.current = newVersion(db.cache)
		file, err := NewLinuxFile(manifestFileName(dbpath))
		if err != nil {
			return err
		}
		data, err := file.Read(0, uint32(file.Size()))
		if err != nil {
			return err
		}
		db.currentLogFileNumber = DecodeFixed64(data)
		db.recoverMemTable()
		db.current.decodeFrom(data[8:])
	}
	return nil
}

func (db *DB) saveManifestFile() error {
	file, err := NewLinuxFile(manifestFileName(db.option.DirPath))
	if err != nil {
		return err
	}

	p := make([]byte, 8)
	EncodeFixed64(p, db.currentLogFileNumber)
	manifestContent := db.current.encodeTo()
	p = append(p, manifestContent...)
	file.Append(string(p))
	file.Sync()
	file.Close()
	return nil
}

// switch mem to imm, and create new memtable and walWriter
func (db *DB) switchToNewMemTable() error {
	// switch mem to imm
	db.imm = db.mem

	// close old wal file
	if db.logWriter != nil {
		if err := db.logWriter.close(); err != nil {
			return err
		}
	}

	// new write ahead log
	db.currentLogFileNumber = db.current.nextFileNumber
	db.current.nextFileNumber++
	LogPath := walFileName(db.option.DirPath, db.currentLogFileNumber)
	logFile, err := NewLinuxFile(LogPath)
	if err != nil {
		return err
	}
	db.logWriter = newWALWriter(logFile, db.option.Sync)

	// new memtable
	db.mem = newMemTable(LogPath)

	return nil
}

func (db *DB) recoverMemTable() error {
	logPath := walFileName(db.option.DirPath, db.currentLogFileNumber)
	file, err := NewLinuxFile(logPath)
	if err != nil {
		return err
	}
	db.mem = newMemTable(logPath)
	reader := newWALReader(file)
	for {
		record, err := reader.readRecord()
		if err != nil {
			break
		}
		memkey := KVEntry(record)
		db.mem.add(memkey.ExtractInternalKey().ExtractSequenceNumber(),
			memkey.ExtractInternalKey().ExtractValueType(),
			memkey.ExtractInternalKey().ExtractUserKey(), memkey.ExtractValue())
	}
	db.logWriter = newWALWriter(file, db.option.Sync)
	return nil
}

func (db *DB) SpaceConsumption() (int64, error) {
	var size int64
	err := filepath.Walk(db.option.DirPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func (db *DB) PrintLevelInfo() {
	db.mu.Lock()
	db.muCompaction.Lock()
	defer db.muCompaction.Unlock()
	defer db.mu.Unlock()

	db.current.info()
}
