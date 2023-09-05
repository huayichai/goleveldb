package goleveldb

import (
	"os"
	"sync"
	"time"
)

type DB struct {
	// Constant after construction
	option Options

	mem *memTable // Memtable
	imm *memTable // Memtable being compacted

	cache *tableCache

	currentLogFileNumber uint64
	logWriter            *walWriter

	current *version

	backgroundCompactionScheduled bool

	mu                           sync.Mutex
	backgroundWorkFinishedSignal *sync.Cond
}

func Open(option Options) (*DB, error) {
	var db DB
	var err error
	db.option = option
	db.backgroundCompactionScheduled = false

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

	db.backgroundWorkFinishedSignal = sync.NewCond(&db.mu)

	return &db, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	seq := db.current.lastSequence
	db.current.lastSequence++

	if err := db.logWriter.addRecord([]byte(NewKVEntry(seq, KTypeValue, key, value))); err != nil {
		return err
	}
	db.mem.add(seq, KTypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	snapshot := db.current.lastSequence
	mem := db.mem
	imm := db.imm
	current := db.current

	lookup_key := NewLookupKey(key, snapshot)
	v, ok := mem.get(lookup_key)
	if ok {
		return v, nil
	}
	if imm != nil {
		v, ok = imm.get(lookup_key)
		if ok {
			return v, nil
		}
	}

	value, err := current.get(lookup_key.ExtractInternalKey())
	return value, err
}

func (db *DB) makeRoomForWrite() error {
	for {
		if db.current.numLevelFiles(0) >= L0_SlowdownWritesTrigger {
			time.Sleep(time.Duration(1) * time.Second)
			db.maybeScheduleCompaction()
		} else if db.mem.approximateMemoryUsage() < uint64(db.option.MemTableSize) {
			// There is room in current memtable
			return nil
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			db.backgroundWorkFinishedSignal.Wait() // will release the mutex 'db.mu'
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			if err := db.switchToNewMemTable(); err != nil {
				return err
			}
			db.maybeScheduleCompaction()
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
		memkey := KVEntry(iter.Key())
		meta.smallest = memkey.ExtractInternalKey()
		for ; iter.Valid(); iter.Next() {
			memkey = KVEntry(iter.Key())
			internal_key := memkey.ExtractInternalKey()
			value := memkey.ExtractValue()
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
	db.mu.Lock()
	defer db.mu.Unlock()
	// wait background compaction
	for db.imm != nil {
		db.backgroundWorkFinishedSignal.Wait()
	}

	// save version
	err := db.saveManifestFile()
	if err != nil {
		return err
	}
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
