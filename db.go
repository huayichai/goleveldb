package goleveldb

import (
	"os"
	"sync"
	"time"
)

type DB struct {
	// Constant after construction
	dbname string // As root dir name
	option Options

	mem *memTable // Memtable
	imm *memTable // Memtable being compacted

	currentLogFileNumber uint64
	logWriter            *walWriter

	current *version

	backgroundCompactionScheduled bool

	mu                           sync.Mutex
	backgroundWorkFinishedSignal *sync.Cond
}

func Open(option Options, name string) *DB {
	var db DB
	var err error
	db.dbname = name
	db.option = option
	db.backgroundCompactionScheduled = false

	// recover from last close
	db.Recover()
	if db.mem == nil {
		if err = db.switchToNewMemTable(); err != nil {
			panic("create log file fialed")
		}
	}

	db.backgroundWorkFinishedSignal = sync.NewCond(&db.mu)

	return &db
}

func (db *DB) Put(key, value []byte) error {
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	seq := db.current.lastSequence
	db.current.lastSequence++

	if err := db.logWriter.addRecord([]byte(NewKVEntry(seq, KTypeValue, key, value))); err != nil {
		panic("wal write failed")
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
		} else if db.mem.approximateMemoryUsage() < uint64(db.option.Write_buffer_size) {
			// There is room in current memtable
			return nil
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			db.backgroundWorkFinishedSignal.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			db.switchToNewMemTable()
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
	filename := sstableFileName(db.dbname, meta.number)
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

func (db *DB) Close() {
	// wait background compaction
	for db.imm != nil {
		db.backgroundWorkFinishedSignal.Wait()
	}

	// save version
	err := db.saveManifestFile()
	if err != nil {
		panic("SaveManifestFile failed")
	}
}

func (db *DB) Recover() {
	db.mem, db.imm = nil, nil
	_, err := os.Stat(db.dbname)
	// db not exist
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(db.dbname, 0755)
		if err != nil {
			panic("Create db fialed")
		}
		db.current = newVersion(db.dbname)
		return
	} else { // recover from last close
		db.current = newVersion(db.dbname)
		file, err := NewLinuxFile(manifestFileName(db.dbname))
		if err != nil {
			panic("Recover failed")
		}
		data, err := file.Read(0, uint32(file.Size()))
		if err != nil {
			panic("Recover failed")
		}
		db.currentLogFileNumber = DecodeFixed64(data)
		db.recoverMemTable()
		db.current.decodeFrom(data[8:])
	}
}

func (db *DB) saveManifestFile() error {
	file, err := NewLinuxFile(manifestFileName(db.dbname))
	if err != nil {
		return err
	}

	p := make([]byte, 8)
	EncodeFixed64(p, db.currentLogFileNumber)
	manifestContent := db.current.encodeTo()
	p = append(p, manifestContent...)
	file.Append(string(p))
	file.Flush()
	file.Close()
	return nil
}

func (db *DB) switchToNewMemTable() error {
	// switch mem to imm
	db.imm = db.mem

	// new write ahead log
	db.currentLogFileNumber = db.current.nextFileNumber
	db.current.nextFileNumber++
	LogPath := walFileName(db.dbname, db.currentLogFileNumber)
	logFile, err := NewLinuxFile(LogPath)
	if err != nil {
		return err
	}
	db.logWriter = newWALWriter(logFile)

	// new memtable
	db.mem = newMemTable(LogPath)

	return nil
}

func (db *DB) recoverMemTable() error {
	logPath := walFileName(db.dbname, db.currentLogFileNumber)
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
	db.logWriter = newWALWriter(file)
	return nil
}
