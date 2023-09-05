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

	mem MemTable // Memtable
	imm MemTable // Memtable being compacted

	currentLogFileNumber uint64
	logWriter            *LogWriter

	current *Version

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

	// db.mu.Lock()
	seq := db.current.LastSequence
	db.current.LastSequence++
	// db.mu.Unlock()

	if err := db.logWriter.AddRecord([]byte(NewMemTableKey(seq, KTypeValue, key, value))); err != nil {
		panic("wal write failed")
	}
	db.mem.Add(seq, KTypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// db.mu.Lock()
	snapshot := db.current.LastSequence
	mem := db.mem
	imm := db.imm
	current := db.current
	// db.mu.Unlock()

	lookup_key := NewLookupKey(key, snapshot)
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
	// db.mu.Lock()
	// defer db.mu.Unlock()
	for {
		if db.current.NumLevelFiles(0) >= L0_SlowdownWritesTrigger {
			// db.mu.Unlock()
			time.Sleep(time.Duration(1) * time.Second)
			db.maybeScheduleCompaction()
			// db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() < uint64(db.option.Write_buffer_size) {
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

func (db *DB) writeLevel0Table(imm MemTable, ver *Version) error {
	// FileMetaData
	var meta FileMetaData
	meta.Number = ver.NextFileNumber

	// file
	ver.NextFileNumber++
	filename := SSTableFileName(db.dbname, meta.Number)
	file, err := NewLinuxFile(filename)
	if err != nil {
		return err
	}

	// sstable build
	builder := NewTableBuilder(&db.option, file)

	iter := imm.Iterator()
	iter.SeekToFirst()
	if iter.Valid() {
		memkey := MemTableKey(iter.Key())
		meta.Smallest = memkey.ExtractInternalKey()
		for ; iter.Valid(); iter.Next() {
			memkey = MemTableKey(iter.Key())
			internal_key := memkey.ExtractInternalKey()
			value := memkey.ExtractValue()
			meta.Largest = internal_key
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
	// db.mu.Lock()
	// defer db.mu.Unlock()

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
		db.current = NewVersion(db.dbname)
		return
	} else { // recover from last close
		db.current = NewVersion(db.dbname)
		file, err := NewLinuxFile(ManifestFileName(db.dbname))
		if err != nil {
			panic("Recover failed")
		}
		data, err := file.Read(0, uint32(file.Size()))
		if err != nil {
			panic("Recover failed")
		}
		db.currentLogFileNumber = DecodeFixed64(data)
		db.recoverMemTable()
		db.current.DecodeFrom(data[8:])
	}
}

func (db *DB) saveManifestFile() error {
	file, err := NewLinuxFile(ManifestFileName(db.dbname))
	if err != nil {
		return err
	}

	p := make([]byte, 8)
	EncodeFixed64(p, db.currentLogFileNumber)
	manifestContent := db.current.EncodeTo()
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
	db.currentLogFileNumber = db.current.NextFileNumber
	db.current.NextFileNumber++
	LogPath := LogFileName(db.dbname, db.currentLogFileNumber)
	logFile, err := NewLinuxFile(LogPath)
	if err != nil {
		return err
	}
	db.logWriter = NewLogWriter(logFile)

	// new memtable
	db.mem = NewMemTable(LogPath)

	return nil
}

func (db *DB) recoverMemTable() error {
	logPath := LogFileName(db.dbname, db.currentLogFileNumber)
	file, err := NewLinuxFile(logPath)
	if err != nil {
		return err
	}
	db.mem = NewMemTable(logPath)
	reader := NewLogReader(file)
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}
		memkey := MemTableKey(record)
		db.mem.Add(memkey.ExtractInternalKey().ExtractSequenceNumber(),
			memkey.ExtractInternalKey().ExtractValueType(),
			memkey.ExtractInternalKey().ExtractUserKey(), memkey.ExtractValue())
	}
	db.logWriter = NewLogWriter(file)
	return nil
}
