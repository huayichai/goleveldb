package goleveldb

import (
	"time"
)

func (db *DB) backgroundCompaction() {
	interval := db.option.CompactionInterval
	timer := time.NewTimer(time.Millisecond * time.Duration(interval))
	var err error
	for {
		select {
		case <-db.dbCloseCh:
			return
		case <-db.immExistCh:
			err = db.compactMemTable()
		case <-timer.C:
			err = db.maybeScheduleCompaction()
		}
		if err != nil {
			panic(err.Error())
		}
		timer.Reset(time.Millisecond * time.Duration(interval))
	}
}

func (db *DB) compactMemTable() error {
	db.muCompaction.Lock()
	defer db.muCompaction.Unlock()
	if db.imm == nil {
		return nil
	}
	if err := db.writeLevel0Table(db.imm, db.current); err != nil {
		return err
	}
	wal_path := db.imm.getLogPath()
	db.imm = nil
	if err := RemoveFile(wal_path); err != nil {
		return err
	}
	return nil
}

func (db *DB) maybeScheduleCompaction() error {
	db.muCompaction.Lock()
	if db.imm != nil {
		db.muCompaction.Unlock()
		return db.compactMemTable()
	}

	defer db.muCompaction.Unlock()
	c := db.current.pickCompaction()
	if c == nil {
		return nil
	} else if c.isTrivialMove() {
		db.current.deleteFile(c.level, c.inputs[0][0], false)
		db.current.addFile(c.level+1, c.inputs[0][0])
	} else {
		if err := db.doCompaction(c); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) doCompaction(c *compaction) error {
	var list []*fileMetaData
	iter, err := db.makeInputIterator(c)
	if err != nil {
		return err
	}
	var prev_user_key []byte = nil
	var current_user_key []byte = nil

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta fileMetaData
		meta.number = db.current.nextFileNumber
		db.current.nextFileNumber++
		file, err := NewLinuxFile(sstableFileName(db.option.DirPath, meta.number))
		if err != nil {
			return err
		}
		builder := newTableBuilder(&db.option, file)

		meta.smallest = iter.Key()
		for ; iter.Valid(); iter.Next() {
			internal_key := InternalKey(iter.Key())
			current_user_key = internal_key.ExtractUserKey()
			if prev_user_key != nil {
				res := UserKeyCompare(prev_user_key, current_user_key)
				if res == 0 {
					continue
				} else if res > 0 {
					return ErrInvalidKey
				}
			}
			prev_user_key = current_user_key
			meta.largest = internal_key
			builder.add(internal_key, iter.Value())
			if builder.fileSize() > uint64(db.option.MaxFileSize) {
				break
			}
		}
		builder.finish()
		meta.fileSize = builder.fileSize()
		list = append(list, &meta)
	}

	for i := 0; i < len(c.inputs[0]); i++ {
		if err := db.current.deleteFile(c.level, c.inputs[0][i], true); err != nil {
			return err
		}
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		if err = db.current.deleteFile(c.level+1, c.inputs[1][i], true); err != nil {
			return err
		}
	}
	for i := 0; i < len(list); i++ {
		db.current.addFile(c.level+1, list[i])
	}
	return nil
}

func (db *DB) makeInputIterator(c *compaction) (Iterator, error) {
	list := make([][]Iterator, 0)
	// first level
	if c.level == 0 {
		for i := 0; i < len(c.inputs[0]); i++ {
			table, err := db.cache.getTable(c.inputs[0][i].number)
			if err != nil {
				return nil, err
			}
			tmp := make([]Iterator, 0)
			tmp = append(tmp, newSSTableIterator(table))
			list = append(list, tmp)
		}
	} else {
		tmp := make([]Iterator, 0)
		for i := 0; i < len(c.inputs[0]); i++ {
			table, err := db.cache.getTable(c.inputs[0][i].number)
			if err != nil {
				return nil, err
			}
			tmp = append(tmp, newSSTableIterator(table))
		}
		list = append(list, tmp)
	}

	// second level
	tmp := make([]Iterator, 0)
	for i := 0; i < len(c.inputs[1]); i++ {
		table, err := db.cache.getTable(c.inputs[1][i].number)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, newSSTableIterator(table))
	}
	list = append(list, tmp)

	return newDeduplicationIterator(newMergeIterator(list)), nil
}
