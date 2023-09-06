package goleveldb

func (db *DB) maybeScheduleCompaction() {
	if db.backgroundCompactionScheduled {
		return
	}
	db.backgroundCompactionScheduled = true
	go db.backgroundCall()
}

func (db *DB) backgroundCall() {
	db.backgroundCompaction()

	db.backgroundCompactionScheduled = false

	db.backgroundWorkFinishedSignal.Broadcast()
}

func (db *DB) backgroundCompaction() {
	if db.imm != nil {
		if err := db.compactMemTable(); err != nil {
			panic(err.Error())
		}
		return
	}

	c := db.current.pickCompaction()
	if c == nil {
		// Nothing to do
	} else if c.isTrivialMove() {
		db.current.deleteFile(c.level, c.inputs[0][0], false)
		db.current.addFile(c.level+1, c.inputs[0][0])
	} else {
		if err := db.doCompaction(c); err != nil {
			panic(err.Error())
		}
	}
}

func (db *DB) compactMemTable() error {
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

func (db *DB) doCompaction(c *compaction) error {
	var list []*fileMetaData
	iter, err := db.makeInputIterator(c)
	if err != nil {
		return err
	}
	var prev_user_key []byte = nil
	var current_user_key []byte = nil

	for iter.seekToFirst(); iter.valid(); iter.next() {
		var meta fileMetaData
		meta.number = db.current.nextFileNumber
		db.current.nextFileNumber++
		file, err := NewLinuxFile(sstableFileName(db.option.DirPath, meta.number))
		if err != nil {
			return err
		}
		builder := newTableBuilder(&db.option, file)

		meta.smallest = iter.internalKey()
		for ; iter.valid(); iter.next() {
			internal_key := iter.internalKey()
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
			meta.largest = current_user_key
			builder.add(internal_key, iter.value())
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

func (db *DB) makeInputIterator(c *compaction) (*mergeIterator, error) {
	list := make([]*sstableIterator, 0)
	for i := 0; i < 2; i++ {
		for j := 0; j < len(c.inputs[i]); j++ {
			table, err := db.cache.getTable(c.inputs[i][j].number)
			defer table.close()
			if err != nil {
				return nil, err
			}
			list = append(list, newSSTableIterator(table))
		}
	}
	return newMergeIterator(list), nil
}
