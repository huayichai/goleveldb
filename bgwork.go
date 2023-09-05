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
		db.compactMemTable()
		return
	}

	c := db.current.pickCompaction()
	if c == nil {
		// Nothing to do
	} else if c.isTrivialMove() {
		db.current.deleteFile(c.level, c.inputs[0][0])
		db.current.addFile(c.level+1, c.inputs[0][0])
	} else {
		db.doCompaction(c)
	}
}

func (db *DB) compactMemTable() {
	if err := db.writeLevel0Table(db.imm, db.current); err != nil {
		panic("writeLevel0Table failed")
	}
	if err := RemoveFile(db.imm.getLogPath()); err != nil {
		panic("remove log file failed")
	}
	db.imm = nil
}

func (db *DB) doCompaction(c *compaction) {
	var list []*fileMetaData
	iter := db.makeInputIterator(c)
	var prev_user_key []byte = nil
	var current_user_key []byte = nil

	for iter.seekToFirst(); iter.valid(); iter.next() {
		var meta fileMetaData
		meta.number = db.current.nextFileNumber
		db.current.nextFileNumber++
		file, err := NewLinuxFile(sstableFileName(db.dbname, meta.number))
		if err != nil {
			panic(err.Error())
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
					panic("internal key unsorted")
				}
			}
			prev_user_key = current_user_key
			meta.largest = current_user_key
			builder.add(internal_key, iter.value())
			if builder.fileSize() > uint64(MaxFileSize) {
				break
			}
		}
		builder.finish()
		meta.fileSize = builder.fileSize()
		list = append(list, &meta)
	}

	for i := 0; i < len(c.inputs[0]); i++ {
		db.current.deleteFile(c.level, c.inputs[0][i])
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		db.current.deleteFile(c.level, c.inputs[1][i])
	}
	for i := 0; i < len(list); i++ {
		db.current.addFile(c.level+1, list[i])
	}
}

func (db *DB) makeInputIterator(c *compaction) *mergeIterator {
	list := make([]*sstableIterator, 0)
	for i := 0; i < 2; i++ {
		for j := 0; j < len(c.inputs[i]); j++ {
			file, err := NewLinuxFile(sstableFileName(db.dbname, c.inputs[i][j].number))
			if err != nil {
				panic(err.Error())
			}
			table, err := openSSTable(file, uint64(file.Size()))
			if err != nil {
				panic(err.Error())
			}
			list = append(list, newSSTableIterator(table))
		}
	}
	return newMergeIterator(list)
}
