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

	c := db.current.PickCompaction()
	if c == nil {
		// Nothing to do
	} else if c.IsTrivialMove() {
		db.current.DeleteFile(c.Level(), c.Input()[0][0])
		db.current.AddFile(c.Level()+1, c.Input()[0][0])
	} else {
		db.doCompaction(c)
	}
}

func (db *DB) compactMemTable() {
	if err := db.writeLevel0Table(db.imm, db.current); err != nil {
		panic("writeLevel0Table failed")
	}
	if err := RemoveFile(db.imm.GetLogPath()); err != nil {
		panic("remove log file failed")
	}
	db.imm = nil
}

func (db *DB) doCompaction(c *Compaction) {
	var list []*FileMetaData
	iter := db.makeInputIterator(c)
	var prev_user_key []byte = nil
	var current_user_key []byte = nil

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.Number = db.current.NextFileNumber
		db.current.NextFileNumber++
		file, err := NewLinuxFile(SSTableFileName(db.dbname, meta.Number))
		if err != nil {
			panic(err.Error())
		}
		builder := NewTableBuilder(&db.option, file)

		meta.Smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			internal_key := iter.InternalKey()
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
			meta.Largest = current_user_key
			builder.Add(internal_key, iter.Value())
			if builder.FileSize() > uint64(MaxFileSize) {
				break
			}
		}
		builder.Finish()
		meta.FileSize = builder.FileSize()
		list = append(list, &meta)
	}

	inputs := c.Input()
	for i := 0; i < len(inputs[0]); i++ {
		db.current.DeleteFile(c.Level(), inputs[0][i])
	}
	for i := 0; i < len(inputs[1]); i++ {
		db.current.DeleteFile(c.Level()+1, inputs[1][i])
	}
	for i := 0; i < len(list); i++ {
		db.current.AddFile(c.Level()+1, list[i])
	}
}

func (db *DB) makeInputIterator(c *Compaction) *MergeIterator {
	list := make([]*SSTableIterator, 0)
	inputs := c.Input()
	for i := 0; i < 2; i++ {
		for j := 0; j < len(inputs[i]); j++ {
			file, err := NewLinuxFile(SSTableFileName(db.dbname, inputs[i][j].Number))
			if err != nil {
				panic(err.Error())
			}
			table, err := OpenSSTable(file, uint64(file.Size()))
			if err != nil {
				panic(err.Error())
			}
			list = append(list, NewSSTableIterator(table))
		}
	}
	return NewMergeIterator(list)
}
