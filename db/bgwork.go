package db

import "github.com/huayichai/goleveldb/log"

func (db *DB) backgroundCompaction() {
	if db.imm == nil {
		return
	}
	if err := db.writeLevel0Table(db.imm, db.current); err != nil {
		panic("writeLevel0Table failed")
	}
	if err := log.RemoveFile(db.imm.GetLogPath()); err != nil {
		panic("remove log file failed")
	}
	db.mu.Lock()
	db.imm = nil
	db.mu.Unlock()
	db.cond.Broadcast()
}
