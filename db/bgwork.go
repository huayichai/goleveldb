package db

func (db *DB) backgroundCompaction() {
	// if db.imm == nil {
	// 	return
	// }
	err := db.writeLevel0Table(db.imm, db.current)
	if err != nil {
		panic("writeLevel0Table failed")
	}
	db.imm = nil
	db.cond.Broadcast()
}
