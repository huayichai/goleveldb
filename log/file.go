package log

import (
	"fmt"
	"os"

	"github.com/huayichai/goleveldb/db"
)

type WritableFile interface {
	Append(data string) db.Status
	Close()
	Flush()
	Sync()
}

type LinuxFile struct {
	file *os.File
}

func NewLinuxFile(fileName string) (*LinuxFile, db.Status) {
	var lf LinuxFile
	var err error
	lf.file, err = os.Create(fileName)
	if err != nil {
		return nil, db.StatusIOError(fmt.Sprintf("create or open file %s failed", fileName))
	}
	return &lf, db.StatusOK()
}

func (lf *LinuxFile) Append(data string) db.Status {
	_, err := lf.file.WriteString(data)
	if err != nil {
		return db.StatusIOError(fmt.Sprintf("%s", err.Error()))
	}
	return db.StatusOK()
}

func (lf *LinuxFile) Close() {
	lf.file.Close()
}

func (lf *LinuxFile) Flush() {

}

func (lf *LinuxFile) Sync() {
	lf.file.Sync()
}

var _ WritableFile = (*LinuxFile)(nil)
