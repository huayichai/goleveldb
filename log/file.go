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

type RandomAccessFile interface {
	Read(offset uint64, n uint32) ([]byte, db.Status)
}

type LinuxFile struct {
	file *os.File
}

func NewLinuxFile(fileName string) (*LinuxFile, db.Status) {
	var lf LinuxFile
	var err error
	lf.file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, db.StatusIOError(fmt.Sprintf("create or open file %s failed", fileName))
	}
	return &lf, db.StatusOK()
}

func (lf *LinuxFile) Append(data string) db.Status {
	_, err := lf.file.WriteString(data)
	if err != nil {
		return db.StatusIOError(err.Error())
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

func (lf *LinuxFile) Read(offset uint64, n uint32) ([]byte, db.Status) {
	buf := make([]byte, n)
	_, err := lf.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, db.StatusIOError(err.Error())
	} else {
		return buf, db.StatusOK()
	}
}

var _ RandomAccessFile = (*LinuxFile)(nil)

func (lf *LinuxFile) Size() int64 {
	fi, _ := lf.file.Stat()
	return fi.Size()
}
