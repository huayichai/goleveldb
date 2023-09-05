package goleveldb

import (
	"os"
)

type WritableFile interface {
	Append(data string) error
	Close() error
	Sync() error
}

type RandomAccessFile interface {
	Read(offset uint64, n uint32) ([]byte, error)
}

type LinuxFile struct {
	file *os.File
}

func NewLinuxFile(fileName string) (*LinuxFile, error) {
	var lf LinuxFile
	var err error
	lf.file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	return &lf, nil
}

func (lf *LinuxFile) Append(data string) error {
	_, err := lf.file.WriteString(data)
	if err != nil {
		return err
	}
	return nil
}

func (lf *LinuxFile) Close() error {
	return lf.file.Close()
}

func (lf *LinuxFile) Sync() error {
	return lf.file.Sync()
}

var _ WritableFile = (*LinuxFile)(nil)

func (lf *LinuxFile) Read(offset uint64, n uint32) ([]byte, error) {
	buf := make([]byte, n)
	_, err := lf.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	} else {
		return buf, nil
	}
}

var _ RandomAccessFile = (*LinuxFile)(nil)

func (lf *LinuxFile) Size() int64 {
	fi, _ := lf.file.Stat()
	return fi.Size()
}

func RemoveFile(path string) error {
	return os.Remove(path)
}
