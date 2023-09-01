package wal

import (
	"fmt"

	"github.com/huayichai/goleveldb/log"
)

type LogReader struct {
	file             log.RandomAccessFile
	lastRecordOffset uint64
}

func NewLogReader(file log.RandomAccessFile) *LogReader {
	return &LogReader{
		file:             file,
		lastRecordOffset: 0,
	}
}

func (reader *LogReader) ReadRecord() ([]byte, error) {
	return reader.readPhysicalRecord()
}

func (reader *LogReader) readPhysicalRecord() ([]byte, error) {
	record := make([]byte, 0)
	for {
		current_block_left := uint64(kBlockSize) - (reader.lastRecordOffset % uint64(kBlockSize))
		if current_block_left < 7 {
			reader.lastRecordOffset += current_block_left
		}
		header, err := reader.file.Read(reader.lastRecordOffset, 7)
		if err != nil {
			return nil, err
		}
		if len(header) != 7 {
			return nil, fmt.Errorf("%s", "Incomplete record")
		}
		a := uint32(header[4]) & 0xff
		b := uint32(header[5]) & 0xff
		record_type := RecordType(header[6])
		length := (a | (b << 8))

		content, err := reader.file.Read(reader.lastRecordOffset+7, length)
		if err != nil {
			return nil, err
		}
		reader.lastRecordOffset += uint64(7 + len(content))
		if record_type == kFullType {
			return content, nil
		}
		record = append(record, content...)
		if record_type == kLastType {
			return record, nil
		}
	}
}
