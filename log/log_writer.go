package log

import "github.com/huayichai/goleveldb/db"

type LogWriter struct {
	dest WritableFile
}

func NewLogWriter(dest WritableFile) *LogWriter {
	return &LogWriter{
		dest: dest,
	}
}

func (writer *LogWriter) AddRecord(data string) db.Status {
	s := writer.dest.Append(data)
	if s.OK() {
		writer.dest.Sync()
	}
	return s
}
