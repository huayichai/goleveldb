package log

type LogWriter struct {
	dest WritableFile
}

func NewLogWriter(dest WritableFile) *LogWriter {
	return &LogWriter{
		dest: dest,
	}
}

func (writer *LogWriter) AddRecord(data string) error {
	err := writer.dest.Append(data)
	if err != nil {
		writer.dest.Sync()
	}
	return nil
}
