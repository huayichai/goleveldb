package goleveldb

type LogWriter struct {
	dest        WritableFile
	blockOffset uint32
}

func NewLogWriter(dest WritableFile) *LogWriter {
	return &LogWriter{
		dest:        dest,
		blockOffset: 0,
	}
}

func (writer *LogWriter) AddRecord(data []byte) error {
	ptr := data
	left := len(ptr)
	begin := true
	for {
		leftover := int(kBlockSize) - int(writer.blockOffset)
		// Switch to a new block
		if leftover < int(kHeaderSize) {
			if leftover > 0 {
				// Fill the trailer (literal below relies on kHeaderSize being 7)
				p := []byte("\x00\x00\x00\x00\x00\x00")[0:leftover]
				writer.dest.Append(string(p))
			}
			writer.blockOffset = 0
		}
		avail := kBlockSize - writer.blockOffset - kHeaderSize
		var fragment_length uint32
		if left < int(avail) {
			fragment_length = uint32(left)
		} else {
			fragment_length = avail
		}
		var record_type RecordType
		end := (fragment_length == uint32(left))
		if begin && end {
			record_type = kFullType
		} else if begin {
			record_type = kFirstType
		} else if end {
			record_type = kLastType
		} else {
			record_type = kMiddleType
		}

		err := writer.emitPhysicalRecord(record_type, ptr, fragment_length)
		if err != nil {
			return err
		}

		ptr = ptr[fragment_length:]
		left -= int(fragment_length)
		begin = false
		if left <= 0 {
			break
		}
	}
	return nil
}

func (writer *LogWriter) emitPhysicalRecord(t RecordType, ptr []byte, length uint32) error {
	// Format the header
	header := make([]byte, 4)
	header = append(header, byte(length&0xff))
	header = append(header, byte(length>>8))
	header = append(header, byte(t))

	// Compute the crc of the record type and the payload.
	// not implement
	EncodeFixed32(header, 0) // crc32

	// Write the header and the payload
	err := writer.dest.Append(string(header))
	if err != nil {
		return err
	}
	err = writer.dest.Append(string(ptr[:length]))
	if err != nil {
		return err
	}
	writer.dest.Flush()
	writer.blockOffset += (kHeaderSize + length)
	return nil
}
