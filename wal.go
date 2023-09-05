package goleveldb

type recordType uint8

const (
	kFullType = 1

	// For fragments
	kFirstType  = 2
	kMiddleType = 3
	kLastType   = 4
)

const kBlockSize uint32 = 32 * 1024

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const kHeaderSize uint32 = 4 + 2 + 1

type walReader struct {
	file             RandomAccessFile
	lastRecordOffset uint64
}

func newWALReader(file RandomAccessFile) *walReader {
	return &walReader{
		file:             file,
		lastRecordOffset: 0,
	}
}

func (reader *walReader) readRecord() ([]byte, error) {
	return reader.readPhysicalRecord()
}

func (reader *walReader) readPhysicalRecord() ([]byte, error) {
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
			return nil, ErrByteCoding
		}
		a := uint32(header[4]) & 0xff
		b := uint32(header[5]) & 0xff
		record_type := recordType(header[6])
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

type walWriter struct {
	dest        WritableFile
	blockOffset uint32
}

func newWALWriter(dest WritableFile) *walWriter {
	return &walWriter{
		dest:        dest,
		blockOffset: 0,
	}
}

func (writer *walWriter) addRecord(data []byte) error {
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
		var record_type recordType
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

func (writer *walWriter) emitPhysicalRecord(t recordType, ptr []byte, length uint32) error {
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
	// writer.dest.Sync()
	writer.blockOffset += (kHeaderSize + length)
	return nil
}
