package version

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/huayichai/goleveldb/internal"
	"github.com/huayichai/goleveldb/log"
	"github.com/huayichai/goleveldb/sstable"
)

type FileMetaData struct {
	FileSize uint64 // File size in bytes
	Number   uint64 // file number
	Smallest []byte // Smallest internal key served by table
	Largest  []byte // Largest internal key served by table
}

func (meta *FileMetaData) EncodeTo() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], meta.FileSize)
	binary.LittleEndian.PutUint64(buf[8:16], meta.Number)
	buf = append(buf, internal.PutLengthPrefixedSlice(meta.Smallest)...)
	buf = append(buf, internal.PutLengthPrefixedSlice(meta.Largest)...)
	return buf
}

func (meta *FileMetaData) DecodeFrom(data []byte) uint32 {
	meta.FileSize = binary.LittleEndian.Uint64(data[0:8])
	meta.Number = binary.LittleEndian.Uint64(data[8:16])
	var n1, n2 uint32
	meta.Smallest, n1 = internal.GetLengthPrefixedSlice(data[16:])
	meta.Largest, n2 = internal.GetLengthPrefixedSlice(data[16+n1:])
	return 16 + n1 + n2
}

type Version struct {
	dbname         string
	NextFileNumber uint64
	LastSequence   uint64
	Files          [internal.NumLevels][]*FileMetaData
}

func NewVersion(dbname string) *Version {
	var version Version
	version.dbname = dbname
	version.NextFileNumber = 1
	version.LastSequence = 0
	return &version
}

func (v *Version) NumLevelFiles(l uint32) uint32 {
	return uint32(len(v.Files[l]))
}

func (v *Version) AddFile(level uint32, meta *FileMetaData) {
	if level == 0 {
		v.Files[level] = append(v.Files[level], meta)
	} else {

	}
}

func (v *Version) Get(key []byte) ([]byte, error) {
	var filemetas []*FileMetaData
	for level := 0; level < 1; level++ {
		numFiles := len(v.Files[level])
		if numFiles == 0 {
			continue
		}
		if level == 0 {
			for idx := 0; idx < numFiles; idx++ {
				meta := v.Files[level][idx]
				if internal.Compare(meta.Smallest, key) <= 0 && internal.Compare(meta.Largest, key) >= 0 {
					filemetas = append(filemetas, meta)
				}
			}
			if len(filemetas) == 0 {
				continue
			}
			sort.Slice(filemetas, func(i, j int) bool {
				return filemetas[i].Number > filemetas[j].Number
			})
		} else {

		}
		numFiles = len(filemetas)
		for idx := 0; idx < numFiles; idx++ {
			file, err := log.NewLinuxFile(internal.SSTableFileName(v.dbname, filemetas[idx].Number))
			if err != nil {
				return nil, err
			}
			defer file.Close()
			sstable, err := sstable.OpenSSTable(file, uint64(file.Size()))
			if err != nil {
				return nil, err
			}
			value, err := sstable.Get(key)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	}
	return nil, fmt.Errorf("%s", "Not Found")
}

func (v *Version) EncodeTo() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf, v.NextFileNumber)
	binary.LittleEndian.PutUint64(buf[8:], v.LastSequence)
	for level := 0; level < len(v.Files); level++ {
		level_size := len(v.Files[level])
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, uint32(level_size))
		buf = append(buf, tmp...)
		for idx := 0; idx < level_size; idx++ {
			buf = append(buf, v.Files[level][idx].EncodeTo()...)
		}
	}
	return buf
}

func (v *Version) DecodeFrom(data []byte) {
	v.NextFileNumber = binary.LittleEndian.Uint64(data)
	v.LastSequence = binary.LittleEndian.Uint64(data[8:])
	offset := uint32(16)
	size := uint32(len(data))
	for level := 0; offset < size; level++ {
		var metas []*FileMetaData
		level_size := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		for idx := 0; idx < int(level_size); idx++ {
			var meta FileMetaData
			n := meta.DecodeFrom(data[offset:])
			offset += n
			metas = append(metas, &meta)
		}
		v.Files[level] = metas
	}
}
