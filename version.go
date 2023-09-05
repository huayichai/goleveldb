package goleveldb

import (
	"encoding/binary"
	"fmt"
	"sort"
)

type FileMetaData struct {
	FileSize uint64      // File size in bytes
	Number   uint64      // file number
	Smallest InternalKey // Smallest internal key served by table
	Largest  InternalKey // Largest internal key served by table
}

func (meta *FileMetaData) EncodeTo() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], meta.FileSize)
	binary.LittleEndian.PutUint64(buf[8:16], meta.Number)
	buf = append(buf, PutLengthPrefixedSlice(meta.Smallest)...)
	buf = append(buf, PutLengthPrefixedSlice(meta.Largest)...)
	return buf
}

func (meta *FileMetaData) DecodeFrom(data []byte) uint32 {
	meta.FileSize = binary.LittleEndian.Uint64(data[0:8])
	meta.Number = binary.LittleEndian.Uint64(data[8:16])
	var n1, n2 uint32
	meta.Smallest, n1 = GetLengthPrefixedSlice(data[16:])
	meta.Largest, n2 = GetLengthPrefixedSlice(data[16+n1:])
	return 16 + n1 + n2
}

type Version struct {
	dbname         string
	NextFileNumber uint64
	LastSequence   SequenceNumber
	Files          [NumLevels][]*FileMetaData

	compactPointer [NumLevels]InternalKey
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

func (v *Version) AddFile(level int, meta *FileMetaData) {
	if level == 0 {
		v.Files[level] = append(v.Files[level], meta)
	} else {
		numFiles := len(v.Files[level])
		index := v.findFile(v.Files[level], meta.Smallest.ExtractUserKey())
		if index >= numFiles {
			v.Files[level] = append(v.Files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, v.Files[level][:index]...)
			tmp = append(tmp, meta)
			v.Files[level] = append(tmp, v.Files[level][index:]...)
		}
	}
}

func (v *Version) DeleteFile(level int, meta *FileMetaData) {
	numFiles := len(v.Files[level])
	for i := 0; i < numFiles; i++ {
		if v.Files[level][i].Number == meta.Number {
			v.Files[level] = append(v.Files[level][:i], v.Files[level][i+1:]...)
			break
		}
	}
}

func (v *Version) Get(internal_key InternalKey) ([]byte, error) {
	var filemetas []*FileMetaData
	user_key := internal_key.ExtractUserKey()
	for level := 0; level < int(NumLevels); level++ {
		filemetas = []*FileMetaData{}
		numFiles := len(v.Files[level])
		if numFiles == 0 {
			continue
		}
		if level == 0 {
			for idx := 0; idx < numFiles; idx++ {
				meta := v.Files[level][idx]
				if UserKeyCompare(meta.Smallest.ExtractUserKey(), user_key) <= 0 && UserKeyCompare(meta.Largest.ExtractUserKey(), user_key) >= 0 {
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
			index := v.findFile(v.Files[level], user_key)
			if index >= numFiles {
				filemetas = nil
			} else {
				if UserKeyCompare(user_key, v.Files[level][index].Smallest.ExtractUserKey()) < 0 {
					filemetas = nil
				} else {
					filemetas = append(filemetas, v.Files[level][index])
				}
			}
		}
		numFiles = len(filemetas)
		for idx := 0; idx < numFiles; idx++ {
			file, err := NewLinuxFile(SSTableFileName(v.dbname, filemetas[idx].Number))
			if err != nil {
				return nil, err
			}
			defer file.Close()
			sstable, err := OpenSSTable(file, uint64(file.Size()))
			if err != nil {
				return nil, err
			}
			value, err := sstable.Get(internal_key)
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
	binary.LittleEndian.PutUint64(buf[8:], uint64(v.LastSequence))
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
	v.LastSequence = SequenceNumber(binary.LittleEndian.Uint64(data[8:]))
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

// Find the first file which largest key >= userkey
func (v *Version) findFile(metas []*FileMetaData, user_key UserKey) int {
	left := 0
	right := len(metas)
	for left < right {
		mid := (left + right) / 2
		f := metas[mid]
		if UserKeyCompare(f.Largest.ExtractUserKey(), user_key) < 0 {
			// Key at "mid.largest" is < "target".  Therefore all
			// files at or before "mid" are uninteresting.
			left = mid + 1
		} else {
			// Key at "mid.largest" is >= "target".  Therefore all files
			// after "mid" are uninteresting.
			right = mid
		}
	}
	return right
}
