package goleveldb

import (
	"encoding/binary"
	"fmt"
	"log"
	"sort"
)

type fileMetaData struct {
	fileSize uint64      // File size in bytes
	number   uint64      // file number
	smallest InternalKey // Smallest internal key served by table
	largest  InternalKey // Largest internal key served by table
}

func (meta *fileMetaData) encodeTo() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], meta.fileSize)
	binary.LittleEndian.PutUint64(buf[8:16], meta.number)
	buf = append(buf, PutLengthPrefixedSlice(meta.smallest)...)
	buf = append(buf, PutLengthPrefixedSlice(meta.largest)...)
	return buf
}

func (meta *fileMetaData) decodeFrom(data []byte) uint32 {
	meta.fileSize = binary.LittleEndian.Uint64(data[0:8])
	meta.number = binary.LittleEndian.Uint64(data[8:16])
	var n1, n2 uint32
	meta.smallest, n1 = GetLengthPrefixedSlice(data[16:])
	meta.largest, n2 = GetLengthPrefixedSlice(data[16+n1:])
	return 16 + n1 + n2
}

type version struct {
	cache *tableCache

	nextFileNumber uint64
	lastSequence   SequenceNumber
	files          [NumLevels][]*fileMetaData

	compactPointer [NumLevels]InternalKey
}

func newVersion(cache *tableCache) *version {
	var version version
	version.cache = cache
	version.nextFileNumber = 1
	version.lastSequence = 0
	return &version
}

func (v *version) numLevelFiles(l uint32) uint32 {
	return uint32(len(v.files[l]))
}

func (v *version) addFile(level int, meta *fileMetaData) {
	if level == 0 {
		v.files[level] = append(v.files[level], meta)
	} else {
		numfiles := len(v.files[level])
		index := v.findFile(v.files[level], meta.smallest.ExtractUserKey())
		if index >= numfiles {
			v.files[level] = append(v.files[level], meta)
		} else {
			var tmp []*fileMetaData
			tmp = append(tmp, v.files[level][:index]...)
			tmp = append(tmp, meta)
			v.files[level] = append(tmp, v.files[level][index:]...)
		}
	}
	if Debug {
		log.Printf("doCompaction add level %d file %d [smallest %s, largest %s]\n", level, meta.number, meta.smallest.ExtractUserKey(), meta.largest.ExtractUserKey())
	}
}

// deleteFile remove FileMetaData from version.files[level]
// If 'disk' is set to true, the file corresponding to 'meta' will be deleted from disk,
// and evicted from cache.
// Specifically, when compaction is trivial move, 'disk' should be set to false.
func (v *version) deleteFile(level int, meta *fileMetaData, disk bool) error {
	if Debug {
		log.Printf("doCompaction remove level %d file %d [smallest %s, largest %s]\n", level, meta.number, meta.smallest.ExtractUserKey(), meta.largest.ExtractUserKey())
	}
	numfiles := len(v.files[level])
	for i := 0; i < numfiles; i++ {
		if v.files[level][i].number == meta.number {
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...)
			break
		}
	}
	if disk {
		v.cache.evict(meta.number)
		return RemoveFile(sstableFileName(v.cache.option.DirPath, meta.number))
	}
	return nil
}

func (v *version) get(internal_key InternalKey) ([]byte, error) {
	var filemetas []*fileMetaData
	user_key := internal_key.ExtractUserKey()
	for level := 0; level < int(NumLevels); level++ {
		filemetas = []*fileMetaData{}
		numfiles := len(v.files[level])
		if numfiles == 0 {
			continue
		}
		if level == 0 {
			for idx := 0; idx < numfiles; idx++ {
				meta := v.files[level][idx]
				if UserKeyCompare(meta.smallest.ExtractUserKey(), user_key) <= 0 && UserKeyCompare(meta.largest.ExtractUserKey(), user_key) >= 0 {
					filemetas = append(filemetas, meta)
				}
			}
			if len(filemetas) == 0 {
				continue
			}
			sort.Slice(filemetas, func(i, j int) bool {
				return filemetas[i].number > filemetas[j].number
			})
		} else {
			index := v.findFile(v.files[level], user_key)
			if index >= numfiles {
				filemetas = nil
			} else {
				if UserKeyCompare(user_key, v.files[level][index].smallest.ExtractUserKey()) < 0 {
					filemetas = nil
				} else {
					filemetas = append(filemetas, v.files[level][index])
				}
			}
		}
		numfiles = len(filemetas)
		for idx := 0; idx < numfiles; idx++ {
			value, err := v.cache.get(filemetas[idx].number, internal_key)
			if err == nil {
				return value, nil
			} else if err == errKeyDeleted {
				return nil, ErrKeyNotFound
			} else if err == ErrKeyNotFound {
				continue
			}
			return nil, err
		}
	}
	return nil, ErrKeyNotFound
}

func (v *version) encodeTo() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf, v.nextFileNumber)
	binary.LittleEndian.PutUint64(buf[8:], uint64(v.lastSequence))
	for level := 0; level < len(v.files); level++ {
		level_size := len(v.files[level])
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, uint32(level_size))
		buf = append(buf, tmp...)
		for idx := 0; idx < level_size; idx++ {
			buf = append(buf, v.files[level][idx].encodeTo()...)
		}
	}
	return buf
}

func (v *version) decodeFrom(data []byte) {
	v.nextFileNumber = binary.LittleEndian.Uint64(data)
	v.lastSequence = SequenceNumber(binary.LittleEndian.Uint64(data[8:]))
	offset := uint32(16)
	size := uint32(len(data))
	for level := 0; offset < size; level++ {
		var metas []*fileMetaData
		level_size := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		for idx := 0; idx < int(level_size); idx++ {
			var meta fileMetaData
			n := meta.decodeFrom(data[offset:])
			offset += n
			metas = append(metas, &meta)
		}
		v.files[level] = metas
	}
}

// Find the first file which largest key >= userkey
func (v *version) findFile(metas []*fileMetaData, user_key UserKey) int {
	left := 0
	right := len(metas)
	for left < right {
		mid := (left + right) / 2
		f := metas[mid]
		if UserKeyCompare(f.largest.ExtractUserKey(), user_key) < 0 {
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

func (v *version) info() {
	for i := 0; i < len(v.files); i++ {
		n := len(v.files[i])
		if n == 0 {
			break
		}
		fmt.Println("==========")
		fmt.Printf("Level %d:\n", i)
		for j := 0; j < n; j++ {
			meta := v.files[i][j]
			fmt.Printf("file %d [smallest %s, largest %s]\n", meta.number, meta.smallest.ExtractUserKey(), meta.largest.ExtractUserKey())
		}
	}
}
