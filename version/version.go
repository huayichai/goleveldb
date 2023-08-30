package version

import (
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
