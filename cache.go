package goleveldb

import lru "github.com/hashicorp/golang-lru"

// cache sstable in memory
type tableCache struct {
	option *Options
	cache  *lru.Cache
}

func newTableCache(option *Options) (*tableCache, error) {
	var tc tableCache
	var err error
	tc.option = option
	tc.cache, err = lru.New(int(option.MaxOpenFiles))
	if err != nil {
		return nil, err
	}
	return &tc, nil
}

func (tc *tableCache) get(fileNumber uint64, key InternalKey) ([]byte, error) {
	table, err := tc.getTable(fileNumber)
	if err != nil {
		return nil, err
	}
	return table.get(key)
}

func (tc *tableCache) getTable(fileNumber uint64) (*sstable, error) {
	table, ok := tc.cache.Get(fileNumber)
	if ok {
		return table.(*sstable), nil
	} else {
		table, err := openSSTable(sstableFileName(tc.option.DirPath, fileNumber))
		if err != nil {
			return nil, err
		}
		tc.cache.Add(fileNumber, table)
		return table, nil
	}
}
