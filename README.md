# goleveldb

goleveldb is a simple implementation version of leveldb, which is used for learning purposes.

Now support: 
- [x] Put
- [x] Get
- [ ] Delete
- [x] Compaction
- [ ] Iterator
- [ ] BatchWrite
- [ ] Concurrency
- [ ] Data Compression

## Getting Started
From your project, run the following command, this will retrieve the library.
```bash
go get github.com/huayichai/goleveldb
```

Following is an example.
```go
package main

import "github.com/huayichai/goleveldb"

func main() {
	// Set Options
	options := goleveldb.DefaultOptions()
	options.DirPath = "/tmp/goleveldb_basic"

	// Open goleveldb
	db, err := goleveldb.Open(*options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}()

	// Put Key-Value
	key := []byte("KV store engine")
	value := []byte("goleveldb")
	err = db.Put(key, value)
	if err != nil {
		panic(err)
	}

	// Get Key-Value
	value, err = db.Get(key)
	if err != nil {
		panic(err)
	}
	println(string(value))
}
```


## Performance

We put a million entries into db. Each entry has a 16 byte key, and a 110 byte value. 

```
Benchmark Entries: 1000000
Throughput: 12144 QPS
Latency: 82.341 micros/op; 1.5 MB/s
```