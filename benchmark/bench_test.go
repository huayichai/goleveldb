package benchmark

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/huayichai/goleveldb"
)

var db *goleveldb.DB

func openDB() func() {
	options := goleveldb.DefaultOptions()
	options.DirPath = "/tmp/golevel-bench"

	os.RemoveAll(options.DirPath)

	var err error
	db, err = goleveldb.Open(*options)
	if err != nil {
		panic(err)
	}
	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func TestRandomPut(t *testing.T) {
	destroy := openDB()
	defer destroy()

	putNum := 100000
	raw_data_size := float64(putNum*(16+110)) / float64(1024*1024) // MB

	startTime := time.Now()
	for i := 0; i < putNum; i++ {
		err := db.Put(GetTestKey(i), RandomValue(100))
		if err != nil {
			panic(err)
		}
	}
	elapsedTime := time.Since(startTime) / time.Millisecond // ms

	throughput := int64(float64(putNum) / float64(elapsedTime) * 1000.0) // QPS
	latency := float64(elapsedTime*1000) / float64(putNum)
	write_speed := raw_data_size * 1000.0 / float64(elapsedTime)
	tmp, _ := db.SpaceConsumption()
	real_data_size := float64(tmp) / float64(1024*1024) // MB

	fmt.Printf("Benchmark Entries: %d\n", putNum)
	fmt.Printf("Throughput: %d QPS\n", throughput)
	fmt.Printf("Latency: %.3f micros/op; %.1f MB/s\n", latency, write_speed)
	fmt.Printf("Spatial amplification: %.3f, real data size: %.3f (MB), raw data size: %.3f (MB)\n",
		float64(real_data_size)/float64(raw_data_size), real_data_size, raw_data_size)
}
