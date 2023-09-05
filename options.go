package goleveldb

type Options struct {
	// If true, the database will be created if it is missing.
	Create_if_missing bool

	// -------------------
	// Parameters that affect performance

	// Amount of data to build up in memory (backed by an unsorted log
	// on disk) before converting to a sorted on-disk file.
	//
	// Larger values increase performance, especially during bulk loads.
	// Up to two write buffers may be held in memory at the same time,
	// so you may wish to adjust this parameter to control memory usage.
	// Also, a larger write buffer will result in a longer recovery time
	// the next time the database is opened.
	Write_buffer_size uint32

	BlockSize uint32
}

func NewOptions() *Options {
	var option Options
	option.Create_if_missing = false
	option.Write_buffer_size = 4 * 1024 * 1024
	option.BlockSize = 4 * 1024
	return &option
}
