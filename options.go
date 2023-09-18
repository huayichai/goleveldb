package goleveldb

type Options struct {
	// DirPath specifies the directory path where all the database files will be stored.
	DirPath string

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemTableSize uint32

	// BlockSize represents the threshold size of data block in sstable.
	// For every data block, largest key will be inserted into index block.
	// So, small blocksize means more index entry, and lower point query latency.
	// Default value is 4KB
	BlockSize uint32

	// MaxFileSize represents the threshold size of sstable file.
	// When process goleveldb.doCompaction(), any sstable which size exceed MaxFileSize will be write to disk immedliately,
	// and a new sstable building process begin.
	// Default value is 128MB
	MaxFileSize uint32

	// MaxOpenFiles is the lru cache capacity.
	// Default value is 2GB / MaxFileSize
	MaxOpenFiles uint32

	// CompactionInterval indicates the time interval for periodic comparison in the background.
	// Unit is MilliSecond. Default value is 1000ms
	CompactionInterval uint32

	// BlockRestartInterval, the number of keys between restart points for delta encoding of keys.
	// This parameter can be changed dynamically.  Most clients should leave this parameter alone.
	// Default value if 16
	BlockRestartInterval uint32
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs with the WithX methods.
func DefaultOptions() *Options {
	var option Options
	option.DirPath = "/goleveldb_tempdb"
	option.Sync = false

	option.MemTableSize = 64 * MB
	option.BlockSize = 4 * KB
	option.MaxFileSize = 128 * MB
	option.MaxOpenFiles = 2 * GB / option.MaxFileSize

	option.CompactionInterval = 1000
	option.BlockRestartInterval = 16
	return &option
}
