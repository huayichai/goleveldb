package db

type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target interface{})
	Next()
	Prev()
	Key() []byte
	Value() []byte
}
