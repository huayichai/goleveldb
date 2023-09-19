package goleveldb

type Iterator interface {
	Valid() bool
	SeekToFirst()
	Seek(target interface{})
	Next()
	Key() []byte
	Value() []byte
}
