package goleveldb

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	errKeyDeleted  = errors.New("key has been deleted")
	ErrInvalidKey  = errors.New("key is invalid")
	ErrByteCoding  = errors.New("coding exception")
)
