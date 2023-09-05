package goleveldb

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidKey  = errors.New("key is invalid")
	ErrByteCoding  = errors.New("coding exception")
)
