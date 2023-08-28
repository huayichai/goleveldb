package internal

import (
	"encoding/binary"
	"io"
)

type ValueType uint8

const (
	KTypeDeletion ValueType = 0x0
	KTypeValue
)

func EncodeTo(w io.Writer, data any) error {
	return binary.Write(w, binary.LittleEndian, data)
}
