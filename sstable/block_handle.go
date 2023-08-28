package sstable

import "encoding/binary"

type BlockHandle struct {
	Offset uint64
	Size   uint64
}

func (handle *BlockHandle) EncodeTo() string {
	p := make([]byte, 16)
	binary.LittleEndian.PutUint64(p, handle.Offset)
	binary.LittleEndian.PutUint64(p[8:], handle.Size)
	return string(p)
}
