package sstable

import (
	"encoding/binary"
)

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

type Footer struct {
	MetaIndexHandle  BlockHandle
	IndexBlockHandle BlockHandle
}

const (
	KEncodedLength int = 32
)

func (footer *Footer) EncodeTo() string {
	p := make([]byte, KEncodedLength)
	binary.LittleEndian.PutUint64(p[0:8], footer.MetaIndexHandle.Offset)
	binary.LittleEndian.PutUint64(p[8:16], footer.MetaIndexHandle.Size)
	binary.LittleEndian.PutUint64(p[16:24], footer.IndexBlockHandle.Offset)
	binary.LittleEndian.PutUint64(p[24:32], footer.IndexBlockHandle.Size)
	return string(p)
}

func (footer *Footer) DecodeFrom(data []byte) {
	// buf := bytes.NewBuffer(data)
	// binary.Read(buf, binary.LittleEndian, &footer.MetaIndexHandle.Offset)
	// binary.Read(buf, binary.LittleEndian, &footer.MetaIndexHandle.Size)
	// binary.Read(buf, binary.LittleEndian, &footer.IndexBlockHandle.Offset)
	// binary.Read(buf, binary.LittleEndian, &footer.IndexBlockHandle.Size)
	footer.MetaIndexHandle.Offset = binary.LittleEndian.Uint64(data[0:8])
	footer.MetaIndexHandle.Size = binary.LittleEndian.Uint64(data[8:16])
	footer.IndexBlockHandle.Offset = binary.LittleEndian.Uint64(data[16:24])
	footer.IndexBlockHandle.Size = binary.LittleEndian.Uint64(data[24:32])
}
