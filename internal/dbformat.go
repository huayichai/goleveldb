package internal

import "bytes"

const (
	NumLevels uint32 = 7

	// Level-0 compaction is started when we hit this many files.
	L0_SlowdownWritesTrigger uint32 = 8

	// Maximum number of level-0 files.  We stop writes at this point.
	L0_StopWritesTrigger uint32 = 12
)

type ValueType uint8

const (
	KTypeDeletion ValueType = 0x0
	KTypeValue    ValueType = 0x1
)

type SequenceNumber uint64

func UserKeyCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// | user_key | (sequence_number + type)(8B) |
type InternalKey []byte

func NewInternalKey(userKey []byte, s SequenceNumber, t ValueType) InternalKey {
	p := make([]byte, 8)
	EncodeFixed64(p, PackSequenceAndType(s, t))
	userKey = append(userKey, p...)
	return userKey
}

func InternalKeyCompare(a, b InternalKey) int {
	r := UserKeyCompare(a.ExtractUserKey(), b.ExtractUserKey())
	if r == 0 {
		aseq := a.ExtractSequenceNumber()
		bseq := b.ExtractSequenceNumber()
		if aseq > bseq {
			r = -1
		} else if aseq < bseq {
			r = +1
		}
	}
	return r
}

func PackSequenceAndType(seq SequenceNumber, t ValueType) uint64 {
	return uint64(seq<<8) | uint64(t)
}

func (ik InternalKey) ExtractUserKey() []byte {
	return ik[0 : len(ik)-8]
}

func (ik InternalKey) ExtractSequenceNumber() SequenceNumber {
	tag := DecodeFixed64(ik[len(ik)-8:])
	return SequenceNumber(tag >> 8)
}

func (ik InternalKey) ExtractValueType() ValueType {
	tag := DecodeFixed64(ik[len(ik)-8:])
	t := tag & 0xFF
	return ValueType(t)
}

// | user_key_size(4B) | user_key | (sequence_number + type)(8B) |
type LookupKey []byte

func NewLookupKey(userKey []byte, s SequenceNumber) LookupKey {
	usize := uint32(len(userKey))
	needed := 4 + usize + 8
	dst := make([]byte, needed)
	EncodeFixed32(dst, usize)
	copy(dst[4:], userKey)
	EncodeFixed64(dst[4+usize:], PackSequenceAndType(s, KTypeValue))
	return dst
}

func (lk LookupKey) ExtractInternalKey() InternalKey {
	user_key_size := DecodeFixed32(lk)
	return InternalKey(lk[4 : 4+user_key_size+8])
}

func (lk LookupKey) ExtractUserKey() []byte {
	user_key_size := DecodeFixed32(lk)
	return lk[4 : 4+user_key_size]
}

func LookupKeyCompare(a, b LookupKey) int {
	return InternalKeyCompare(a.ExtractInternalKey(), b.ExtractInternalKey())
}

// | user_key_size(4B) | user_key | (sequence_number + type)(8B) | value_size(4B) | value |
type MemTableKey []byte

func NewMemTableKey(seq SequenceNumber, valueType ValueType, userKey, value []byte) MemTableKey {
	// Format of an entry is concatenation of:
	//  key_size     : uint32 of user_key.size()
	//  key bytes    : byte[user_key.size()]
	//  tag          : uint64((sequence << 8) | type)
	//  value_size   : uint32 of value.size()
	//  value bytes  : byte[value.size()]
	user_key_size := uint32(len(userKey))
	val_size := uint32(len(value))
	encode_len := uint32(4 + user_key_size + 8 + 4 + val_size)
	p := make([]byte, encode_len)
	offset := 0

	// encode key_size
	EncodeFixed32(p, user_key_size)
	offset += 4

	// encode internal_key
	copy(p[offset:], userKey) // key
	offset += int(user_key_size)
	EncodeFixed64(p[offset:], PackSequenceAndType(seq, KTypeValue)) // tag
	offset += 8

	// encode value_size
	EncodeFixed32(p[offset:], val_size)
	offset += 4

	// encode value
	copy(p[offset:], value)

	return MemTableKey(p)
}

func (memkey MemTableKey) ExtractInternalKey() InternalKey {
	user_key_size := DecodeFixed32(memkey)
	internal_key_encode_len := 4 + user_key_size + 8
	return InternalKey(memkey[4:internal_key_encode_len])
}

func (memkey MemTableKey) ExtractValue() []byte {
	user_key_size := DecodeFixed32(memkey)
	offset := 4 + user_key_size + 8
	value_size := DecodeFixed32(memkey[offset:])
	offset += 4
	return memkey[offset : offset+value_size]
}

func MemTableKeyCompare(a, b MemTableKey) int {
	return InternalKeyCompare(a.ExtractInternalKey(), b.ExtractInternalKey())
}