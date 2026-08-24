package storage

import (
	"encoding/binary"
	"hash/crc32"
)

// OpType identifies the kind of a WAL record.
type OpType byte

const (
	OpCreateQueue OpType = iota + 1
	OpEnqueue
	OpAck
)

// Record is one logical WAL entry as returned by Replay.
type Record struct {
	Op      OpType
	Payload []byte
}

// On-disk framing:
//
//	magic    4 bytes   "FQW1"
//	version  1 byte
//	op       1 byte
//	length   4 bytes   uint32 BE, payload length
//	payload  N bytes
//	checksum 4 bytes   uint32 BE, CRC32 IEEE over version+op+length+payload
//
// See DESIGN_DECISIONS.md #11/#12 and STEP_BY_STEP_PROMPTS.md Phase 3 for
// the rationale behind the framing and the recovery policy that consumes it.
var magic = [4]byte{'F', 'Q', 'W', '1'}

const currentVersion byte = 1

const (
	headerSize = 4 + 1 + 1 + 4 // magic + version + op + length
	footerSize = 4             // checksum
)

// MaxPayloadBytes bounds a record's payload so a corrupted or malicious
// on-disk length field can never trigger an unbounded allocation.
const MaxPayloadBytes = 1 << 20 // 1 MiB

// encodeRecord builds the full on-disk byte sequence for one record.
func encodeRecord(op OpType, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload)+footerSize)

	copy(buf[0:4], magic[:])
	buf[4] = currentVersion
	buf[5] = byte(op)
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(payload)))
	copy(buf[headerSize:headerSize+len(payload)], payload)

	sum := checksum(currentVersion, op, uint32(len(payload)), payload)
	binary.BigEndian.PutUint32(buf[headerSize+len(payload):], sum)

	return buf
}

// checksum computes the CRC32 IEEE checksum over version+op+length+payload.
func checksum(version byte, op OpType, length uint32, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte{version, byte(op)})
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], length)
	h.Write(lenBuf[:])
	h.Write(payload)
	return h.Sum32()
}

// decodedHeader is a parsed, not-yet-validated record header.
type decodedHeader struct {
	version byte
	op      OpType
	length  uint32
}

func decodeHeader(buf []byte) decodedHeader {
	return decodedHeader{
		version: buf[4],
		op:      OpType(buf[5]),
		length:  binary.BigEndian.Uint32(buf[6:10]),
	}
}
