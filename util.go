package raftmdb

import (
	"bytes"
	"encoding/binary"

	"github.com/hashicorp/go-msgpack/codec"
)

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(buf *bytes.Buffer, in interface{}) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	return enc.Encode(in)
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice and appends the result to b.
func uint64ToBytes(b []byte, u uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u)
	return append(b, buf[:]...)
}
