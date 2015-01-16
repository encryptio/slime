package chunk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io/ioutil"
)

var (
	ErrChunkTooShort    = errors.New("chunk data too short")
	ErrChunkBadMagic    = errors.New("chunk data has bad magic number")
	ErrUnknownVersion   = errors.New("chunk data is of unknown version")
	ErrChunkBadChecksum = errors.New("chunk has bad data checksum")
)

var chunkMagicNumber = []byte("SLCK")
var chunkVersion0000 = []byte("0000")
var chunkVersion0001 = []byte("0001")

type FileInfo struct {
	SHA256       [32]byte
	FullLength   uint32
	DataChunks   uint32
	ParityChunks uint32
	MappingValue uint32
	WriteTime    int64
}

type Chunk struct {
	FileInfo

	ChunkIndex uint32
	Data       []byte
}

func (c *Chunk) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.Write(c.SHA256[:])
	binary.Write(buf, binary.BigEndian, c.FullLength)
	binary.Write(buf, binary.BigEndian, c.DataChunks)
	binary.Write(buf, binary.BigEndian, c.ParityChunks)
	binary.Write(buf, binary.BigEndian, c.MappingValue)
	binary.Write(buf, binary.BigEndian, c.WriteTime)

	binary.Write(buf, binary.BigEndian, c.ChunkIndex)
	buf.Write(c.Data)

	data := buf.Bytes()

	h := fnv.New64a()
	h.Write(data)
	fnvSum := h.Sum(nil)

	header := make([]byte, 16)
	copy(header[0:4], chunkMagicNumber)
	copy(header[4:8], chunkVersion0001)
	copy(header[8:16], fnvSum)

	return append(header, data...), nil
}

func (c *Chunk) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return ErrChunkTooShort
	}

	if !bytes.Equal(data[0:4], chunkMagicNumber) {
		return ErrChunkBadMagic
	}

	version := data[4:8]

	hash := data[8:16]
	data = data[16:]

	h := fnv.New64a()
	h.Write(data)
	if !bytes.Equal(hash, h.Sum(nil)) {
		return ErrChunkBadChecksum
	}

	switch {
	case bytes.Equal(version, chunkVersion0000):
		return c.unmarshalVersion0000(data)
	case bytes.Equal(version, chunkVersion0001):
		return c.unmarshalVersion0001(data)
	default:
		return ErrUnknownVersion
	}
}

func (c *Chunk) unmarshalVersion0000(data []byte) error {
	if len(data) < 32+4*5 {
		return ErrChunkTooShort
	}

	copy(c.SHA256[:], data[0:32])
	buf := bytes.NewBuffer(data[32:])

	var err error
	for _, field := range []*uint32{&c.FullLength, &c.DataChunks,
		&c.ParityChunks, &c.MappingValue, &c.ChunkIndex} {

		err = binary.Read(buf, binary.BigEndian, field)
		if err != nil {
			return err
		}
	}

	c.Data, err = ioutil.ReadAll(buf)
	if err != nil {
		return err
	}

	c.WriteTime = 0

	return nil
}

func (c *Chunk) unmarshalVersion0001(data []byte) error {
	if len(data) < 32+4*5+8 {
		return ErrChunkTooShort
	}

	copy(c.SHA256[:], data[0:32])
	buf := bytes.NewBuffer(data[32:])

	var err error
	for _, field := range []interface{}{&c.FullLength, &c.DataChunks,
		&c.ParityChunks, &c.MappingValue, &c.WriteTime, &c.ChunkIndex} {

		err = binary.Read(buf, binary.BigEndian, field)
		if err != nil {
			return err
		}
	}

	c.Data, err = ioutil.ReadAll(buf)
	if err != nil {
		return err
	}

	return nil
}
