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
	ErrChunkBadChecksum = errors.New("chunk has bad data checksum")
)

var chunkMagicNumber = []byte("SLCK0000")

type FileInfo struct {
	SHA256       [32]byte
	FullLength   uint32
	DataChunks   uint32
	ParityChunks uint32
	MappingValue uint32
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

	binary.Write(buf, binary.BigEndian, c.ChunkIndex)
	buf.Write(c.Data)

	data := buf.Bytes()

	h := fnv.New64a()
	h.Write(data)
	fnvSum := h.Sum(nil)

	header := make([]byte, 16)
	copy(header[0:8], chunkMagicNumber)
	copy(header[8:16], fnvSum)

	return append(header, data...), nil
}

func (c *Chunk) UnmarshalBinary(data []byte) error {
	if len(data) < 16 {
		return ErrChunkTooShort
	}

	if !bytes.Equal(data[:8], chunkMagicNumber) {
		return ErrChunkBadMagic
	}

	hash := data[8:16]
	data = data[16:]

	h := fnv.New64a()
	h.Write(data)
	if !bytes.Equal(hash, h.Sum(nil)) {
		return ErrChunkBadChecksum
	}

	if len(data) < 32+4*5 {
		return ErrChunkTooShort
	}

	copy(c.SHA256[:], data[0:32])
	buf := bytes.NewBuffer(data[32:])

	var err error
	for _, field := range []*uint32{&c.FullLength, &c.DataChunks, &c.ParityChunks, &c.MappingValue, &c.ChunkIndex} {
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
