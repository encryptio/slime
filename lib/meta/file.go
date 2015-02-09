package meta

import (
	"encoding/binary"
	"errors"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/tuple"
)

var (
	ErrUnknownMetaVersion = errors.New("unknown meta version")
	ErrBadKeyType         = errors.New("unexpected key type")
	ErrBadFormat          = errors.New("bad pair format")
)

type File struct {
	Path         string
	Size         uint64
	SHA256       [32]byte
	WriteTime    uint64
	DataChunks   uint16
	ParityChunks uint16
	MappingValue uint32
	Locations    [][16]byte
}

func fileKey(path string) []byte {
	key := make([]byte, len(path)+1)
	key[0] = 'f'
	copy(key[1:], []byte(path))
	return key
}

func (f *File) toPair() kvl.Pair {
	var p kvl.Pair

	p.Key = fileKey(f.Path)

	p.Value = make([]byte, 59+16*len(f.Locations))
	p.Value[0] = '\x00' // version
	binary.BigEndian.PutUint64(p.Value[1:9], f.Size)
	copy(p.Value[9:41], f.SHA256[:])
	binary.BigEndian.PutUint64(p.Value[41:49], f.WriteTime)
	binary.BigEndian.PutUint16(p.Value[49:51], f.DataChunks)
	binary.BigEndian.PutUint16(p.Value[51:53], f.ParityChunks)
	binary.BigEndian.PutUint32(p.Value[53:57], f.MappingValue)
	binary.BigEndian.PutUint16(p.Value[57:59], uint16(len(f.Locations)))
	at := 59
	for _, loc := range f.Locations {
		copy(p.Value[at:], loc[:])
		at += 16
	}

	return p
}

func (f *File) fromPair(p kvl.Pair) error {
	if len(p.Key) <= 1 {
		return ErrBadFormat
	}

	if p.Key[0] != 'f' {
		return ErrBadKeyType
	}

	f.Path = string(p.Key[1:])

	if len(p.Value) < 59 {
		return ErrBadFormat
	}

	if p.Value[0] != '\x00' {
		return ErrUnknownMetaVersion
	}

	f.Size = binary.BigEndian.Uint64(p.Value[1:9])
	copy(f.SHA256[:], p.Value[9:41])
	f.WriteTime = binary.BigEndian.Uint64(p.Value[41:49])
	f.DataChunks = binary.BigEndian.Uint16(p.Value[49:51])
	f.ParityChunks = binary.BigEndian.Uint16(p.Value[51:53])
	f.MappingValue = binary.BigEndian.Uint32(p.Value[53:57])
	locs := int(binary.BigEndian.Uint16(p.Value[57:59]))
	if len(p.Value) != 59+16*locs {
		return ErrBadFormat
	}
	f.Locations = make([][16]byte, locs)
	for i := range f.Locations {
		copy(f.Locations[i][:], p.Value[59+i*16:])
	}

	return nil
}

func (f *File) indexPairs() []kvl.Pair {
	ret := make([]kvl.Pair, 0, len(f.Locations))

	for _, loc := range f.Locations {
		key, err := tuple.Append(nil, "file", "location", loc[:])
		if err != nil {
			panic(err)
		}

		ret = append(ret, kvl.Pair{key, []byte(f.Path)})
	}

	return ret
}
