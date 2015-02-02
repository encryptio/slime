package meta

import (
	"encoding/binary"
	"errors"

	"git.encryptio.com/slime/lib/meta/store"
)

var (
	ErrUnknownMetaVersion = errors.New("unknown meta version")
	ErrBadKeyType         = errors.New("unexpected key type")
	ErrBadFormat          = errors.New("bad pair format")
)

type File struct {
	Path        string
	Size        uint64
	SHA256      [32]byte
	WriteTime   uint64
	Locations [][16]byte
}

func fileKey(path string) []byte {
	key := make([]byte, len(path)+1)
	key[0] = 'f'
	copy(key[1:], []byte(path))
	return key
}

func (f *File) toPair() store.Pair {
	var p store.Pair

	p.Key = fileKey(f.Path)

	p.Value = make([]byte, 1+8+32+8+2+16*len(f.LocationIDs))
	p.Value[0] = '\x00' // version
	binary.BigEndian.PutUint64(p.Value[1:9], f.Size)
	copy(p.Value[9:41], f.SHA256[:])
	binary.BigEndian.PutUint64(p.Value[41:49], f.WriteTime)
	binary.BigEndian.PutUint16(p.Value[49:57], uint16(len(f.Locations)))
	at := 57
	for _, loc := range f.LocationIDs {
		copy(p.Value[at:], loc[:])
		at += 16
	}

	return p
}

func (f *File) fromPair(p store.Pair) error {
	if len(p.Key) <= 1 {
		return ErrBadFormat
	}

	if p.Key[0] != 'f' {
		return ErrBadKeyType
	}

	f.Path = string(p.Key[1:])

	if len(p.Value) < 1+8+16+8+2 {
		return ErrBadFormat
	}

	if p.Value[0] != '\x00' {
		return ErrUnknownMetaVersion
	}

	f.Size = binary.BigEndian.Uint64(p.Value[1:9])
	copy(f.SHA256[:], p.Value[9:41])
	f.WriteTime = binary.BigEndian.Uint64(p.Value[41:49])
	locs := int(binary.BigEndian.Uint16(p.Value[49:57]))
	if len(p.Value) != 57+16*locs {
		return ErrBadFormat
	}
	f.Locations = make([][16]byte, locs)
	for i := range f.LocationIDs {
		copy(f.Locations[i][:], p.Value[57+i*16:])
	}

	return nil
}
