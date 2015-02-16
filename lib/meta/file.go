package meta

import (
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
	MappingValue uint32
	Locations    [][16]byte
}

func fileKey(path string) []byte {
	return tuple.MustAppend(nil, "file", path)
}

func (f *File) toPair() kvl.Pair {
	var p kvl.Pair

	p.Key = fileKey(f.Path)

	p.Value = tuple.MustAppend(nil,
		0, f.Size, f.SHA256[:], f.WriteTime, f.DataChunks, f.MappingValue)
	for _, loc := range f.Locations {
		p.Value = tuple.MustAppend(p.Value, loc[:])
	}

	return p
}

func (f *File) fromPair(p kvl.Pair) error {
	var typ string
	err := tuple.UnpackInto(p.Key, &typ, &f.Path)
	if err != nil {
		return err
	}
	if typ != "file" {
		return ErrBadKeyType
	}

	var version int
	var sha256 []byte
	left, err := tuple.UnpackIntoPartial(p.Value, &version, &f.Size, &sha256,
		&f.WriteTime, &f.DataChunks, &f.MappingValue)
	if version != 0 {
		return ErrUnknownMetaVersion
	}
	if len(sha256) != 32 {
		return ErrBadFormat
	}
	copy(f.SHA256[:], sha256)

	f.Locations = nil
	for len(left) > 0 {
		var next []byte
		left, err = tuple.UnpackIntoPartial(left, &next)
		if err != nil {
			return err
		}
		if len(next) != 16 {
			return ErrBadFormat
		}
		var data [16]byte
		copy(data[:], next)
		f.Locations = append(f.Locations, data)
	}

	return nil
}

func (f *File) indexPairs() []kvl.Pair {
	ret := make([]kvl.Pair, 0, len(f.Locations))

	for _, loc := range f.Locations {
		key, err := tuple.Append(nil, "file", "location", loc[:], f.Path)
		if err != nil {
			panic(err)
		}

		ret = append(ret, kvl.Pair{key, nil})
	}

	return ret
}
