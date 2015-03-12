package meta

import (
	"errors"
	"fmt"

	"git.encryptio.com/slime/lib/uuid"

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
	PrefixID     [16]byte
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
		0, f.Size, f.SHA256, f.WriteTime, f.PrefixID, f.DataChunks,
		f.MappingValue)
	for _, loc := range f.Locations {
		p.Value = tuple.MustAppend(p.Value, loc)
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
	left, err := tuple.UnpackIntoPartial(p.Value, &version, &f.Size, &f.SHA256,
		&f.WriteTime, &f.PrefixID, &f.DataChunks, &f.MappingValue)
	if version != 0 {
		return ErrUnknownMetaVersion
	}

	f.Locations = nil
	for len(left) > 0 {
		var next [16]byte
		left, err = tuple.UnpackIntoPartial(left, &next)
		if err != nil {
			return err
		}
		f.Locations = append(f.Locations, next)
	}

	return nil
}

func (f *File) indexPairs() []kvl.Pair {
	ret := make([]kvl.Pair, 0, len(f.Locations))

	for idx, loc := range f.Locations {
		localKey := fmt.Sprintf("%v_%x_%v",
			uuid.Fmt(f.PrefixID), f.SHA256[:8], idx)

		ret = append(ret, kvl.Pair{
			tuple.MustAppend(nil, "file", "location", loc, f.Path),
			nil,
		}, kvl.Pair{
			tuple.MustAppend(nil, "locationlist", loc, localKey),
			nil,
		})
	}

	ret = append(ret, kvl.Pair{
		tuple.MustAppend(nil, "file", "prefix", f.PrefixID),
		[]byte(f.Path),
	})

	return ret
}
