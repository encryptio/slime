package meta

import (
	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/tuple"
)

type Location struct {
	UUID       [16]byte
	URL        string
	Name       string
	AllocSplit []string
}

func (l *Location) toPair() kvl.Pair {
	var p kvl.Pair

	p.Key = tuple.MustAppend(nil, "location", l.UUID[:])

	p.Value = tuple.MustAppend(nil, 0, l.URL, l.Name)
	for _, split := range l.AllocSplit {
		p.Value = tuple.MustAppend(p.Value, split)
	}

	return p
}

func (l *Location) fromPair(p kvl.Pair) error {
	var typ string
	var uuid []byte
	err := tuple.UnpackInto(p.Key, &typ, &uuid)
	if err != nil {
		return err
	}
	if typ != "location" {
		return ErrBadKeyType
	}
	if len(uuid) != 16 {
		return ErrBadFormat
	}
	copy(l.UUID[:], uuid)

	var version int
	left, err := tuple.UnpackIntoPartial(p.Value,
		&version, &l.URL, &l.Name)
	if err != nil {
		return err
	}

	if version != 0 {
		return ErrUnknownMetaVersion
	}

	l.AllocSplit = nil
	for len(left) > 0 {
		var next string
		left, err = tuple.UnpackIntoPartial(left, &next)
		if err != nil {
			return err
		}
		l.AllocSplit = append(l.AllocSplit, next)
	}

	return nil
}

func (l *Location) indexPairs() []kvl.Pair {
	return nil
}
