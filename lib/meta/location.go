package meta

import (
	"encoding/binary"

	"git.encryptio.com/kvl"
)

type Location struct {
	UUID [16]byte
	Host string
	Path string
	Name string
}

func (l *Location) toPair() kvl.Pair {
	var p kvl.Pair

	p.Key = make([]byte, 17)
	p.Key[0] = 'l'
	copy(p.Key[1:], l.UUID[:])

	p.Value = make([]byte, 1+4+len(l.Host)+len(l.Path))
	p.Value[0] = '\x00' // version
	binary.BigEndian.PutUint16(p.Value[1:3], uint16(len(l.Host)))
	binary.BigEndian.PutUint16(p.Value[3:5], uint16(len(l.Path)))
	binary.BigEndian.PutUint16(p.Value[5:7], uint16(len(l.Name)))
	copy(p.Value[7:], []byte(l.Host))
	copy(p.Value[7+len(l.Host):], []byte(l.Path))
	copy(p.Value[7+len(l.Host)+len(l.Path):], []byte(l.Name))

	return p
}

func (l *Location) fromPair(p kvl.Pair) error {
	if len(p.Key) != 17 {
		return ErrBadFormat
	}

	if p.Key[0] != 'l' {
		return ErrBadKeyType
	}

	copy(l.UUID[:], p.Key[1:])

	if len(p.Value) < 5 {
		return ErrBadFormat
	}

	if p.Value[0] != '\x00' {
		return ErrUnknownMetaVersion
	}

	hostLen := int(binary.BigEndian.Uint16(p.Value[1:3]))
	pathLen := int(binary.BigEndian.Uint16(p.Value[3:5]))
	nameLen := int(binary.BigEndian.Uint16(p.Value[5:7]))

	if len(p.Value) != 7+hostLen+pathLen+nameLen {
		return ErrBadFormat
	}

	l.Host = string(p.Value[7 : 7+hostLen])
	l.Path = string(p.Value[7+hostLen : 7+hostLen+pathLen])
	l.Name = string(p.Value[7+hostLen+pathLen:])

	return nil
}

func (l *Location) indexPairs() []kvl.Pair {
	return nil
}
