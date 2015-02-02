package meta

import (
	"encoding/binary"

	"git.encryptio.com/slime/lib/meta/store"
)

type Location struct {
	UUID [16]byte
	Host string
	Path string
}

func (l *Location) toPair() store.Pair {
	var p store.Pair

	p.Key = make([]byte, 17)
	p.Key[0] = 'l'
	copy(p.Key[1:], l.UUID[:])

	p.Value = make([]byte, 1+16+4+len(l.Host)+len(l.Path))
	p.Value[0] = '\x00' // version
	binary.BigEndian.PutUint16(p.Value[1:3], uint16(len(l.Host)))
	binary.BigEndian.PutUint16(p.Value[3:5], uint16(len(l.Path)))
	copy(p.Value[5:], []byte(l.Host))
	copy(p.Value[5+len(l.Host):], []byte(l.Path))

	return p
}

func (l *Location) fromPair(p store.Pair) error {
	if len(p.Key) != 17 {
		return ErrBadFormat
	}

	if p.Key[0] != 'l' {
		return ErrBadKeyType
	}

	copy(l.UUID[:], l.Key[1:])

	if len(p.Value) < 5 {
		return ErrBadFormat
	}

	if p.Value[0] != '\x00' {
		return ErrUnknownMetaVersion
	}

	hostLen := int(binary.BigEndian.Uint16(p.Value[1:3]))
	pathLen := int(binary.BigEndian.Uint16(p.Value[3:5]))

	if len(p.Value) != 5+hostLen+pathLen {
		return ErrBadFormat
	}

	l.Host = string(p.Value[5 : 5+hostLen])
	l.Path = string(p.Value[5+hostLen:])

	return nil
}
