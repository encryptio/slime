package uuid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var ErrBadFormat = errors.New("bad format for UUID")

func Gen4() ([16]byte, error) {
	var ret [16]byte
	_, err := rand.Read(ret[:])
	if err != nil {
		return ret, err
	}
	ret[6] = (ret[6] & 0x0F) | 0x40
	ret[8] = (ret[8] & 0x3F) | 0x40
	return ret, nil
}

func Fmt(uuid [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

func Parse(s string) ([16]byte, error) {
	var uuid [16]byte

	s = strings.Replace(s, "-", "", -1)
	s = strings.Replace(s, "{", "", -1)
	s = strings.Replace(s, "}", "", -1)

	if len(s) != 32 {
		return uuid, ErrBadFormat
	}

	b, err := hex.DecodeString(s)
	if err != nil {
		return uuid, ErrBadFormat
	}

	copy(uuid[:], b)
	return uuid, nil
}
