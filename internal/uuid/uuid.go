// Package uuid provides utilities for working with UUIDs
package uuid

import (
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

// ErrBadFormat is returned from Parse when the input does not look like a UUID.
var ErrBadFormat = errors.New("bad format for UUID")

var rng *rand.Rand
var rngMu sync.Mutex

func init() {
	var buf [8]byte
	_, err := crand.Read(buf[:])
	if err != nil {
		panic(err)
	}
	seed := int64(binary.BigEndian.Uint64(buf[:]))
	source := rand.NewSource(seed)
	rng = rand.New(source)
}

// Gen4 generates a type-4 (random) UUID and returns it.
func Gen4() [16]byte {
	var ret [16]byte
	rngMu.Lock()
	for i := 0; i < 4; i++ {
		binary.BigEndian.PutUint32(ret[i*4:], rng.Uint32())
	}
	rngMu.Unlock()
	ret[6] = (ret[6] & 0x0F) | 0x40
	ret[8] = (ret[8] & 0x3F) | 0x40
	return ret
}

// Fmt formats a UUID as a string using the standard XXXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX format.
func Fmt(uuid [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// Parse parses a UUID from a string.
//
// It accepts these formats:
//     {12345678-1234-1234-1234-12345678abcd}
//     12345678-1234-1234-1234-12345678abcd
//     1234567812341234123412345678abcd
//
// When passed a UUID format other than these, it may either return ErrBadFormat
// or return a best guess parsing of the UUID.
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
