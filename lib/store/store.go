// package store provides an interface for object stores
package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned from Store.Stat if the key requested was not
	// found in the object store.
	ErrNotFound = errors.New("key not found")

	// ErrCASFailure is returned from Store.CAS if the data stored at the key
	// given did not match the "from" CASV.
	ErrCASFailure = errors.New("cas failure")
)

type Stat struct {
	SHA256 [32]byte
	Size   int64
}

// A Store is a object store. Keys are strings of non-zero length, subject to
// implementation-specific restrictions. Data is arbitrary, subject to
// implementation-specific limits on length.
type Store interface {
	UUID() [16]byte
	Name() string

	// Get retrieves the value for the given key and its SHA256 hash, or
	// ErrNotFound if the key does not exist.
	Get(key string) ([]byte, [32]byte, error)

	// List returns a list of keys which compare bytewise greater than after,
	// up to the limit number of keys. If limit is <=0, then the return size is
	// unlimited.
	List(after string, limit int) ([]string, error)

	// FreeSpace returns the expected number of bytes free on this Store.
	FreeSpace() (int64, error)

	// Stat returns a bit of info about the key given, or ErrNotFound if the
	// key-value pair is nonexistent.
	Stat(key string) (*Stat, error)

	// CAS writes to the object store. It returns ErrCASFailure if the value
	// currently in the store does not match the "from" argument. If it does,
	// then CAS atomically writes the value in the "to" argument to the store.
	//
	// If "from.Any" is true, it matches any value (including nonexistence.)
	// If "from.Present" is false, it matches only the nonexistent value.
	// Otherwise, "from.SHA256" is consulted and must match the SHA256 of
	// the value stored. "from.Data" is ignored.
	//
	// If "to.Present" is false, the value is deleted (set to the nonexistent
	// value.) Otherwise, "to.Data" is written with the assumed-correct hash
	// given in "to.SHA256."
	CAS(key string, from, to CASV) error
}

var (
	AnyV     = CASV{Any: true}
	MissingV = CASV{Present: false}
)

func DataV(data []byte) CASV {
	return CASV{
		Present: true,
		SHA256:  sha256.Sum256(data),
		Data:    data,
	}
}

// A CASV is a value used for the store.CAS operation.
type CASV struct {
	Any     bool
	Present bool
	SHA256  [32]byte
	Data    []byte
}

func (v CASV) String() string {
	if v.Any {
		return "casv(any)"
	}
	if !v.Present {
		return "casv(not present)"
	}
	return fmt.Sprintf("casv(sha256=%x, data=%#v)", v.SHA256, string(v.Data))
}
