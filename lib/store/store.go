// Package store provides an interface for object stores
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

	// ErrUnavailable is returned from a SometimesStore when the store is
	// unavailable.
	ErrUnavailable = errors.New("unavailable")

	// ErrCancelled is returned from Stores when the cancel channel is closed
	// and the operation has been aborted.
	ErrCancelled = errors.New("cancelled")
)

type Stat struct {
	SHA256 [32]byte
	Size   int64
}

// A Store is a object store. Keys are strings of non-zero length, subject to
// implementation-specific restrictions. Data is arbitrary, subject to
// implementation-specific limits on length.
//
// If a non-nil cancel channel is given, then if it is closed, the operation
// may exit early and return ErrCancelled. However, the operation may be in
// any state after returning ErrCancelled, including successful completion.
type Store interface {
	UUID() [16]byte
	Name() string

	// Get retrieves the value for the given key and its Stat, or ErrNotFound
	// if the key does not exist.
	Get(key string, cancel <-chan struct{}) ([]byte, Stat, error)

	// List returns a list of keys which compare bytewise greater than after,
	// up to the limit number of keys. If limit is <=0, then the return size is
	// unlimited.
	List(after string, limit int, cancel <-chan struct{}) ([]string, error)

	// FreeSpace returns the expected number of bytes free on this Store.
	FreeSpace(cancel <-chan struct{}) (int64, error)

	// Stat returns a bit of info about the key given, or ErrNotFound if the
	// key-value pair is nonexistent.
	Stat(key string, cancel <-chan struct{}) (Stat, error)

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
	CAS(key string, from, to CASV, cancel <-chan struct{}) error

	// Close cleans up any resources for the Store. The behavior of the other
	// methods is undefined as soon as Close has been called, regardless of
	// its return value.
	Close() error
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

// A SometimesStore is a Store that may not be available at all times. When it
// is available, the Store methods return normally. When it is not, store
// operations will return ErrUnavailable. If a store is not Available, its Name
// and UUID methods may return the zero value.
type SometimesStore interface {
	Store

	// Available returns the most recent known information on whether the store
	// is available. Note that there may be a delay between the store becoming
	// available/unavailable and this method returning the correct value.
	Available() bool
}
