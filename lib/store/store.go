// package store provides an interface for object stores
package store

import (
	"errors"
)

var (
	// ErrNotFound is returned from Store.Get and Store.Delete if the key
	// requested was not found in the object store
	ErrNotFound = errors.New("key not found")

	// ErrCASFailure is returned from Store256.CASWith256 if the data stored
	// at the key given did not match the key given.
	ErrCASFailure = errors.New("cas failure")
)

// A Store is a object store. Keys are strings of non-zero length, subject to
// implementation-specific restrictions. Data is arbitrary, subject to
// implementation-specific limits on length.
type Store interface {
	UUID() [16]byte

	// Get retrieves the value for the given key, or ErrNotFound if the key does
	// not exist.
	Get(key string) ([]byte, error)

	// Set adds or replaces the value for the given key.
	Set(key string, data []byte) error

	// Delete removes the value for a given key. It may return ErrNotFound if
	// the key does not exist.
	Delete(key string) error

	// List returns a list of keys which compare bytewise greater than after,
	// up to the limit number of keys. If limit is <=0, then the return size is
	// unlimited.
	List(after string, limit int) ([]string, error)

	// FreeSpace returns the expected number of bytes free on this Store.
	FreeSpace() (int64, error)
}

// A Store256 is a Store with additional methods that specify the SHA256 of the
// content.
type Store256 interface {
	Store

	// GetWith256 is like Get, but also returns the SHA256 of the content.
	GetWith256(key string) ([]byte, [32]byte, error)

	// SetWith256 is like Set, but passes in the (already-verified) SHA256 of
	// the content.
	SetWith256(key string, data []byte, h [32]byte) error

	// CASWith256 is an atomic compare-and-swap that sets the content of the
	// given key-object pair iff the existing data matches oldHash. newHash
	// is the already-verified SHA256 of the new content, like SetWith256.
	CASWith256(key string, oldH [32]byte, data []byte, newH [32]byte) error
}
