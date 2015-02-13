// package store provides an interface for object stores
package store

import (
	"errors"
)

var (
	// ErrNotFound is returned from Store.Get and Store.Delete if the key
	// requested was not found in the object store
	ErrNotFound = errors.New("key not found")
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
