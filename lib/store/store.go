package store

import (
	"errors"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Store interface {
	UUID() [16]byte
	Get(file string) ([]byte, error)
	Set(file string, data []byte) error
	Delete(file string) error
	List(after string, limit int) ([]string, error)
	FreeSpace() (int64, error)
}
