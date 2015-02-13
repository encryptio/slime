package store

import (
	"errors"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Store interface {
	UUID() [16]byte
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Delete(key string) error
	List(after string, limit int) ([]string, error)
	FreeSpace() (int64, error)
}
