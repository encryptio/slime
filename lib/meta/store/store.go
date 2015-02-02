package store

import (
	"errors"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Tx func(Ctx) (interface{}, error)

type Store interface {
	RunTx(Tx) (interface{}, error)
}

type Pair struct {
	Key, Value []byte
}

type Ctx interface {
	Get(key []byte) (Pair, error)
	Set(p Pair) error
	Delete(key []byte) error
	Range(low, high []byte, limit int) ([]Pair, error)
}
