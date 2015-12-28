package storehttp

import (
	"errors"
	"fmt"
)

var (
	ErrUnparsableSHAResponse = errors.New("response had an unparsable sha256")
)

type HashMismatchError struct {
	Got, Want [32]byte
}

func (e HashMismatchError) Error() string {
	return fmt.Sprintf("bad hash (got %x, wanted %x)", e.Got, e.Want)
}
