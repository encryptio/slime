package store

import (
	"errors"
	"path"
	"strings"
)

var (
	ErrItemDoesNotExist = errors.New("item does not exist")
)

const MaxItemSize = 16 * 1024 * 1024

type FileInfo struct {
	Name  string
	IsDir bool
}

type Target interface {
	// Search gets a filename prefix and returns a slice of file names/directory
	// names that exist whose prefix is the one given.
	Search(prefix string) ([]FileInfo, error)
	Get(file string) ([]byte, error)
	Set(file string, data []byte) error

	FreeSpace() (int64, error)
	GetConfig() ([]byte, error)
	SetConfig([]byte) error

	Name() string
}

func normalizePath(p string) string {
	suf := ""
	if strings.HasSuffix(p, "/") {
		suf = "/"
	}

	cleaned := path.Clean("/" + p)
	if cleaned == "/" {
		suf = ""
	}

	return cleaned + suf
}
