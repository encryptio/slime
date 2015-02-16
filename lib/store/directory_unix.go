// +build !windows

package store

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

func (ds *Directory) FreeSpace() (int64, error) {
	s := unix.Statfs_t{}
	err := unix.Statfs(filepath.Join(ds.Dir, "data"), &s)
	if err != nil {
		return -1, err
	}

	// TODO: figure out and properly handle overflow
	return int64(s.Bavail) * s.Bsize, nil
}
