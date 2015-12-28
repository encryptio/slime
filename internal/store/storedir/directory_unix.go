// +build !windows

package storedir

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

func (ds *Directory) FreeSpace(cancel <-chan struct{}) (int64, error) {
	s := unix.Statfs_t{}
	err := unix.Statfs(filepath.Join(ds.Dir, "data"), &s)
	if err != nil {
		return -1, err
	}

	// TODO: figure out and properly handle overflow
	return int64(s.Bavail) * int64(s.Bsize), nil
}
