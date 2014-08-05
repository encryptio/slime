package store

// TODO: support for path separators that are not /

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

var ErrNotImplemented = errors.New("not implemented yet")

type NotDirectoryError string

func (e NotDirectoryError) Error() string {
	return string(e) + " is not a directory"
}

type fs struct {
	Path string
	mu   sync.RWMutex
}

func NewFS(path string) (Target, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, NotDirectoryError(path)
	}

	fi, err = os.Stat(filepath.Join(path, "files"))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		err = os.Mkdir(filepath.Join(path, "files"), 0700)
		if err != nil {
			return nil, err
		}
	} else if !fi.IsDir() {
		return nil, NotDirectoryError(filepath.Join(path, "files"))
	}

	return &fs{Path: path}, nil
}

func (fs *fs) Search(prefix string) ([]FileInfo, error) {
	prefix = normalizePath(prefix)
	dir, file := filepath.Split(prefix)

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	fis, err := ioutil.ReadDir(filepath.Join(fs.Path, "files", dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []FileInfo
	for _, fi := range fis {
		if strings.HasPrefix(fi.Name(), file) {
			out = append(out, FileInfo{filepath.Join(dir, fi.Name()), fi.IsDir()})
		}
	}

	return out, nil
}

func (fs *fs) Get(file string) ([]byte, error) {
	file = normalizePath(file)
	return fs.getPath(filepath.Join(fs.Path, "files", file))
}

func (fs *fs) getPath(path string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrItemDoesNotExist
		}
		return nil, err
	}
	defer fh.Close()

	data, err := ioutil.ReadAll(fh)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, ErrItemDoesNotExist
	}

	return data, nil
}

func (fs *fs) Set(file string, data []byte) error {
	file = normalizePath(file)
	return fs.setPath(filepath.Join(fs.Path, "files", file), data)
}

func (fs *fs) setPath(path string, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if len(data) == 0 {
		err := os.Remove(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		prefix := filepath.Join(fs.Path, "files/")

		// remove all empty directories up
		for {
			path, _ = filepath.Split(path)
			path = strings.TrimSuffix(path, string(filepath.Separator))

			if !strings.HasPrefix(path, prefix) || path == prefix {
				break
			}

			err = os.Remove(path)
			if err != nil {
				if os.IsNotExist(err) {
					break
				}

				pe, ok := err.(*os.PathError)
				if !ok {
					return err
				}

				errno, ok := pe.Err.(syscall.Errno)
				if !ok {
					return err
				}

				if errno == syscall.ENOTEMPTY {
					break
				}

				return err
			}
		}

		return nil
	}

	fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		dir, _ := filepath.Split(path)
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			return err
		}

		fh, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
	}
	defer fh.Close()

	n, err := fh.Write(data)
	if err != nil {
		return err
	}

	err = fh.Truncate(int64(n))
	if err != nil {
		return err
	}

	return nil
}

func (fs *fs) FreeSpace() (int64, error) {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(filepath.Join(fs.Path, "files"), &s)
	if err != nil {
		return 0, err
	}

	// TODO: figure out and properly handle overflow
	return int64(s.Bavail) * int64(s.Bsize), nil
}

func (fs *fs) GetConfig() ([]byte, error) {
	// TODO: verify checksum
	return fs.getPath(filepath.Join(fs.Path, "config"))
}

func (fs *fs) SetConfig(data []byte) error {
	// TODO: write checksum file
	return fs.setPath(filepath.Join(fs.Path, "config"), data)
}

func (fs *fs) Name() string {
	return fmt.Sprintf("fs(%v)", fs.Path)
}
