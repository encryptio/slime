package store

import (
	"fmt"
	"path"
	"strings"
	"sync"
)

type ram struct {
	store  map[string][]byte
	config []byte
	m      sync.Mutex
}

func NewRAM() Target {
	return &ram{store: make(map[string][]byte, 16)}
}

func (r *ram) Search(prefix string) ([]FileInfo, error) {
	r.m.Lock()
	defer r.m.Unlock()
	prefix = normalizePath(prefix)

	dir, file := path.Split(prefix)

	var out []FileInfo
	marked := make(map[string]struct{})
	for k := range r.store {
		isdir := false
		for k != "" && k != "/" {
			kdir, kfile := path.Split(k)

			if kdir == dir && strings.HasPrefix(kfile, file) {
				_, ok := marked[k]
				if !ok {
					marked[k] = struct{}{}
					out = append(out, FileInfo{k, isdir})
				}
			}

			k = strings.TrimSuffix(kdir, "/")
			isdir = true
		}
	}

	return out, nil
}

func (r *ram) Get(file string) ([]byte, error) {
	r.m.Lock()
	defer r.m.Unlock()
	file = normalizePath(file)

	d, ok := r.store[file]
	if !ok {
		return nil, ErrItemDoesNotExist
	}

	return d, nil
}

func (r *ram) Set(file string, data []byte) error {
	r.m.Lock()
	defer r.m.Unlock()
	file = normalizePath(file)

	if len(data) == 0 {
		delete(r.store, file)
	} else {
		r.store[file] = data
	}

	return nil
}

func (r *ram) FreeSpace() (int64, error) {
	return 0, ErrNotImplemented
}

func (r *ram) GetConfig() ([]byte, error) {
	r.m.Lock()
	defer r.m.Unlock()

	if len(r.config) == 0 {
		return nil, ErrItemDoesNotExist
	}

	return r.config, nil
}

func (r *ram) SetConfig(data []byte) error {
	r.m.Lock()
	defer r.m.Unlock()

	r.config = data

	return nil
}

func (r *ram) Name() string {
	return fmt.Sprintf("ram(%p)", r)
}
