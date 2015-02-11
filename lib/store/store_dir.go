package store

import (
	"bytes"
	"encoding/base64"
	"golang.org/x/sys/unix"
	"time"
	"sort"
	"errors"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"git.encryptio.com/slime/lib/uuid"
)

var ErrCorruptObject = errors.New("object is corrupt")

type DirStore struct {
	Dir  string
	uuid [16]byte
}

func CreateDirStore(dir string) error {
	dirs := []string{
		dir,
		filepath.Join(dir, "data"),
		filepath.Join(dir, "quarantine"),
	}
	for _, d := range dirs {
		err := os.Mkdir(d, 0755)
		if err != nil && !os.IsExist(err) {
			return err
		}
	}

	f, err := os.OpenFile(filepath.Join(dir, "uuid"),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write([]byte(uuid.Fmt(uuid.Gen4())))
	if err != nil {
		f.Close()
		return err
	}

	return f.Close()
}

func OpenDirStore(dir string) (*DirStore, error) {
	data, err := ioutil.ReadFile(filepath.Join(dir, "uuid"))
	if err != nil {
		return nil, err
	}

	myUUID, err := uuid.Parse(string(data))
	if err != nil {
		return nil, err
	}

	return &DirStore{
		Dir:  dir,
		uuid: myUUID,
	}, nil
}

func (ds *DirStore) encodeKey(key string) string {
	return base64.URLEncoding.EncodeToString([]byte(key))
}

func (ds *DirStore) keyToFilename(key string) string {
	return filepath.Join(ds.Dir, "data", ds.encodeKey(key))
}

func (ds *DirStore) Get(key string) ([]byte, error) {
	path := ds.keyToFilename(key)

	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer fh.Close()

	var expectedHash [8]byte
	_, err = io.ReadFull(fh, expectedHash[:])
	if err != nil {
		return nil, err
	}

	h := fnv.New64a()
	rdr := io.TeeReader(fh, h)
	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, err
	}

	actualHash := h.Sum(nil)

	if !bytes.Equal(actualHash, expectedHash[:]) {
		ds.quarantine(key)
		return nil, ErrCorruptObject
	}

	return data, nil
}

func (ds *DirStore) Set(key string, data []byte) error {
	h := fnv.New64a()
	h.Write(data)
	hashValue := h.Sum(nil)

	fh, err := ioutil.TempFile(filepath.Join(ds.Dir, "data"), ".set_tmp_")
	if err != nil {
		return err
	}
	tmpname := fh.Name()

	_, err = fh.Write(hashValue)
	if err != nil {
		os.Remove(tmpname)
		fh.Close()
		return err
	}

	_, err = fh.Write(data)
	if err != nil {
		os.Remove(tmpname)
		fh.Close()
		return err
	}

	err = fh.Close()
	if err != nil {
		os.Remove(tmpname)
		return err
	}

	err = os.Rename(tmpname, ds.keyToFilename(key))
	if err != nil {
		os.Remove(tmpname)
		return err
	}

	return nil
}

func (ds *DirStore) Delete(key string) error {
	err := os.Remove(ds.keyToFilename(key))
	if os.IsNotExist(err) {
		return ErrNotFound
	}
	return err
}

func (ds *DirStore) List(afterKey string, limit int) ([]string, error) {
	dh, err := os.Open(filepath.Join(ds.Dir, "data"))
	if err != nil {
		return nil, err
	}
	defer dh.Close()

	allNames, err := dh.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	decodedNames := make([]string, 0, 100)
	for _, name := range allNames {
		dec, err := base64.URLEncoding.DecodeString(name)
		if err != nil {
			continue
		}
		str := string(dec)
		if str > afterKey {
			decodedNames = append(decodedNames, str)
		}
	}

	sort.Strings(decodedNames)

	if limit > 0 && len(decodedNames) > limit {
		// copy instead of slice to avoid having the caller retain a possibly
		// very large array of strings
		cut := make([]string, limit)
		copy(cut, decodedNames)
		decodedNames = cut
	}

	return decodedNames, nil
}

func (ds *DirStore) FreeSpace() (int64, error) {
	s := unix.Statfs_t{}
	err := unix.Statfs(filepath.Join(ds.Dir, "data"), &s)
	if err != nil {
		return -1, err
	}

	// TODO: figure out and properly handle overflow
	return int64(s.Bavail) * s.Bsize, nil
}

func (ds *DirStore) UUID() [16]byte {
	return ds.uuid
}

func (ds *DirStore) Hashcheck(perFileWait, perByteWait time.Duration) (good, bad int64) {

	after := ""
	for {
		keys, err := ds.List(after, 1000)
		if err != nil {
			log.Printf("Couldn't list in %v for hash check: %v", ds.Dir, err)
			return
		}

		if len(keys) == 0 {
			return
		}

		for _, key := range keys {
			data, err := ds.Get(key)
			if err != nil {
				bad++
			} else {
				good++
			}

			wait := perFileWait + time.Duration(len(data))*perByteWait
			data = nil // free memory before sleep
			if wait > 0 {
				time.Sleep(wait)
			}
		}

		after = keys[len(keys)-1]
	}
	return
}

func (ds *DirStore) quarantine(key string) {
	quarantinePath := filepath.Join(ds.Dir, "quarantine", ds.encodeKey(key))
	dataPath := ds.keyToFilename(key)

	err := os.Rename(dataPath, quarantinePath)
	if err != nil {
		log.Printf("Couldn't quarantine %v into %v: %v",
			dataPath, quarantinePath, err)
	}
}
