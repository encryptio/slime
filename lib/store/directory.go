package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.encryptio.com/slime/lib/uuid"
)

var ErrCorruptObject = errors.New("object is corrupt")

// A Directory is a Store which stores its data on a local filesystem.
type Directory struct {
	Dir  string
	uuid [16]byte
	name string

	// protects write operations into the directory, to make the multiple
	// open calls during CAS operations work atomically
	mu sync.Mutex
}

// CreateDirectory initializes a new Directory at the given location, suitable
// for OpenDirectory. It will return an error if one already exists.
func CreateDirectory(dir string) error {
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

// OpenDirectory opens an existing directory store.
func OpenDirectory(dir string) (*Directory, error) {
	data, err := ioutil.ReadFile(filepath.Join(dir, "uuid"))
	if err != nil {
		return nil, err
	}

	myUUID, err := uuid.Parse(string(data))
	if err != nil {
		return nil, err
	}

	host, _ := os.Hostname()

	return &Directory{
		Dir:  dir,
		uuid: myUUID,
		name: host + ":" + dir,
	}, nil
}

func (ds *Directory) encodeKey(key string) string {
	return base64.URLEncoding.EncodeToString([]byte(key))
}

func (ds *Directory) keyToFilename(key string) string {
	return filepath.Join(ds.Dir, "data", ds.encodeKey(key))
}

func (ds *Directory) Get(key string) ([]byte, error) {
	d, _, err := ds.GetWith256(key)
	return d, err
}

func (ds *Directory) GetWith256(key string) ([]byte, [32]byte, error) {
	var h [32]byte

	path := ds.keyToFilename(key)

	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, h, ErrNotFound
		}
		return nil, h, err
	}
	defer fh.Close()

	var expectedFNV [8]byte
	_, err = io.ReadFull(fh, expectedFNV[:])
	if err != nil {
		return nil, h, err
	}

	fnver := fnv.New64a()
	rdr := io.TeeReader(fh, fnver)

	_, err = io.ReadFull(rdr, h[:])
	if err != nil {
		return nil, h, err
	}

	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, h, err
	}

	actualFNV := fnver.Sum(nil)

	if !bytes.Equal(actualFNV, expectedFNV[:]) {
		ds.quarantine(key)
		return nil, h, ErrCorruptObject
	}

	return data, h, nil
}

func (ds *Directory) Stat(key string) (*Stat, error) {
	fh, err := os.Open(ds.keyToFilename(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer fh.Close()

	st := &Stat{}

	_, err = fh.ReadAt(st.SHA256[:], 8)
	if err != nil {
		return nil, err
	}

	fi, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	st.Size = fi.Size()

	return st, nil
}

func (ds *Directory) Set(key string, data []byte) error {
	return ds.SetWith256(key, data, sha256.Sum256(data))
}

func (ds *Directory) SetWith256(key string, data []byte, sha [32]byte) error {
	h := fnv.New64a()
	h.Write(sha[:])
	h.Write(data)
	fnvHash := h.Sum(nil)

	ds.mu.Lock()
	defer ds.mu.Unlock()

	return ds.lockedSet(key, data, sha, fnvHash)
}

func (ds *Directory) CASWith256(key string, oldH [32]byte, data []byte, newH [32]byte) error {
	h := fnv.New64a()
	h.Write(newH[:])
	h.Write(data)
	fnvHash := h.Sum(nil)

	ds.mu.Lock()
	defer ds.mu.Unlock()

	fh, err := os.Open(ds.keyToFilename(key))
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	defer fh.Close()

	var haveSHA [32]byte
	_, err = fh.ReadAt(haveSHA[:], 8)
	if err != nil {
		return err
	}

	if haveSHA != oldH {
		return ErrCASFailure
	}

	return ds.lockedSet(key, data, newH, fnvHash)
}

func (ds *Directory) lockedSet(key string, data []byte, sha [32]byte, fnvHash []byte) error {
	fh, err := ioutil.TempFile(filepath.Join(ds.Dir, "data"), ".set_tmp_")
	if err != nil {
		return err
	}
	tmpname := fh.Name()

	_, err = fh.Write(fnvHash)
	if err != nil {
		os.Remove(tmpname)
		fh.Close()
		return err
	}

	_, err = fh.Write(sha[:])
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

func (ds *Directory) Delete(key string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	err := os.Remove(ds.keyToFilename(key))
	if os.IsNotExist(err) {
		return ErrNotFound
	}
	return err
}

func (ds *Directory) List(afterKey string, limit int) ([]string, error) {
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

func (ds *Directory) UUID() [16]byte {
	return ds.uuid
}

func (ds *Directory) Name() string {
	return ds.name
}

func (ds *Directory) Hashcheck(perFileWait, perByteWait time.Duration, stop <-chan struct{}) (good, bad int64) {
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

			select {
			case <-stop:
				return
			default:
			}
		}

		after = keys[len(keys)-1]
	}
}

func (ds *Directory) quarantine(key string) {
	quarantinePath := filepath.Join(ds.Dir, "quarantine", ds.encodeKey(key))
	dataPath := ds.keyToFilename(key)

	ds.mu.Lock()
	defer ds.mu.Unlock()

	err := os.Rename(dataPath, quarantinePath)
	if err != nil {
		log.Printf("Couldn't quarantine %v into %v: %v",
			dataPath, quarantinePath, err)
	}
}
