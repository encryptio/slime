package storedir

import (
	"bytes"
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

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

var ErrCorruptObject = errors.New("object is corrupt")

// A Directory is a Store which stores its data on a local filesystem.
type Directory struct {
	Dir         string
	uuid        [16]byte
	name        string
	perFileWait time.Duration
	perByteWait time.Duration

	stop chan struct{}

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
func OpenDirectory(dir string, perFileWait, perByteWait time.Duration) (*Directory, error) {
	data, err := ioutil.ReadFile(filepath.Join(dir, "uuid"))
	if err != nil {
		return nil, err
	}

	myUUID, err := uuid.Parse(string(data))
	if err != nil {
		return nil, err
	}

	host, _ := os.Hostname()

	ds := &Directory{
		Dir:         dir,
		uuid:        myUUID,
		name:        host + ":" + dir,
		stop:        make(chan struct{}),
		perFileWait: perFileWait,
		perByteWait: perByteWait,
	}

	go ds.hashcheckLoop()

	return ds, nil
}

func (ds *Directory) encodeKey(key string) string {
	return base64.URLEncoding.EncodeToString([]byte(key))
}

func (ds *Directory) keyToFilename(key string) string {
	return filepath.Join(ds.Dir, "data", ds.encodeKey(key))
}

func (ds *Directory) StillValid() bool {
	data, err := ioutil.ReadFile(filepath.Join(ds.Dir, "uuid"))
	if err != nil {
		return false
	}

	thatUUID, err := uuid.Parse(string(data))
	if err != nil {
		return false
	}

	return thatUUID == ds.uuid
}

func (ds *Directory) Close() error {
	close(ds.stop)
	return nil
}

func (ds *Directory) Get(key string) ([]byte, [32]byte, error) {
	var h [32]byte

	path := ds.keyToFilename(key)

	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, h, store.ErrNotFound
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

func (ds *Directory) Stat(key string) (*store.Stat, error) {
	fh, err := os.Open(ds.keyToFilename(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	defer fh.Close()

	st := &store.Stat{}

	_, err = fh.ReadAt(st.SHA256[:], 8)
	if err != nil {
		return nil, err
	}

	fi, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	st.Size = fi.Size() - 40

	return st, nil
}

func (ds *Directory) CAS(key string, from, to store.CASV) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !from.Any {
		if from.Present {
			fh, err := os.Open(ds.keyToFilename(key))
			if err != nil {
				if os.IsNotExist(err) {
					return store.ErrCASFailure
				}
				return err
			}

			var sha [32]byte
			_, err = fh.ReadAt(sha[:], 8)
			fh.Close()
			if err != nil {
				return err
			}

			if sha != from.SHA256 {
				return store.ErrCASFailure
			}
		} else {
			_, err := os.Stat(ds.keyToFilename(key))
			if !os.IsNotExist(err) {
				if err != nil {
					return err
				}
				return store.ErrCASFailure
			}
		}
	}

	if to.Present {
		h := fnv.New64a()
		h.Write(to.SHA256[:])
		h.Write(to.Data)
		var fnvHash [8]byte
		h.Sum(fnvHash[:0])

		fh, err := ioutil.TempFile(filepath.Join(ds.Dir, "data"), ".set_tmp_")
		if err != nil {
			return err
		}
		tmpname := fh.Name()

		for _, slice := range [][]byte{fnvHash[:], to.SHA256[:], to.Data} {
			_, err = fh.Write(slice)
			if err != nil {
				os.Remove(tmpname)
				fh.Close()
				return err
			}
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
	} else {
		err := os.Remove(ds.keyToFilename(key))
		if err != nil {
			if os.IsNotExist(err) {
				if from.Any || !from.Present {
					return nil
				} else {
					return store.ErrCASFailure
				}
			}
			return err
		}
		return nil
	}
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

		if limit > 0 && len(decodedNames) > limit*2+50 {
			sort.Strings(decodedNames)
			decodedNames = decodedNames[:limit]
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

func (ds *Directory) Hashcheck() (good, bad int64) {
	after := ""
	for {
		var goodStep, badStep int64
		goodStep, badStep, after = ds.hashstepInner(after)
		good += goodStep
		bad += badStep

		if after == "" {
			return
		}
	}
}

func (ds *Directory) hashcheckLoop() {
	for {
		_, bad := ds.hashstep()
		if bad != 0 {
			log.Printf("Found %v bad items hash check on %v\n",
				bad, uuid.Fmt(ds.UUID()))
		}

		select {
		case <-time.After(5 * time.Second):
		case <-ds.stop:
			return
		}
	}
}

func (ds *Directory) hashstep() (good, bad int64) {
	statePath := filepath.Join(ds.Dir, "hashcheck-at")
	after := ""

	fh, err := os.Open(statePath)
	if err == nil {
		data, err := ioutil.ReadAll(fh)
		fh.Close()
		if err != nil {
			log.Printf("Couldn't read from %v: %v", statePath, err)
			return
		}
		after = string(data)
	} else if !os.IsNotExist(err) {
		log.Printf("Couldn't open %v: %v", statePath, err)
		return
	}

	good, bad, after = ds.hashstepInner(after)

	fh, err = os.Create(statePath)
	if err != nil {
		log.Printf("Couldn't create %v: %v", statePath, err)
		return
	}
	defer fh.Close()

	_, err = fh.Write([]byte(after))
	if err != nil {
		log.Printf("Couldn't write to %v: %v", statePath, err)
	}

	return
}

func (ds *Directory) hashstepInner(afterIn string) (good, bad int64, after string) {
	after = afterIn

	keys, err := ds.List(after, 100)
	if err != nil {
		log.Printf("Couldn't list in %v for hash check: %v", ds.Dir, err)
		return
	}

	if len(keys) == 0 {
		after = ""
		return
	}

	for _, key := range keys {
		data, _, err := ds.Get(key)
		if err != nil && err != store.ErrNotFound {
			bad++
		} else {
			good++
		}

		wait := ds.perFileWait + time.Duration(len(data))*ds.perByteWait
		data = nil // free memory before sleep
		if wait > 0 {
			time.Sleep(wait)
		}

		after = key

		select {
		case <-ds.stop:
			return
		default:
		}
	}

	return
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
