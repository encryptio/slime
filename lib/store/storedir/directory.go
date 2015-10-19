package storedir

// The directory structure used by storedir is:
//
// $DIR/
//     uuid
//     hashcheck-at
//     quarantine/
//         $ENCODEDKEY
//         ...
//     data/
//         $SPLITNAME/
//             $ENCODEDKEY
//             ...
//         ...
//
// The ENCODEDKEY is the base64 URL encoding of the key stored.
//
// The data is split, by key, into subdirectories of "data", each one of which
// tries to tend towards a fixed number of keys. Each subdirectory handles a
// contiguous subset of keys.
//
// Splits do not overlap their key ranges.
//
// Each key file has the following format:
//     8-byte FNV-1a 64-byte hash of all of the following data (including sha)
//     32-byte SHA256 of the data
//     variable size data
//
// Key files that are found to violate their FNV hashes are moved into the
// "quarantine" directory.

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
	"strings"
	"sync"
	"time"

	"gopkg.in/tomb.v2"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

var ErrCorruptObject = errors.New("object is corrupt")

type split struct {
	Name      string
	Low, High string // inclusive
}

type splitsByLow []split

func (l splitsByLow) Len() int           { return len(l) }
func (l splitsByLow) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l splitsByLow) Less(i, j int) bool { return l[i].Low < l[j].Low }

// A Directory is a Store which stores its data on a local filesystem.
type Directory struct {
	Dir         string
	uuid        [16]byte
	name        string
	perFileWait time.Duration
	perByteWait time.Duration

	tomb tomb.Tomb

	// mu protects all operations in the directory as well as the fields below
	mu                         sync.RWMutex
	splits                     []split
	minSplitSize, maxSplitSize int
	resplitIndex               int
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
		err := os.Mkdir(d, 0777)
		if err != nil && !os.IsExist(err) {
			return err
		}
	}

	f, err := os.OpenFile(filepath.Join(dir, "uuid"),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
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
	return openDirectoryImpl(dir, perFileWait, perByteWait, false)
}

func openDirectoryImpl(dir string, perFileWait, perByteWait time.Duration, disableBackgroundLoops bool) (*Directory, error) {
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
		Dir:          dir,
		uuid:         myUUID,
		name:         host + ":" + dir,
		perFileWait:  perFileWait,
		perByteWait:  perByteWait,
		minSplitSize: 500,
		maxSplitSize: 2000,
	}

	err = ds.loadSplitsAndRecover()
	if err != nil {
		return nil, err
	}

	ds.tomb.Go(func() error {
		if !disableBackgroundLoops {
			ds.tomb.Go(ds.hashcheckLoop)
			ds.tomb.Go(ds.resplitLoop)
		}
		return nil
	})

	return ds, nil
}

func readdirnames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdirnames(-1)
	f.Close()
	return list, err
}

func (ds *Directory) loadSplitsAndRecover() error {
	fis, err := ioutil.ReadDir(filepath.Join(ds.Dir, "data"))
	if err != nil {
		return err
	}

	ds.splits = nil

	var toMigrate []os.FileInfo
	for _, fi := range fis {
		if !fi.IsDir() {
			// an older version of storedir stored keys directly in data/
			// we need to migrate these files to a new split
			toMigrate = append(toMigrate, fi)
			continue
		}

		this := split{
			Name: fi.Name(),
		}

		thisPath := filepath.Join(ds.Dir, "data", this.Name)

		contents, err := readdirnames(thisPath)
		if err != nil {
			return err
		}

		if len(contents) == 0 {
			// no files were found, this directory is a leftover
			err := os.Remove(thisPath)
			if err != nil {
				return err
			}

			continue
		}

		foundOne := false
		for _, name := range contents {
			if strings.HasSuffix(name, ".old") || strings.HasSuffix(name, ".new") {
				// an incomplete write, recover from it

				baseName := strings.TrimSuffix(strings.TrimSuffix(name, ".old"), ".new")
				basePath := filepath.Join(thisPath, baseName)

				// remove .new files, if they exist
				err := os.Remove(basePath + ".new")
				if err != nil && !os.IsNotExist(err) {
					return err
				}

				_, err = os.Stat(basePath)
				if err != nil && !os.IsNotExist(err) {
					return err
				}

				if err == nil {
					// base file exists, it should take precedence. remove any .old files.
					err := os.Remove(basePath + ".old")
					if err != nil && !os.IsNotExist(err) {
						return err
					}
				} else {
					// base file does NOT exist, move any .old file into place.
					err = os.Rename(basePath+".old", basePath)
					if err != nil && !os.IsNotExist(err) {
						return err
					}
				}

				name = baseName // we've tried hard to make this exist, continue with it
			}

			keyBytes, err := base64.URLEncoding.DecodeString(name)
			if err != nil {
				log.Printf("Bad filename in storedir at %v", filepath.Join(thisPath, name))
				continue
			}

			key := string(keyBytes)

			if !foundOne {
				this.Low = key
				this.High = key
				foundOne = true
			}

			if key < this.Low {
				this.Low = key
			}
			if key > this.High {
				this.High = key
			}
		}

		if !foundOne {
			// only bad filenames in this split, skip it
			continue
		}

		ds.splits = append(ds.splits, this)
	}

	if len(toMigrate) > 0 {
		log.Printf("Migrating %v files in %v to migration split dir", len(toMigrate), ds.Dir)

		err := os.Mkdir(filepath.Join(ds.Dir, "data", "0"), 0777)
		if err != nil && !os.IsExist(err) {
			return err
		}

		this := split{
			Name: "0",
		}

		migrated := 0

		foundOne := false
		for _, fi := range toMigrate {
			migrated++
			if migrated%1000 == 0 {
				log.Printf("Migrated %v files in %v so far\n", migrated, ds.Dir)
			}

			name := fi.Name()

			keyBytes, err := base64.URLEncoding.DecodeString(name)
			if err != nil {
				log.Printf("Bad filename in storedir at %v", filepath.Join(ds.Dir, "data", name))
				continue
			}

			key := string(keyBytes)

			if !foundOne {
				this.Low = key
				this.High = key
				foundOne = true
			}

			if key < this.Low {
				this.Low = key
			}
			if key > this.High {
				this.High = key
			}

			oldPath := filepath.Join(ds.Dir, "data", name)
			newPath := filepath.Join(ds.Dir, "data", "0", name)

			err = os.Rename(oldPath, newPath)
			if err != nil {
				log.Printf("Couldn't rename from %v to %v during migration: %v", oldPath, newPath, err)
			}
		}

		if foundOne {
			// at least one non-bad filename was found, the split is valid
			ds.splits = append(ds.splits, this)
		}
	}

	sort.Sort(splitsByLow(ds.splits))

	return nil
}

func (ds *Directory) Available() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

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
	ds.tomb.Kill(nil)
	return ds.tomb.Wait()
}

func (ds *Directory) findAndOpen(key string) (*os.File, string, error) {
	encodedKey := base64.URLEncoding.EncodeToString([]byte(key))

	for _, s := range ds.splits {
		if s.Low <= key && key <= s.High {
			path := filepath.Join(ds.Dir, "data", s.Name, encodedKey)

			fh, err := os.Open(path)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, "", err
			}

			return fh, path, nil
		}
	}

	return nil, "", nil
}

func (ds *Directory) Get(key string, cancel <-chan struct{}) ([]byte, store.Stat, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	fh, path, err := ds.findAndOpen(key)
	if err != nil {
		return nil, store.Stat{}, err
	}
	if fh == nil {
		return nil, store.Stat{}, store.ErrNotFound
	}
	defer fh.Close()

	var expectedFNV [8]byte
	_, err = io.ReadFull(fh, expectedFNV[:])
	if err != nil {
		return nil, store.Stat{}, err
	}

	fnver := fnv.New64a()
	rdr := io.TeeReader(fh, fnver)

	var expectedSHA256 [32]byte
	_, err = io.ReadFull(rdr, expectedSHA256[:])
	if err != nil {
		return nil, store.Stat{}, err
	}

	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, store.Stat{}, err
	}

	actualFNV := fnver.Sum(nil)

	if !bytes.Equal(actualFNV, expectedFNV[:]) {
		fh.Close()

		// TODO: this relocking is fucked and racy
		ds.mu.RUnlock()
		ds.mu.Lock()
		ds.quarantine(key, path)
		ds.mu.Unlock()
		ds.mu.RLock()
		return nil, store.Stat{}, ErrCorruptObject
	}

	return data, store.Stat{
		SHA256: expectedSHA256,
		Size:   int64(len(data)),
	}, nil
}

func (ds *Directory) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	st, _, err := ds.statUnlocked(key)
	return st, err
}

func (ds *Directory) statUnlocked(key string) (store.Stat, string, error) {
	fh, path, err := ds.findAndOpen(key)
	if err != nil {
		return store.Stat{}, "", err
	}
	if fh == nil {
		return store.Stat{}, "", store.ErrNotFound
	}
	defer fh.Close()

	st := store.Stat{}

	_, err = fh.ReadAt(st.SHA256[:], 8)
	if err != nil {
		return store.Stat{}, "", err
	}

	fi, err := fh.Stat()
	if err != nil {
		return store.Stat{}, "", err
	}
	st.Size = fi.Size() - 40

	return st, path, nil
}

func (ds *Directory) CAS(key string, from, to store.CASV, cancel <-chan struct{}) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stat, oldPath, err := ds.statUnlocked(key)
	if err != nil && err != store.ErrNotFound {
		return err
	}
	oldMissing := err == store.ErrNotFound

	if !from.Any {
		if from.Present {
			if oldMissing || stat.SHA256 != from.SHA256 {
				return store.ErrCASFailure
			}
		} else {
			if !oldMissing {
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

		path := oldPath

		if path == "" {
			// find a split for this data
			s, err := ds.chooseSplit(key)
			if err != nil {
				return err
			}

			encodedKey := base64.URLEncoding.EncodeToString([]byte(key))
			path = filepath.Join(ds.Dir, "data", s.Name, encodedKey)
		}

		// write a .new file
		fh, err := os.Create(path + ".new")
		if err != nil {
			return err
		}

		for _, slice := range [][]byte{fnvHash[:], to.SHA256[:], to.Data} {
			_, err = fh.Write(slice)
			if err != nil {
				fh.Close()
				os.Remove(path + ".new")
				return err
			}
		}

		err = fh.Close()
		if err != nil {
			os.Remove(path + ".new")
			return err
		}

		if oldPath != "" {
			// move the old file out of the way

			err = os.Rename(oldPath, oldPath+".old")
			if err != nil {
				os.Remove(path + ".new")
				return err
			}
		}

		// move the .new file to its resting place

		err = os.Rename(path+".new", path)
		if err != nil {
			os.Remove(path + ".new")
			if oldPath != "" {
				os.Rename(path+".old", path)
			}
			return err
		}

		// clean up the old file
		if oldPath != "" {
			err = os.Remove(oldPath + ".old")
			if err != nil {
				return err
			}
		}

		return nil
	}

	// !to.Present

	if oldPath == "" {
		return nil
	}

	return os.Remove(oldPath)
}

func (ds *Directory) chooseSplit(key string) (split, error) {
	// if there are no splits, make one
	if len(ds.splits) == 0 {
		this := split{
			Name: "1",
			Low:  key,
			High: key,
		}

		err := os.Mkdir(filepath.Join(ds.Dir, "data", "1"), 0777)
		if err != nil {
			return split{}, err
		}

		ds.splits = append(ds.splits, this)

		return this, nil
	}

	// see if it fits in an existing split
	for _, s := range ds.splits {
		if s.Low <= key && key <= s.High {
			return s, nil
		}
	}

	if key < ds.splits[0].Low {
		// key is before the first split, extend that one
		first := ds.splits[0]
		first.Low = key
		ds.splits[0] = first
		return first, nil
	}

	if key > ds.splits[len(ds.splits)-1].High {
		// key is after the last split, extend that one
		last := ds.splits[len(ds.splits)-1]
		last.High = key
		ds.splits[len(ds.splits)-1] = last
		return last, nil
	}

	// key is in one of the holes between splits

	// find the last split that is before the key
	idx := -1
	for i, s := range ds.splits {
		if s.Low > key {
			break
		}
		idx = i
	}

	if idx == -1 {
		// before all splits, already handled above
		panic("not reached")
	}

	// idx points to the last split that is before the key, extend that split
	// to include the new key
	middle := ds.splits[idx]
	if middle.High > key {
		panic("not reached")
	}
	middle.High = key
	ds.splits[idx] = middle
	return middle, nil
}

func (ds *Directory) List(afterKey string, limit int, cancel <-chan struct{}) ([]string, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	ret := make([]string, 0, 100)
	for _, s := range ds.splits {
		if s.High < afterKey {
			continue
		}

		fis, err := ioutil.ReadDir(filepath.Join(ds.Dir, "data", s.Name))
		if err != nil {
			return nil, err
		}

		for _, fi := range fis {
			keyByte, err := base64.URLEncoding.DecodeString(fi.Name())
			if err != nil {
				continue
			}

			key := string(keyByte)

			if key > afterKey {
				ret = append(ret, key)
			}
		}

		if limit > 0 && len(ret) >= limit {
			break
		}
	}

	sort.Strings(ret)

	if limit > 0 && len(ret) > limit {
		// copy instead of slice to avoid having the caller retain a possibly
		// very large array of strings
		cut := make([]string, limit)
		copy(cut, ret)
		ret = cut
	}

	return ret, nil
}

func (ds *Directory) UUID() [16]byte {
	return ds.uuid
}

func (ds *Directory) Name() string {
	return ds.name
}
