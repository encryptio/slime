package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func shouldList(t *testing.T, s Store, expect []string) {
	got, err := s.List("", 0)
	if err != nil {
		t.Errorf("Unexpected error from List(): %v", err)
		return
	}

	if !((len(got) == 0 && len(expect) == 0) ||
		reflect.DeepEqual(got, expect)) {

		t.Errorf("List() = %#v, but wanted %#v", got, expect)
	}
}

func shouldSet(t *testing.T, s Store, key string, data []byte) {
	err := s.Set(key, data)
	if err != nil {
		t.Errorf("Set(%#v, %#v) returned unexpected error %v", key, data, err)
	}
}

func shouldGet(t *testing.T, s Store, key string, data []byte) {
	got, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, got, data)
	}
}

func shouldGetError(t *testing.T, s Store, key string, wantErr error) {
	got, err := s.Get(key)
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func shouldGetMiss(t *testing.T, s Store, key string) {
	shouldGetError(t, s, key, ErrNotFound)
}

func shouldDelete(t *testing.T, s Store, key string) {
	err := s.Delete(key)
	if err != nil {
		t.Errorf("Delete(%#v) returned unexpected error %v", err)
	}
}

func shouldDeleteMiss(t *testing.T, s Store, key string) {
	err := s.Delete(key)
	if err != ErrNotFound {
		t.Errorf("Delete(%#v) = %v, but wanted %v", key, err, ErrNotFound)
	}
}

func shouldHashcheck(t *testing.T, ds *DirStore, good, bad int64) {
	gotGood, gotBad := ds.Hashcheck(0, 0, nil)
	if gotGood != good || gotBad != bad {
		t.Errorf("Hashcheck() = (%v, %v), but wanted (%v, %v)",
			gotGood, gotBad, good, bad)
	}
}

func shouldCorrupt(t *testing.T, filename string) {
	fh, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Couldn't open file for writing: %v", err)
	}
	defer fh.Close()

	b := make([]byte, 10)
	_, err = io.ReadFull(fh, b)
	if err != nil {
		t.Fatalf("Couldn't read from file: %v", err)
	}

	_, err = fh.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatalf("Couldn't seek in file: %v", err)
	}

	b[9]++

	_, err = fh.Write(b)
	if err != nil {
		t.Fatalf("Couldn't write to file: %v", err)
	}
}

func shouldFileExist(t *testing.T, filename string) {
	_, err := os.Stat(filename)
	if err != nil {
		t.Errorf("Couldn't stat %v: %v", filename, err)
	}
}

func makeDirStore(t *testing.T) (*DirStore, string) {
	tmpDir, err := ioutil.TempDir("", "slime_test_")
	if err != nil {
		t.Fatalf("Couldn't create temporary directory: %v", err)
	}

	err = CreateDirStore(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("CreateDirStore returned unexpected error %v", err)
	}

	ds, err := OpenDirStore(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("OpenDirStore returned unexpected error %v", err)
	}

	return ds, tmpDir
}

func TestDirStoreBasics(t *testing.T) {
	ds, tmpDir := makeDirStore(t)
	defer os.RemoveAll(tmpDir)

	shouldList(t, ds, nil)
	shouldGetMiss(t, ds, "hello")
	shouldSet(t, ds, "hello", []byte("world"))
	shouldGet(t, ds, "hello", []byte("world"))
	shouldList(t, ds, []string{"hello"})
	shouldSet(t, ds, "a", []byte("a"))
	shouldSet(t, ds, "empty", nil)
	shouldList(t, ds, []string{"a", "empty", "hello"})
	shouldGet(t, ds, "a", []byte("a"))
	shouldGet(t, ds, "empty", nil)
	shouldGet(t, ds, "hello", []byte("world"))
	shouldHashcheck(t, ds, 3, 0)
	shouldDeleteMiss(t, ds, "b")
	shouldDelete(t, ds, "a")
	shouldDeleteMiss(t, ds, "a")
	shouldGetMiss(t, ds, "a")
	shouldList(t, ds, []string{"empty", "hello"})
	shouldHashcheck(t, ds, 2, 0)
}

func TestDirStoreCorruption(t *testing.T) {
	ds, tmpDir := makeDirStore(t)
	defer os.RemoveAll(tmpDir)

	shouldSet(t, ds, "hello", []byte("world"))
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "aGVsbG8="))
	shouldGetError(t, ds, "hello", ErrCorruptObject)
	shouldGetMiss(t, ds, "hello")
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "aGVsbG8="))

	shouldSet(t, ds, "other", []byte("werld"))
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "b3RoZXI="))
	shouldHashcheck(t, ds, 0, 1)
	shouldGetMiss(t, ds, "other")
	shouldHashcheck(t, ds, 0, 0)
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "b3RoZXI="))
}
