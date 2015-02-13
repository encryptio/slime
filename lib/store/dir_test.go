package store

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

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

	testStoreBasics(t, ds)
	shouldHashcheck(t, ds, 0, 0)
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
