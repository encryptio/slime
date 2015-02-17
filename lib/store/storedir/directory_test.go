package storedir

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"git.encryptio.com/slime/lib/store/storetests"
)

func shouldHashcheck(t *testing.T, ds *Directory, good, bad int64) {
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

func TestDirectoryCommon(t *testing.T) {
	ds, tmpDir := MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)

	storetests.TestStore(t, ds)
	shouldHashcheck(t, ds, 0, 0)
}

func TestDirectoryCorruption(t *testing.T) {
	ds, tmpDir := MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)

	storetests.ShouldSet(t, ds, "hello", []byte("world"))
	shouldHashcheck(t, ds, 1, 0)
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "aGVsbG8="))
	storetests.ShouldGetError(t, ds, "hello", ErrCorruptObject)
	storetests.ShouldGetMiss(t, ds, "hello")
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "aGVsbG8="))

	storetests.ShouldSet(t, ds, "other", []byte("werld"))
	shouldHashcheck(t, ds, 1, 0)
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "b3RoZXI="))
	shouldHashcheck(t, ds, 0, 1)
	storetests.ShouldGetMiss(t, ds, "other")
	shouldHashcheck(t, ds, 0, 0)
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "b3RoZXI="))
}
