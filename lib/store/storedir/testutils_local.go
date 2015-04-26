package storedir

import (
	"io"
	"os"
	"testing"
)

func shouldHashcheck(t *testing.T, ds *Directory, good, bad int64) {
	gotGood, gotBad := ds.Hashcheck()
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
