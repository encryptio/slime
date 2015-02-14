package multi

import (
	"io/ioutil"
	"os"
	"testing"

	"git.encryptio.com/slime/lib/store"
)

func makeDirectory(t *testing.T) (*store.Directory, string) {
	tmpDir, err := ioutil.TempDir("", "slime_test_")
	if err != nil {
		t.Fatalf("Couldn't create temporary directory: %v", err)
	}

	err = store.CreateDirectory(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("CreateDirectory returned unexpected error %v", err)
	}

	ds, err := store.OpenDirectory(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("OpenDirectory returned unexpected error %v", err)
	}

	return ds, tmpDir
}
