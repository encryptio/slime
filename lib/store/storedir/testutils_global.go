package storedir

import (
	"io/ioutil"
	"os"
	"testing"
)

func MakeTestingDirectory(t *testing.T) (*Directory, string) {
	tmpDir, err := ioutil.TempDir("", "slime_test_")
	if err != nil {
		t.Fatalf("Couldn't create temporary directory: %v", err)
	}

	err = CreateDirectory(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("CreateDirectory returned unexpected error %v", err)
	}

	ds, err := openDirectoryImpl(tmpDir, 0, 0, true)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("OpenDirectory returned unexpected error %v", err)
	}

	return ds, tmpDir
}
