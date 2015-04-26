package storedir

import (
	"os"
	"path/filepath"
	"testing"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storetests"
)

func TestDirectoryCorruption(t *testing.T) {
	ds, tmpDir := MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)
	defer ds.Close()

	storetests.ShouldCAS(t, ds, "hello", store.AnyV, store.DataV([]byte("world")))
	shouldHashcheck(t, ds, 1, 0)
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "first", "aGVsbG8="))
	storetests.ShouldGetError(t, ds, "hello", ErrCorruptObject)
	storetests.ShouldGetMiss(t, ds, "hello")
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "aGVsbG8="))

	storetests.ShouldCAS(t, ds, "other", store.AnyV, store.DataV([]byte("werld")))
	shouldHashcheck(t, ds, 1, 0)
	shouldCorrupt(t, filepath.Join(tmpDir, "data", "first", "b3RoZXI="))
	shouldHashcheck(t, ds, 0, 1)
	storetests.ShouldGetMiss(t, ds, "other")
	shouldHashcheck(t, ds, 0, 0)
	shouldFileExist(t, filepath.Join(tmpDir, "quarantine", "b3RoZXI="))
}