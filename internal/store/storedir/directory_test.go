package storedir

import (
	"os"
	"testing"

	"github.com/encryptio/slime/internal/store/storetests"
)

func TestDirectoryCommon(t *testing.T) {
	ds, tmpDir := makeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)
	defer ds.Close()

	storetests.TestStore(t, ds)
	shouldHashcheck(t, ds, 0, 0)
}

func TestDirectoryCommonSmallSplit(t *testing.T) {
	ds, tmpDir := makeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)
	defer ds.Close()

	ds.mu.Lock()
	ds.minSplitSize = 2
	ds.maxSplitSize = 4
	ds.mu.Unlock()

	storetests.TestStore(t, ds)
	shouldHashcheck(t, ds, 0, 0)
}
