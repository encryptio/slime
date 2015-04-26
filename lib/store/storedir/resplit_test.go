package storedir

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storetests"
)

func splitCounts(dir string) []int {
	fis, err := ioutil.ReadDir(filepath.Join(dir, "data"))
	if err != nil {
		panic(err)
	}

	var ret []int
	for _, fi := range fis {
		inner, err := ioutil.ReadDir(filepath.Join(dir, "data", fi.Name()))
		if err != nil {
			panic(err)
		}
		ret = append(ret, len(inner))
	}

	return ret
}

func checkSplitCountSizes(t *testing.T, context string, dir string, min, max int) {
	counts := splitCounts(dir)

	failed := false
	if len(counts) > 1 {
		for _, count := range counts {
			failed = failed || count < min || count > max
		}
	} else if len(counts) == 1 {
		// a single split is allowed to be undersized
		failed = counts[0] > max
	}

	if failed {
		t.Errorf("in %v, split counts were out of range: %v", context, counts)
	}
}

func TestResplit(t *testing.T) {
	ds, tmpDir := MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)
	defer ds.Close()

	ds.mu.Lock()
	ds.minSplitSize = 2
	ds.maxSplitSize = 8
	ds.mu.Unlock()

	const itemCount = 50

	for i := 0; i < itemCount; i++ {
		key := strconv.FormatInt(int64(i), 3)
		storetests.ShouldCAS(t, ds, key, store.AnyV, store.DataV([]byte(key)))

		ds.resplit()

		for j := 0; j < itemCount; j++ {
			key := strconv.FormatInt(int64(j), 3)
			if j <= i {
				storetests.ShouldGet(t, ds, key, []byte(key))
			} else {
				storetests.ShouldGetMiss(t, ds, key)
			}
		}

		checkSplitCountSizes(t, "insertion", tmpDir, 2, 8)
	}

	for i := 0; i < itemCount; i++ {
		key := strconv.FormatInt(int64(i), 3)
		storetests.ShouldCAS(t, ds, key, store.AnyV, store.MissingV)

		ds.resplit()

		for j := 0; j < itemCount; j++ {
			key := strconv.FormatInt(int64(j), 3)
			if j <= i {
				storetests.ShouldGetMiss(t, ds, key)
			} else {
				storetests.ShouldGet(t, ds, key, []byte(key))
			}
		}

		checkSplitCountSizes(t, "deletion", tmpDir, 2, 8)
	}
}
