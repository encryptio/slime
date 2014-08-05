package store

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func shouldSearch(t *testing.T, tg Target, prefix string, expect []FileInfo) {
	res, err := tg.Search(prefix)
	if err != nil {
		t.Errorf("Unexpected error from Search(%#v): %v", prefix, err)
		return
	}

	haveRes := make(map[FileInfo]struct{})
	haveExpect := make(map[FileInfo]struct{})

	for _, s := range res {
		haveRes[s] = struct{}{}
	}

	for _, s := range expect {
		haveExpect[s] = struct{}{}
	}

	if !reflect.DeepEqual(haveRes, haveExpect) {
		t.Errorf("Search(%#v) = %v, wanted %v", prefix, res, expect)
	}
}

func shouldSet(t *testing.T, tg Target, path string, data []byte) {
	err := tg.Set(path, data)
	if err != nil {
		t.Errorf("Unexpected error from Set(%#v, %v): %v", path, data, err)
	}
}

func shouldGet(t *testing.T, tg Target, path string, data []byte) {
	res, err := tg.Get(path)
	if err != nil {
		t.Errorf("Unexpected error from Get(%#v): %v", path, err)
		return
	}

	if !reflect.DeepEqual(res, data) {
		t.Errorf("Get(%#v) = %v, wanted %v", path, res, data)
	}
}

func shouldNotGet(t *testing.T, tg Target, path string) {
	res, err := tg.Get(path)
	if err == nil {
		t.Errorf("Unexpected success from Get(%#v) = %v", path, res)
		return
	}

	if err != ErrItemDoesNotExist {
		t.Errorf("Get(%#v) returned error %v, wanted %v", path, err, ErrItemDoesNotExist)
	}
}

func testStoreGeneric(t *testing.T, tg Target) {
	for _, p := range []string{"", "/"} {
		shouldSearch(t, tg, p, nil)
		shouldSet(t, tg, p+"a1", []byte("hello"))
		shouldGet(t, tg, p+"a1", []byte("hello"))
		shouldNotGet(t, tg, p+"a2")
		shouldSearch(t, tg, p, []FileInfo{{"/a1", false}})
		shouldSet(t, tg, p+"a2", []byte("world"))
		shouldSearch(t, tg, p+"a", []FileInfo{{"/a1", false}, {"/a2", false}})
		shouldSet(t, tg, p+"a1", nil)
		shouldNotGet(t, tg, p+"a1")
		shouldSet(t, tg, p+"b/file.1", []byte("a"))
		shouldSet(t, tg, p+"b/file.2", []byte("b"))
		shouldSet(t, tg, p+"b/other", []byte("c"))
		shouldSearch(t, tg, p, []FileInfo{{"/a2", false}, {"/b", true}})
		shouldSearch(t, tg, p+"b", []FileInfo{{"/b", true}})
		shouldSearch(t, tg, p+"b/", []FileInfo{{"/b/file.1", false}, {"/b/file.2", false}, {"/b/other", false}})
		shouldSearch(t, tg, p+"b/f", []FileInfo{{"/b/file.1", false}, {"/b/file.2", false}})
		shouldSet(t, tg, p+"b/file.1", nil)
		shouldSet(t, tg, p+"b/other", nil)
		shouldSearch(t, tg, p+"b/", []FileInfo{{"/b/file.2", false}})
		shouldSet(t, tg, p+"b/file.2", nil)
		shouldSearch(t, tg, p, []FileInfo{{"/a2", false}})
		shouldSet(t, tg, p+"a2", nil)
		shouldSearch(t, tg, p, nil)
	}
}

func TestRAMStore(t *testing.T) {
	testStoreGeneric(t, NewRAM())
}

func TestFSStore(t *testing.T) {
	tmpDir := fmt.Sprintf("tmp-%d", rand.Int31())
	err := os.Mkdir(tmpDir, 0700)
	if err != nil {
		t.Fatalf("Couldn't mkdir %v: %v", tmpDir, err)
	}
	defer os.RemoveAll(tmpDir)

	fs, err := NewFS(tmpDir)
	if err != nil {
		t.Fatalf("Couldn't NewFS(%v): %v", tmpDir, err)
	}

	testStoreGeneric(t, fs)
}
