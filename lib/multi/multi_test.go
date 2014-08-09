package multi

import (
	"bytes"
	"fmt"
	"git.encryptio.com/slime/lib/store"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func shouldGet(t *testing.T, m *Multi, path string, want []byte) {
	res, err := m.Get(path)
	if err != nil {
		t.Fatalf("Get(%v) returned unexpected error %v", path, err)
	} else if !bytes.Equal(res.Data, want) {
		t.Errorf("Get(%v) = %v, wanted %v", path, res.Data, want)
	}
}

func shouldSet(t *testing.T, m *Multi, path string, data []byte) {
	err := m.Set(path, data)
	if err != nil {
		t.Fatalf("Set(%v) returned unexpected error %v", path, err)
	}
}

func shouldNotGet(t *testing.T, m *Multi, path string, wantErr error) {
	_, err := m.Get(path)
	if err != wantErr {
		t.Errorf("Get(%v) returned error %v, wanted %v", path, err, wantErr)
	}
}

func shouldList(t *testing.T, m *Multi, prefix string, wantList []store.FileInfo) {
	haveList, err := m.List(prefix)
	if err != nil {
		t.Errorf("List(%v) returned unexpected error %v", prefix, err)
		return
	}

	want := make(map[store.FileInfo]struct{})
	for _, v := range wantList {
		want[v] = struct{}{}
	}

	have := make(map[store.FileInfo]struct{})
	for _, v := range haveList {
		have[v] = struct{}{}
	}

	if !reflect.DeepEqual(want, have) {
		t.Errorf("List(%v) = %v, wanted %v", prefix, haveList, wantList)
	}
}

func switchSize(t *testing.T, old *Multi, targets []store.Target, count int) *Multi {
	if old != nil {
		old.Stop()
	}

	subset := make([]store.Target, 0, count)
	for len(subset) < count {
		chosen := targets[rand.Intn(len(targets))]
		have := false
		for _, tgt := range subset {
			if chosen == tgt {
				have = true
				break
			}
		}

		if !have {
			subset = append(subset, chosen)
		}
	}

	m, err := New(targets)
	if err != nil {
		t.Fatalf("Unexpected error from New(): %v", err)
	}

	return m
}

func testData(t *testing.T, targets []store.Target) {
	m := switchSize(t, nil, targets, 5)
	shouldList(t, m, "/", []store.FileInfo{})
	shouldSet(t, m, "/a", []byte("hello, world"))
	shouldList(t, m, "/", []store.FileInfo{{"/a", false}})
	shouldGet(t, m, "/a", []byte("hello, world"))
	shouldNotGet(t, m, "/b", ErrNotFound)
	m = switchSize(t, m, targets, 4)
	shouldList(t, m, "/", []store.FileInfo{{"/a", false}})
	shouldGet(t, m, "/a", []byte("hello, world"))
	shouldSet(t, m, "/b", []byte("other"))
	shouldList(t, m, "/", []store.FileInfo{{"/a", false}, {"/b", false}})
	m = switchSize(t, m, targets, 5)
	shouldList(t, m, "/", []store.FileInfo{{"/a", false}, {"/b", false}})
	shouldGet(t, m, "/a", []byte("hello, world"))
	shouldGet(t, m, "/b", []byte("other"))
	shouldSet(t, m, "/dir/one", []byte("file one goes here"))
	shouldSet(t, m, "/dir/two", []byte("file two goes here"))
	shouldSet(t, m, "/dir/three", []byte("file three goes here"))
	shouldList(t, m, "/", []store.FileInfo{{"/a", false}, {"/b", false}, {"/dir", true}})
	shouldList(t, m, "/dir", []store.FileInfo{{"/dir", true}})
	shouldList(t, m, "/dir/", []store.FileInfo{{"/dir/one", false}, {"/dir/two", false}, {"/dir/three", false}})
	shouldSet(t, m, "/a", nil)
	shouldNotGet(t, m, "/a", ErrNotFound)
	shouldList(t, m, "/", []store.FileInfo{{"/dir", true}, {"/b", false}})
	shouldSet(t, m, "/b", nil)
	shouldList(t, m, "/", []store.FileInfo{{"/dir", true}})
	shouldSet(t, m, "/dir/one", nil)
	shouldList(t, m, "/dir/", []store.FileInfo{{"/dir/two", false}, {"/dir/three", false}})
	shouldSet(t, m, "/dir/two", nil)
	shouldSet(t, m, "/dir/three", nil)
	shouldList(t, m, "/dir/", nil)
	shouldList(t, m, "/", nil)
}

func TestDataRAM(t *testing.T) {
	targets := []store.Target{
		store.NewRAM(),
		store.NewRAM(),
		store.NewRAM(),
		store.NewRAM(),
		store.NewRAM(),
	}

	testData(t, targets)
}

func TestDataFS(t *testing.T) {
	var targets []store.Target
	for i := 0; i < 5; i++ {
		tmpDir := fmt.Sprintf("tmp-%d", rand.Int31())
		err := os.Mkdir(tmpDir, 0700)
		if err != nil {
			t.Fatalf("Couldn't mkdir %v: %v", tmpDir, err)
		}
		defer os.RemoveAll(tmpDir)

		fs, err := store.NewFS(tmpDir)
		if err != nil {
			t.Fatalf("Couldn't NewFS(%v): %v", tmpDir, err)
		}

		targets = append(targets, fs)
	}

	testData(t, targets)
}

func BenchmarkSet1MB(b *testing.B) {
	m, err := New([]store.Target{store.NewRAM(), store.NewRAM(), store.NewRAM(), store.NewRAM()})
	if err != nil {
		b.Fatal(err)
	}

	d := make([]byte, 1024*1024)
	b.SetBytes(int64(len(d)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := m.Set("/path", d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet1MB(b *testing.B) {
	m, err := New([]store.Target{store.NewRAM(), store.NewRAM(), store.NewRAM(), store.NewRAM()})
	if err != nil {
		b.Fatal(err)
	}

	d := make([]byte, 1024*1024)
	b.SetBytes(int64(len(d)))

	err = m.Set("/path", d)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := m.Get("/path")
		if err != nil {
			b.Fatal(err)
		}
	}
}
