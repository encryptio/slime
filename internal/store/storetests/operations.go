package storetests

import (
	"bytes"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/encryptio/slime/internal/store"
)

func ShouldList(t *testing.T, s store.Store, after string, limit int, expect []string) {
	got, err := s.List(after, limit, nil)
	if err != nil {
		t.Errorf("Unexpected error from List(%#v, %v): %v", after, limit, err)
		return
	}

	if !((len(got) == 0 && len(expect) == 0) ||
		reflect.DeepEqual(got, expect)) {

		t.Errorf("List(%#v, %v) = %#v, but wanted %#v",
			after, limit, got, expect)
	}
}

func ShouldListCount(t *testing.T, s store.Store, count int) {
	actualCount := 0

	from := ""
	for {
		list, err := s.List(from, 100, nil)
		if err != nil {
			t.Errorf("Couldn't List(%#v, 100): %v", from, err)
			return
		}

		actualCount += len(list)
		if len(list) < 100 {
			break
		}
	}

	if actualCount != count {
		t.Errorf("Full list returned %v elements but wanted %v", actualCount, count)
	}
}

func ShouldCASError(t *testing.T, s store.Store, key string, from, to store.CASV, wantErr error) {
	err := s.CAS(key, from, to, nil)
	if err != wantErr {
		t.Errorf("CAS(%#v, %v, %v) returned error %v, but wanted %v",
			key, from, to, err, wantErr)
	}
}

func ShouldCAS(t *testing.T, s store.Store, key string, from, to store.CASV) {
	ShouldCASError(t, s, key, from, to, nil)
}

func ShouldCASFail(t *testing.T, s store.Store, key string, from, to store.CASV) {
	ShouldCASError(t, s, key, from, to, store.ErrCASFailure)
}

func ShouldFullList(t *testing.T, s store.Store, expect []string) {
	ShouldList(t, s, "", 0, expect)
}

func ShouldGet(t *testing.T, s store.Store, key string, data []byte) {
	got, st, err := s.Get(key, store.GetOptions{})
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	wantStat := store.Stat{
		SHA256: sha256.Sum256(data),
		Size:   int64(len(got)),
	}

	if !bytes.Equal(got, data) || st != wantStat {
		t.Errorf("Get(%#v) = (%#v, %#v), but wanted (%#v, %#v)",
			key, got, st, data, wantStat)
	}
}

func ShouldGetError(t *testing.T, s store.Store, key string, wantErr error) {
	got, _, err := s.Get(key, store.GetOptions{})
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func ShouldGetMiss(t *testing.T, s store.Store, key string) {
	ShouldGetError(t, s, key, store.ErrNotFound)
}

func ShouldFreeSpace(t *testing.T, s store.Store) {
	free, err := s.FreeSpace(nil)
	if err != nil {
		t.Errorf("FreeSpace() returned unexpected error %v", err)
		return
	}
	if free <= 0 {
		t.Errorf("FreeSpace() returned nonpositive %v", free)
	}
}

func ShouldStat(t *testing.T, s store.Store, key string, stat store.Stat) {
	st, err := s.Stat(key, nil)
	if err != nil {
		t.Errorf("Stat(%#v) returned unexpected error %v", key, err)
		return
	}
	if !reflect.DeepEqual(st, stat) {
		t.Errorf("Stat(%#v) = %#v, but wanted %#v", key, st, stat)
	}
}

func ShouldStatMiss(t *testing.T, s store.Store, key string) {
	st, err := s.Stat(key, nil)
	if err != store.ErrNotFound {
		t.Errorf("Stat(%#v) returned (%v, %v), but wanted %v",
			key, st, err, store.ErrNotFound)
	}
}
