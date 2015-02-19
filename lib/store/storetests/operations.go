package storetests

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	"git.encryptio.com/slime/lib/store"
)

func ShouldList(t *testing.T, s store.Store, after string, limit int, expect []string) {
	got, err := s.List(after, limit)
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

func ShouldCASError(t *testing.T, s store.Store, key string, from, to store.CASV, wantErr error) {
	err := s.CAS(key, from, to)
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
	got, gothash, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	wantHash := sha256.Sum256(data)

	if !bytes.Equal(got, data) || gothash != wantHash {
		t.Errorf("Get(%#v) = (%#v, %#v), but wanted (%#v, %#v)",
			key, got, hex.EncodeToString(gothash[:]), data, hex.EncodeToString(wantHash[:]))
	}
}

func ShouldGetError(t *testing.T, s store.Store, key string, wantErr error) {
	got, _, err := s.Get(key)
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func ShouldGetMiss(t *testing.T, s store.Store, key string) {
	ShouldGetError(t, s, key, store.ErrNotFound)
}

func ShouldFreeSpace(t *testing.T, s store.Store) {
	free, err := s.FreeSpace()
	if err != nil {
		t.Errorf("FreeSpace() returned unexpected error %v", err)
		return
	}
	if free <= 0 {
		t.Errorf("FreeSpace() returned nonpositive %v", free)
	}
}

func ShouldStat(t *testing.T, s store.Store, key string, stat *store.Stat) {
	st, err := s.Stat(key)
	if err != nil {
		t.Errorf("Stat(%#v) returned unexpected error %v", key, err)
		return
	}
	if !reflect.DeepEqual(st, stat) {
		t.Errorf("Stat(%#v) = %#v, but wanted %#v", key, st, stat)
	}
}

func ShouldStatMiss(t *testing.T, s store.Store, key string) {
	st, err := s.Stat(key)
	if err != store.ErrNotFound {
		t.Errorf("Stat(%#v) returned (%v, %v), but wanted %v",
			key, st, err, store.ErrNotFound)
	}
}