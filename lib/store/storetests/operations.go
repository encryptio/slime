package storetests

import (
	"bytes"
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

func ShouldFullList(t *testing.T, s store.Store, expect []string) {
	ShouldList(t, s, "", 0, expect)
}

func ShouldSet(t *testing.T, s store.Store, key string, data []byte) {
	err := s.Set(key, data)
	if err != nil {
		t.Errorf("Set(%#v, %#v) returned unexpected error %v", key, data, err)
	}
}

func ShouldGet(t *testing.T, s store.Store, key string, data []byte) {
	got, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%#v) returned unexpected error %v", key, err)
		return
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Get(%#v) = %#v, but wanted %#v", key, got, data)
	}
}

func ShouldGetError(t *testing.T, s store.Store, key string, wantErr error) {
	got, err := s.Get(key)
	if err != wantErr {
		t.Errorf("Get(%#v) = (%#v, %v), but wanted err = %v",
			key, got, err, wantErr)
	}
}

func ShouldGetMiss(t *testing.T, s store.Store, key string) {
	ShouldGetError(t, s, key, store.ErrNotFound)
}

func ShouldDelete(t *testing.T, s store.Store, key string) {
	err := s.Delete(key)
	if err != nil {
		t.Errorf("Delete(%#v) returned unexpected error %v", key, err)
	}
}

func ShouldDeleteMiss(t *testing.T, s store.Store, key string) {
	err := s.Delete(key)
	if err != store.ErrNotFound {
		t.Errorf("Delete(%#v) = %v, but wanted %v", key, err, store.ErrNotFound)
	}
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

func ShouldGetWith256(t *testing.T, s store.Store, key string, data []byte, sha [32]byte) {
	got, gothash, err := s.GetWith256(key)
	if err != nil {
		t.Errorf("GetWith256(%#v) returned unexpected error %v", key, err)
		return
	}

	if !bytes.Equal(got, data) || gothash != sha {
		t.Errorf("GetWith256(%#v) = (%#v, %#v), but wanted (%#v, %#v)",
			key, got, hex.EncodeToString(gothash[:]), data, hex.EncodeToString(sha[:]))
	}
}

func ShouldSetWith256(t *testing.T, s store.Store, key string, data []byte, sha [32]byte) {
	err := s.SetWith256(key, data, sha)
	if err != nil {
		t.Errorf("SetWith256(%#v, %#v, %#v) returned unexpected error %v",
			key, data, sha, err)
	}
}

func ShouldCASWith256(t *testing.T, s store.Store, key string, oldH [32]byte, data []byte, newH [32]byte) {
	err := s.CASWith256(key, oldH, data, newH)
	if err != nil {
		t.Errorf("CASWith256(%#v, %#v, %#v, %#v) returned unexpected error %v",
			key, oldH, data, newH, err)
	}
}

func ShouldCASWith256Error(t *testing.T, s store.Store, key string, oldH [32]byte, data []byte, newH [32]byte, wantErr error) {
	err := s.CASWith256(key, oldH, data, newH)
	if err != wantErr {
		t.Errorf("CASWith256(%#v, %#v, %#v, %#v) returned %v, but wanted %v",
			key, oldH, data, newH, err, wantErr)
	}
}

func ShouldCASWith256Fail(t *testing.T, s store.Store, key string, oldH [32]byte, data []byte, newH [32]byte) {
	ShouldCASWith256Error(t, s, key, oldH, data, newH, store.ErrCASFailure)
}
