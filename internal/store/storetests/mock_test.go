package storetests

import (
	"testing"
	"time"

	"github.com/encryptio/slime/internal/store"
)

func TestMockStoreGeneric(t *testing.T) {
	TestStore(t, NewMockStore(0))
}

func TestMockStoreGenericLimited(t *testing.T) {
	TestStore(t, NewMockStore(1024*1024))
}

func TestMockStoreBlocked(t *testing.T) {
	st := NewMockStore(0)
	st.SetBlocked(true)

	ch := make(chan struct{})
	go func() {
		st.Get("asdf", store.GetOptions{})
		close(ch)
	}()

	select {
	case <-ch:
		t.Error("Get returned early")
	case <-time.After(20 * time.Millisecond):
	}

	st.SetBlocked(false)

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Error("Get didn't unblock")
	}
}
