package storetests

import (
	"crypto/sha256"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/encryptio/slime/internal/store"
)

func TestStore(t *testing.T, s store.Store) {
	TestStoreBasics(t, s)
	TestStoreList(t, s)
	TestStoreCASCountRace(t, s)
	//TestStoreGoroutineLeaks(t, s) // TODO: unreliable
	if rangeStore, ok := s.(store.RangeReadStore); ok {
		TestStoreRangeRead(t, rangeStore)
	}
	TestStoreWriteTime(t, s)
}

func TestStoreBasics(t *testing.T, s store.Store) {
	t.Logf("TestStoreBasics()")

	ShouldFullList(t, s, nil)
	ShouldGetMiss(t, s, "hello")
	ShouldCAS(t, s, "hello", store.MissingV, store.MissingV)
	ShouldGetMiss(t, s, "hello")
	ShouldCAS(t, s, "hello", store.MissingV, store.DataV([]byte("world")))
	ShouldGet(t, s, "hello", []byte("world"))
	ShouldStatNoTime(t, s, "hello", store.Stat{SHA256: sha256.Sum256([]byte("world")), Size: 5})
	ShouldCAS(t, s, "a", store.AnyV, store.MissingV)
	ShouldFullList(t, s, []string{"hello"})
	ShouldCAS(t, s, "b", store.AnyV, store.DataV([]byte("beta")))
	ShouldCAS(t, s, "b", store.AnyV, store.DataV([]byte("other")))
	ShouldGet(t, s, "b", []byte("other"))
	ShouldFullList(t, s, []string{"b", "hello"})
	ShouldCASFail(t, s, "b", store.MissingV, store.MissingV)
	ShouldCAS(t, s, "hello", store.AnyV, store.MissingV)
	ShouldFullList(t, s, []string{"b"})
	ShouldCAS(t, s, "empty", store.AnyV, store.DataV(nil))
	ShouldGet(t, s, "empty", nil)
	ShouldCAS(t, s, "empty", store.DataV(nil), store.DataV([]byte("one")))
	ShouldCAS(t, s, "empty", store.DataV([]byte("one")), store.DataV([]byte("two")))
	ShouldGet(t, s, "empty", []byte("two"))

	ShouldCAS(t, s, "b", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "empty", store.AnyV, store.MissingV)
	ShouldFullList(t, s, nil)

	ShouldFreeSpace(t, s)
}

func TestStoreGoroutineLeaks(t *testing.T, s store.Store) {
	t.Logf("TestStoreGoroutineLeaks()")

	// do the operations twice, only check goroutine counts on the second iteration
	// the implementation may start helper routines, which are not an issue unless
	// they leak for each operation.
	for i := 0; i < 2; i++ {
		routinesAtStart := runtime.NumGoroutine()

		ShouldCAS(t, s, "hello", store.MissingV, store.DataV([]byte("world")))
		ShouldGet(t, s, "hello", []byte("world"))
		ShouldFullList(t, s, []string{"hello"})
		ShouldFreeSpace(t, s)
		ShouldCAS(t, s, "hello", store.AnyV, store.MissingV)

		if i == 1 {
			routinesAtEnd := runtime.NumGoroutine()

			if routinesAtStart != routinesAtEnd {
				t.Errorf("Had %v goroutines running, wanted %v", routinesAtEnd, routinesAtStart)
			}
		}
	}
}

func TestStoreList(t *testing.T, s store.Store) {
	t.Logf("TestStoreList()")

	ShouldFullList(t, s, nil)
	ShouldCAS(t, s, "a", store.MissingV, store.DataV([]byte("a")))
	ShouldCAS(t, s, "x", store.MissingV, store.DataV([]byte("x")))
	ShouldCAS(t, s, "z", store.MissingV, store.DataV([]byte("z")))
	ShouldCAS(t, s, "c", store.MissingV, store.DataV([]byte("c")))
	ShouldCAS(t, s, "b", store.MissingV, store.DataV([]byte("b")))
	ShouldCAS(t, s, "y", store.MissingV, store.DataV([]byte("y")))
	ShouldList(t, s, "", 1, []string{"a"})
	ShouldList(t, s, "", 3, []string{"a", "b", "c"})
	ShouldList(t, s, "a", 3, []string{"b", "c", "x"})
	ShouldList(t, s, "c", 5, []string{"x", "y", "z"})
	ShouldList(t, s, "z", 3, nil)
	ShouldCAS(t, s, "a", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "b", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "c", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "x", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "y", store.AnyV, store.MissingV)
	ShouldCAS(t, s, "z", store.AnyV, store.MissingV)
	ShouldFullList(t, s, nil)
}

func TestStoreCASCountRace(t *testing.T, s store.Store) {
	t.Logf("TestStoreCASCountRace()")

	const (
		goroutines = 4
		iterations = 15
	)

	ShouldFullList(t, s, nil)

	ShouldCAS(t, s, "key", store.AnyV, store.DataV([]byte("0")))

	errs := make(chan error)
	casFailures := uint64(0)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			for j := 0; j < iterations; j++ {
				for {
					data, st, err := s.Get("key", store.GetOptions{})
					if err != nil {
						t.Logf("Routine %v: Couldn't get key: %v", i, err)
						errs <- err
						return
					}

					num, err := strconv.ParseInt(string(data), 10, 64)
					if err != nil {
						t.Logf("Routine %v: Couldn't parse int: %v", i, err)
						errs <- err
						return
					}

					num++

					data = strconv.AppendInt(data[:0], num, 10)

					err = s.CAS("key",
						store.CASV{Present: true, SHA256: st.SHA256},
						store.DataV(data),
						nil)
					if err != nil {
						if err == store.ErrCASFailure {
							atomic.AddUint64(&casFailures, 1)
							continue
						}
						t.Logf("Routine %v: Couldn't cas: %v", i, err)
						errs <- err
						return
					}

					break
				}
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < goroutines; i++ {
		err := <-errs
		if err != nil {
			t.Errorf("Got error from goroutine: %v", err)
		}
	}

	t.Logf("%v cas failures", casFailures)

	ShouldGet(t, s, "key", []byte(strconv.FormatInt(goroutines*iterations, 10)))
	ShouldCAS(t, s, "key", store.AnyV, store.MissingV)
}

func TestStoreRangeRead(t *testing.T, s store.RangeReadStore) {
	t.Logf("TestStoreRangeRead()")

	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(rand.Int31())
	}

	ShouldFullList(t, s, nil)
	ShouldCAS(t, s, "key", store.AnyV, store.DataV(data))
	ShouldGet(t, s, "key", data)
	ShouldGetPartial(t, s, "key", 0, len(data), data)
	ShouldGetPartial(t, s, "key", 1, len(data), data[1:])
	ShouldGetPartial(t, s, "key", 0, -1, data)
	ShouldGetPartial(t, s, "key", 1, -1, data[1:])
	ShouldGetPartial(t, s, "key", 128, -1, data[128:])
	ShouldGetPartial(t, s, "key", 128, 128, data[128:256])
	ShouldGetPartial(t, s, "key", 555, 1, data[555:556])
	ShouldGetPartial(t, s, "key", 1020, -1, data[1020:])
	ShouldGetPartial(t, s, "key", 1023, -1, data[1023:])
	ShouldGetPartial(t, s, "key", 1024, -1, nil)
	ShouldGetPartial(t, s, "key", 1023, 1, data[1023:])
	ShouldGetPartial(t, s, "key", 1024, 1, nil)
	ShouldGetPartial(t, s, "key", 1023, 0, nil)
	ShouldGetPartial(t, s, "key", 1024, 0, nil)
	ShouldGetPartial(t, s, "key", 5555, -1, nil)
	ShouldGetPartial(t, s, "key", 1000, 60, data[1000:])
	ShouldCAS(t, s, "key", store.AnyV, store.MissingV)
}

func TestStoreWriteTime(t *testing.T, s store.Store) {
	t.Logf("TestStoreWriteTime()")

	ShouldCAS(t, s, "key", store.AnyV, store.DataV([]byte("one")))
	now := time.Now().Unix()

	st, err := s.Stat("key", nil)
	if err != nil {
		t.Fatalf("Couldn't stat key: %v", err)
	}
	diff := st.WriteTime - now
	if diff < 0 {
		diff = -diff
	}
	if diff > 2 {
		t.Fatalf("Store returned timestamp %v, but wanted %v", st.WriteTime, now)
	}

	ShouldCAS(t, s, "key", store.AnyV, store.MissingV)
}
