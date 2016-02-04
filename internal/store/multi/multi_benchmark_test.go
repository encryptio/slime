package multi

import (
	"math/rand"
	"testing"

	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/store/storetests"
)

func BenchmarkMultiGet6Way1MB(b *testing.B) {
	benchMultiGet(b, 6, 1024*1024, false)
}

func BenchmarkMultiGet6Way1MBNoVerify(b *testing.B) {
	benchMultiGet(b, 6, 1024*1024, true)
}

func BenchmarkMultiGet6Way50MB(b *testing.B) {
	benchMultiGet(b, 6, 50*1024*1024, false)
}

func BenchmarkMultiGet6Way50MBNoVerify(b *testing.B) {
	benchMultiGet(b, 6, 50*1024*1024, true)
}

func benchMultiGet(b *testing.B, width int, size int, noverify bool) {
	_, multi, _, done := prepareMultiTest(b, width, width+2, width+2)
	defer done()

	value := make([]byte, size)
	for i := range value {
		value[i] = byte(rand.Int())
	}
	storetests.ShouldCAS(b, multi, "key", store.MissingV, store.DataV(value))

	b.SetBytes(int64(size))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := multi.Get("key", store.GetOptions{NoVerify: noverify})
		if err != nil {
			b.Fatalf("Couldn't Get key: %v", err)
		}
	}
	b.StopTimer() // done() should not be benchmarked
}
