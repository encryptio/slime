package rs

import (
	"sync"
)

var parityCache = make(map[struct{ d, p int }][][]uint32)
var parityCacheLock sync.RWMutex

// ParityMatrixCached is a memoized version of ParityMatrix.
func ParityMatrixCached(d, p int) [][]uint32 {
	key := struct{ d, p int }{d, p}

	parityCacheLock.RLock()
	v := parityCache[key]
	parityCacheLock.RUnlock()

	if v == nil {
		parityCacheLock.Lock()
		v = parityCache[key]
		if v == nil {
			v = ParityMatrix(d, p)
			parityCache[key] = v
		}
		parityCacheLock.Unlock()
	}

	return v
}
