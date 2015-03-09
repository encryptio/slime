package gf

import (
	"math/rand"
	"testing"
)

func TestMInverseRaise(t *testing.T) {
	for i := 1; i <= 1000; i++ {
		v := uint32(0)
		for v >= MaxVal || v == 0 {
			v = rand.Uint32()
		}

		inv := MInverse(v)
		if (uint64(v)*uint64(inv))%MaxVal != 1 {
			t.Errorf("MInverse(%v) = %v, but %v*%v %% %v = %v",
				v, inv, v, inv, uint32(MaxVal), (v*inv)%MaxVal)
		}

		if inv != Raise(v, MaxVal-2) {
			t.Errorf("Raise(%v, %v) = %v, wanted %v",
				v, uint32(MaxVal-2), Raise(v, MaxVal-2), inv)
		}
	}
}
