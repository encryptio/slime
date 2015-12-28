package gf

// MInverse calculates the multiplicative inverse of the given element of
// GF(2^32-5). In other words, (Inverse(n) * n) % MaxVal = 1.
func MInverse(in uint32) uint32 {
	// Since forall n in G. n^(MaxVal-1) = 1, n^-1 = n^(MaxVal-2)

	// raise in to the power 2^32-7 using unrolled exponentiation by squaring
	// TODO(encryptio): look for a more optimal exponentiation chain
	n := uint64(in)
	o := (((n * n) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal
	o = (o * o) % MaxVal
	o = (o * o) % MaxVal
	o = (((o * o) % MaxVal) * n) % MaxVal

	return uint32(o)
}

func Raise(x, n uint32) uint32 {
	if n == 0 {
		return 1
	}

	if x == 0 || x == 1 {
		return x
	}

	v := Raise(uint32((uint64(x)*uint64(x))%MaxVal), n/2)
	if n%2 == 1 {
		v = uint32((uint64(x) * uint64(v)) % MaxVal)
	}
	return v
}
