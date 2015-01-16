package gf

import (
	"math/rand"
)

const MaxVal = 1<<32 - 5

// MapToGF maps a byte slice to a sequence of points in GF(2^32-5),
// along with a number that represents the particular mapping used.
//
// This is only possible for byte slices strictly shorter than 4*(2^32-5) bytes,
// and becomes significantly slower as larger data points are used. If you need
// to map such a large byte slice, chunk it into smaller pieces.
func MapToGF(in []byte) (uint32, []uint32) {
	outLength := (len(in) + 3) / 4

	extra := in[(len(in)/4)*4:]
	in = in[:len(in)-len(extra)]

	out := make([]uint32, outLength)
	for i := 0; i < len(in)/4; i++ {
		out[i] = uint32(in[i*4])<<24 +
			uint32(in[i*4+1])<<16 +
			uint32(in[i*4+2])<<8 +
			uint32(in[i*4+3])
	}
	if len(extra) > 0 {
		out[len(out)-1] = 0
		for i := uint(0); i < uint(len(extra)); i++ {
			out[len(out)-1] += uint32(extra[i]) << ((3 - i) * 8)
		}
	}

	zeroIsOkay := true
	for _, v := range out {
		if v >= MaxVal {
			zeroIsOkay = false
			break
		}
	}

	if zeroIsOkay {
		return 0, out
	}

	outn := uint32(1 << 31) // try just switching the high bit first
	for {
		okay := true
		for _, v := range out {
			if v^outn >= MaxVal {
				okay = false
				break
			}
		}

		if okay {
			for i := range out {
				out[i] ^= outn
			}
			return outn, out
		}

		// this one didn't work, try a random new one
		outn = rand.Uint32()
	}
}

// MapToGFWith maps an arbitrary byte slice to a sequence of points in
// GF(2^32-5), using a mapping defined by n, previously chosen by MapToGF.
//
// Note that the output data may be padded with up to 3 zero bytes to the next
// multiple of 4 bytes.
func MapToGFWith(in []byte, n uint32) []uint32 {
	outLength := (len(in) + 3) / 4

	extra := in[(len(in)/4)*4:]
	in = in[:len(in)-len(extra)]

	out := make([]uint32, outLength)
	for i := 0; i < len(in)/4; i++ {
		out[i] = uint32(in[i*4])<<24 +
			uint32(in[i*4+1])<<16 +
			uint32(in[i*4+2])<<8 +
			uint32(in[i*4+3])
	}
	if len(extra) > 0 {
		out[len(out)-1] = 0
		for i := uint(0); i < uint(len(extra)); i++ {
			out[len(out)-1] += uint32(extra[i]) << ((3 - i) * 8)
		}
	}

	for i := range out {
		out[i] ^= n
	}
	return out
}

// MapFromGF maps a mapping number and sequence of points from MapToGF back to
// a byte slice. Note that the output will be padded with zeroes up to the next
// multiple of 4 bytes.
func MapFromGF(inn uint32, inv []uint32) []byte {
	out := make([]byte, len(inv)*4)
	for i := range inv {
		w := inv[i] ^ inn
		out[i*4] = byte(w >> 24)
		out[i*4+1] = byte(w >> 16)
		out[i*4+2] = byte(w >> 8)
		out[i*4+3] = byte(w)
	}
	return out
}
