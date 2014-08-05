package rs

import (
	"git.encryptio.com/slime/lib/gf"
)

// CreateParity takes a slice of data chunks (in GF(2^32-5)), a total number of
// parity blocks, and target index to generate.
//
// The first len(data) indexes are exactly the data blocks, so it's only useful
// to call this function with index >= len(data).
//
// The data chunks must be of the same length, and the parity output will also
// be of that length.
//
// You may optionally passed an output slice, in which case it will be reused if
// it is large enough.
func CreateParity(data [][]uint32, index int, out []uint32) []uint32 {
	for i := 1; i < len(data); i++ {
		if len(data[i]) != len(data[0]) {
			panic("CreateParity called on data chunks of varying length")
		}
	}

	if cap(out) < len(data[0]) {
		out = make([]uint32, len(data[0]))
	} else {
		out = out[:len(data[0])]
	}

	p := 0
	if index >= len(data) {
		p = index - len(data) + 1
	}

	mat := ParityMatrixCached(len(data), p)

	applyMatrix([][]uint32{mat[index]}, data, [][]uint32{out})

	return out
}

// RecoverData takes a slice of len(data) chunks (whose content is data or
// parity) as well as a slice of indices (see docs on CreateParity for what the
// indices mean.)
//
// There must be exactly len(data) input chunks, as this is used to calculate
// the width of the recovery matrix. (Also, more chunks wouldn't help and less
// chunks would be unrecoverable.)
func RecoverData(chunks [][]uint32, indices []int) [][]uint32 {
	if len(chunks) != len(indices) {
		panic("RecoverData: len(chunks) != len(indices)")
	}

	if len(chunks) == 0 {
		panic("RecoverData: len(chunks) == 0")
	}

	maxIndex := -1
	for _, index := range indices {
		if index > maxIndex {
			maxIndex = index
		}
	}
	if maxIndex == -1 {
		panic("RecoverData: No indices given")
	}

	mat := ParityMatrixCached(len(chunks), maxIndex)

	// gather the rows we have and find their inverse to the data rows
	have := make([][]uint32, len(chunks))
	for i, index := range indices {
		have[i] = mat[index]
	}

	inv := invertMatrix(have)

	// create empty data output chunks
	data := make([][]uint32, len(chunks))
	for i := range data {
		data[i] = make([]uint32, len(chunks[0]))
	}

	applyMatrix(inv, chunks, data)

	return data
}

func applyMatrix(mat [][]uint32, in, out [][]uint32) {
	// TODO(encryptio): optimize with high/low split, as described in http://www.lshift.net/blog/2006/11/29/gf232-5
	for i := 0; i < len(out); i++ {
		for b := 0; b < len(out[i]); b++ {
			var o uint64
			for j := 0; j < len(in); j++ {
				o = ((uint64(in[j][b])*uint64(mat[i][j]))%gf.MaxVal + o) % gf.MaxVal
			}
			out[i][b] = uint32(o)
		}
	}
}
