package rs

import (
	"math/rand"
	"reflect"
	"testing"

	"git.encryptio.com/slime/internal/rs/gf"
)

func genRandomGFVector(l int) []uint32 {
	v := make([]uint32, l)
	for i := range v {
		for {
			v[i] = rand.Uint32()
			if v[i] < gf.MaxVal {
				break
			}
		}
	}
	return v
}

func TestParityData(t *testing.T) {
	tests := []struct {
		Data  [][]uint32
		Index int
		Out   []uint32
	}{
		{
			Data: [][]uint32{
				[]uint32{0, 0, 0},
				[]uint32{1, 2, 3},
			},
			Index: 0,
			Out:   []uint32{0, 0, 0},
		},
		{
			Data: [][]uint32{
				[]uint32{0, 0, 0},
				[]uint32{1, 2, 3},
			},
			Index: 1,
			Out:   []uint32{1, 2, 3},
		},
		{
			Data: [][]uint32{
				[]uint32{0, 0, 0},
				[]uint32{1, 2, 3},
			},
			Index: 2,
			Out:   []uint32{3, 6, 9},
		},
	}

	for _, test := range tests {
		out := CreateParity(test.Data, test.Index, nil)
		if !reflect.DeepEqual(out, test.Out) {
			t.Errorf("CreateParity(%v, %v, nil) = %v, wanted %v",
				test.Data, test.Index, out, test.Out)
		}
	}
}

func TestParityRecovery(t *testing.T) {
	for i := 1; i < 10; i++ {
		data := make([][]uint32, rand.Intn(20))
		if len(data) == 0 {
			continue
		}

		for j := range data {
			data[j] = genRandomGFVector(i)
		}

		var parity [][]uint32
		for j := 0; j < rand.Intn(20); j++ {
			parity = append(parity, CreateParity(data, len(data)+j, nil))
		}

		// mark len(data) things as "have"
		have := make([]bool, len(data)+len(parity))
		for i := 0; i < len(data); i++ {
			for {
				idx := rand.Intn(len(have))
				if !have[idx] {
					have[idx] = true
					break
				}
			}
		}

		var recoveryInput [][]uint32
		var recoveryIndices []int
		for i, h := range have {
			if !h {
				continue
			}

			recoveryIndices = append(recoveryIndices, i)
			if i < len(data) {
				recoveryInput = append(recoveryInput, data[i])
			} else {
				recoveryInput = append(recoveryInput, parity[i-len(data)])
			}
		}

		recovered := RecoverData(recoveryInput, recoveryIndices)
		if !reflect.DeepEqual(recovered, data) {
			t.Errorf("Couldn't recover")
		}
	}
}

func BenchmarkGeneration64k(b *testing.B) {
	data := make([][]uint32, 4)
	for i := range data {
		data[i] = genRandomGFVector(16384)
	}
	b.SetBytes(int64(len(data[0]) * len(data)))

	out := CreateParity(data, len(data), nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = CreateParity(data, len(data), out)
	}
}

func BenchmarkRecovery64k(b *testing.B) {
	data := make([][]uint32, 4)
	for i := range data {
		data[i] = genRandomGFVector(16384)
	}
	b.SetBytes(int64(len(data[0]) * len(data)))

	parity := CreateParity(data, len(data), nil)

	input := make([][]uint32, 4)
	copy(input, data)
	input[0] = parity

	indices := []int{4, 1, 2, 3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RecoverData(input, indices)
	}
}
