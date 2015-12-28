package gf

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestMapTrivial(t *testing.T) {
	tests := []struct {
		In   []byte
		OutN uint32
		OutV []uint32
	}{
		{[]byte{0}, 0, []uint32{0}},
		{[]byte{1}, 0, []uint32{1 << 24}},
		{[]byte{0, 1}, 0, []uint32{1 << 16}},
		{[]byte{0, 0, 1}, 0, []uint32{1 << 8}},
		{[]byte{0, 0, 0, 1}, 0, []uint32{1}},
		{[]byte{0, 0, 0, 0, 1}, 0, []uint32{0, 1 << 24}},
		{[]byte{0, 0, 0, 0, 0, 1}, 0, []uint32{0, 1 << 16}},
		{[]byte{0, 0, 0, 0, 0, 0, 1}, 0, []uint32{0, 1 << 8}},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, 0, []uint32{0, 1}},
		{[]byte{0, 0, 0, 0, 0, 0, 1, 0}, 0, []uint32{0, 1 << 8}},
		{[]byte{0, 0, 0, 0, 0, 1, 0, 0}, 0, []uint32{0, 1 << 16}},
		{[]byte{0, 0, 0, 0, 1, 0, 0, 0}, 0, []uint32{0, 1 << 24}},
		{[]byte{0, 0, 0, 1, 0, 0, 0, 0}, 0, []uint32{1, 0}},
		{[]byte{0, 0, 1, 0, 0, 0, 0, 0}, 0, []uint32{1 << 8, 0}},
		{[]byte{0, 1, 0, 0, 0, 0, 0, 0}, 0, []uint32{1 << 16, 0}},
		{[]byte{1, 0, 0, 0, 0, 0, 0, 0}, 0, []uint32{1 << 24, 0}},
		{[]byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0,
			[]uint32{0xFF000000, 0x00000000}},
		{[]byte{0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0,
			[]uint32{0xFFFF0000, 0x00000000}},
		{[]byte{0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00}, 0,
			[]uint32{0xFFFFFF00, 0x00000000}},
		{[]byte{0x0F, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00}, 0,
			[]uint32{0x0FFFFFFF, 0x00000000}},
		{[]byte{0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00}, 0,
			[]uint32{0x0FFFFFFF, 0xFF000000}},
		{[]byte{0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00}, 0,
			[]uint32{0x0FFFFFFF, 0xFFFF0000}},
		{[]byte{0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00}, 0,
			[]uint32{0x0FFFFFFF, 0xFFFFFF00}},
		{[]byte{0x0F, 0xFF, 0xFF, 0xFF, 0x0F, 0xFF, 0xFF, 0xFF}, 0,
			[]uint32{0x0FFFFFFF, 0x0FFFFFFF}},
		{[]byte{0xFF, 0xFF, 0xFF, 0xFF}, 1 << 31, []uint32{0x7FFFFFFF}},
	}

	for _, test := range tests {
		gotn, gotv := MapToGF(test.In)
		if gotn != test.OutN {
			t.Errorf("MapToGF(%v)[0] = %v, wanted %v", test.In, gotn, test.OutN)
		}

		same := len(gotv) == len(test.OutV)
		if same {
			for i := 0; i < len(gotv); i++ {
				if gotv[i] != test.OutV[i] {
					same = false
					break
				}
			}
		}
		if !same {
			t.Errorf("MapToGF(%v)[1] = %v, wanted %v", test.In, gotv, test.OutV)
		}

		remapped := MapFromGF(gotn, gotv)
		remapped = remapped[:len(test.In)]
		if !bytes.Equal(test.In, remapped) {
			t.Errorf("MapFromGF(%v,%v) = %v, wanted %v",
				gotn, gotv, remapped, test.In)
		}
	}
}

func TestMapTricky(t *testing.T) {
	tests := [][]byte{
		[]byte{0xFF, 0xFF, 0xFF, 0xFB},
		[]byte{0xFF, 0xFF, 0xFF, 0xFC},
		[]byte{0xFF, 0xFF, 0xFF, 0xFD},
		[]byte{0xFF, 0xFF, 0xFF, 0xFE},
		[]byte{0xFF, 0xFF, 0xFF, 0xFF},
		[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF},
	}

	for _, test := range tests {
		gotn, gotv := MapToGF(test)
		for i := 0; i < len(gotv); i++ {
			if gotv[i] >= MaxVal {
				t.Errorf("MapToGF(%v) = %v,%v, but element %v is %v, which is over %v",
					test, gotn, gotv, i, gotv[i], uint32(MaxVal))
				continue
			}
		}

		remapped := MapFromGF(gotn, gotv)
		remapped = remapped[:len(test)]
		if !bytes.Equal(test, remapped) {
			t.Errorf("MapFromGF(%v,%v) = %v, wanted %v",
				gotn, gotv, remapped, test)
		}
	}
}

func BenchmarkMapToGF64kWorst(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(rand.Int31())
	}

	copy(data[len(data)-8:],
		[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF})

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapToGF(data)
	}
}

func BenchmarkMapToGF64kBest(b *testing.B) {
	data := make([]byte, 65536)
	for {
		for i := range data {
			data[i] = byte(rand.Int31())
		}

		n, _ := MapToGF(data)
		if n == 0 {
			break
		}
	}
	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapToGF(data)
	}
}
