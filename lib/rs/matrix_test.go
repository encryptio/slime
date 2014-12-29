package rs

import (
	"reflect"
	"testing"
)

func TestVandermondeMatrix(t *testing.T) {
	tests := []struct {
		D, P int
		M    [][]uint32
	}{
		{
			D: 4,
			P: 0,
			M: [][]uint32{
				[]uint32{1, 1, 1, 1},
				[]uint32{1, 2, 3, 4},
				[]uint32{1, 4, 9, 16},
				[]uint32{1, 8, 27, 64},
			},
		},
		{
			D: 3,
			P: 1,
			M: [][]uint32{
				[]uint32{1, 1, 1},
				[]uint32{1, 2, 3},
				[]uint32{1, 4, 9},
				[]uint32{1, 8, 27},
			},
		},
		{
			D: 3,
			P: 4,
			M: [][]uint32{
				[]uint32{1, 1, 1},
				[]uint32{1, 2, 3},
				[]uint32{1, 4, 9},
				[]uint32{1, 8, 27},
				[]uint32{1, 16, 81},
				[]uint32{1, 32, 243},
				[]uint32{1, 64, 729},
			},
		},
	}

	for _, test := range tests {
		mat := vandermondeMatrix(test.D, test.P)
		if !reflect.DeepEqual(mat, test.M) {
			t.Errorf("vandermondeMatrix(%v, %v) = %v, wanted %v", test.D, test.P, mat, test.M)
		}
	}
}

func TestParityMatrix(t *testing.T) {
	tests := []struct {
		D, P int
		M    [][]uint32
	}{
		{
			D: 4,
			P: 0,
			M: [][]uint32{
				[]uint32{1, 0, 0, 0},
				[]uint32{0, 1, 0, 0},
				[]uint32{0, 0, 1, 0},
				[]uint32{0, 0, 0, 1},
			},
		},
		{
			D: 4,
			P: 1,
			M: [][]uint32{
				[]uint32{1, 0, 0, 0},
				[]uint32{0, 1, 0, 0},
				[]uint32{0, 0, 1, 0},
				[]uint32{0, 0, 0, 1},
				[]uint32{4294967267, 50, 4294967256, 10},
			},
		},
		{
			D: 7,
			P: 10,
			M: [][]uint32{
				[]uint32{1, 0, 0, 0, 0, 0, 0},
				[]uint32{0, 1, 0, 0, 0, 0, 0},
				[]uint32{0, 0, 1, 0, 0, 0, 0},
				[]uint32{0, 0, 0, 1, 0, 0, 0},
				[]uint32{0, 0, 0, 0, 1, 0, 0},
				[]uint32{0, 0, 0, 0, 0, 1, 0},
				[]uint32{0, 0, 0, 0, 0, 0, 1},
				[]uint32{5040, 4294954223, 13132, 4294960522, 1960, 4294966969, 28},
				[]uint32{141120, 4294606427, 354628, 4294790891, 48111, 4294960235, 462},
				[]uint32{2328480, 4289070995, 5706120, 4292194641, 729120, 4294866638, 5880},
				[]uint32{29635200, 4220455931, 71319864, 4260871691, 8752150, 4293803051, 63987},
				[]uint32{322494480, 3488420375, 765765924, 3933159152, 91318920, 4283115627, 627396},
				[]uint32{3162075840, 713618134, 3137450065, 813889691, 867888021, 4184264699, 5715424},
				[]uint32{3035933214, 1487358955, 2754122155, 3104450628, 3426186149, 3322488784, 49329280},
				[]uint32{3806435613, 2645995824, 739402974, 3851674533, 1005591735, 427059862, 408741333},
				[]uint32{2766985931, 1019038682, 1528067030, 4223222592, 1825803796, 2534868530, 3281882604},
				[]uint32{769286519, 373517494, 2999596516, 35549606, 2852124514, 1621558894, 4233268331},
			},
		},
	}

	for _, test := range tests {
		mat := ParityMatrix(test.D, test.P)
		if !reflect.DeepEqual(mat, test.M) {
			t.Errorf("ParityMatrix(%v, %v) = %v, wanted %v", test.D, test.P, mat, test.M)
		}
	}
}

func testParityMatrixNonSingularPart(t *testing.T, d, p int) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatalf("Matrix for %vx%v is non-singular: %v", d, p, err)
		}
	}()
	mat := ParityMatrix(d, p)

	pick := make([]int, d)
	for i := range pick {
		pick[i] = i
	}

	for {
		newMat := make([][]uint32, d)
		for i := range pick {
			newMat[i] = mat[pick[i]]
		}
		newMat = cloneMatrix(newMat)

		solveSubIdentity(newMat) // panics if the matrix is singular

		// increment pick[] to point to the next subset
		i := len(pick) - 1
		for i >= 0 {
			old := pick[i]
			pick[i]++
			if old < p+i {
				break
			}
			i--
		}
		if i < 0 {
			return
		}
		old := pick[i]
		for j := i; j < d; j++ {
			pick[j] = old + j - i
		}
	}

}

func TestParityMatrixNonSingular(t *testing.T) {
	size := 8
	if testing.Short() {
		size = 5
	}

	for d := 1; d <= size; d++ {
		for p := 0; p <= size; p++ {
			testParityMatrixNonSingularPart(t, d, p)
		}
	}
}
