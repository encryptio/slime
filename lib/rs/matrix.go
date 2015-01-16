package rs

import (
	"git.encryptio.com/slime/lib/gf"
)

// vandermondeMatrix returns a Vandermonde matrix with d+p rows and d columns.
func vandermondeMatrix(d, p int) [][]uint32 {
	underlying := make([]uint32, d*(d+p))

	m := make([][]uint32, d+p)
	for i := 0; i < len(m); i++ {
		m[i] = underlying[:d]
		underlying = underlying[d:]

		for j := 0; j < len(m[i]); j++ {
			m[i][j] = gf.Raise(uint32(j+1), uint32(i))
		}
	}

	return m
}

// ParityMatrix returns a matrix with d+p rows and d columns with the upper left
// d by d submatrix being the identity matrix, and such that picking any d rows
// creates an invertible matrix.
func ParityMatrix(d, p int) [][]uint32 {
	m := vandermondeMatrix(d, p)
	solveSubIdentity(m)
	return m
}

// solveSubIdentity solves the upper len(m[0]) x len(m[0]) submatrix for the
// identity using column operations. panics if the matrix is singular.
func solveSubIdentity(m [][]uint32) {
	swap := func(m [][]uint32, a, b int) {
		for i := 0; i < len(m); i++ {
			m[i][a], m[i][b] = m[i][b], m[i][a]
		}
	}

	multiply := func(m [][]uint32, a int, n uint32) {
		for i := 0; i < len(m); i++ {
			m[i][a] = uint32((uint64(m[i][a]) * uint64(n)) % gf.MaxVal)
		}
	}

	addMultiple := func(m [][]uint32, a, b int, n uint32) {
		for i := 0; i < len(m); i++ {
			val := (uint64(m[i][b]) * uint64(n)) % gf.MaxVal
			m[i][a] = uint32((uint64(m[i][a]) + val) % gf.MaxVal)
		}
	}

	// Apply Gaussian elimination on the columns
	for i := 0; i < len(m[0]); i++ {
		// swap a column to the right of us if needed to ensure m[i][i] != 0
		if m[i][i] == 0 {
			for j := i + 1; j < len(m[0]); j++ {
				if m[i][j] != 0 {
					swap(m, i, j)
					break
				}
			}

			if m[i][i] == 0 {
				// shouldn't happen unless d == MaxVal-1
				panic("Couldn't ensure nonzero m[i][i]")
			}
		}

		// divide this column by m[i][i]
		if m[i][i] != 1 {
			multiply(m, i, gf.MInverse(m[i][i]))

			if m[i][i] != 1 {
				panic("Couldn't ensure one m[i][i]")
			}
		}

		// ensure every other value in m[i] is 0 by adding a multiple of
		// column i to them
		for j := 0; j < len(m[0]); j++ {
			if j == i {
				continue
			}

			if m[i][j] != 0 {
				addMultiple(m, j, i, gf.MaxVal-m[i][j])

				if m[i][j] != 0 {
					panic("Couldn't ensure zero m[i][j]")
				}
			}
		}
	}
}

func cloneMatrix(m [][]uint32) [][]uint32 {
	underlying := make([]uint32, len(m)*len(m[0]))

	n := make([][]uint32, len(m))
	for i := range m {
		n[i] = underlying[:len(m[i])]
		underlying = underlying[len(m[i]):]
		copy(n[i], m[i])
	}

	return n
}

func invertMatrix(m [][]uint32) [][]uint32 {
	c := cloneMatrix(m)
	for i := 0; i < len(m[0]); i++ {
		idRow := make([]uint32, len(m[0]))
		idRow[i] = 1
		c = append(c, idRow)
	}
	solveSubIdentity(c)
	return c[len(c)-len(m[0]):]
}
