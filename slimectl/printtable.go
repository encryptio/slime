package main

import (
	"io"
)

func printTable(w io.Writer, tbl [][]string, widthLimit int) {
	if len(tbl) == 0 {
		return
	}

	// gather maximum column widths
	colW := make([]int, len(tbl[0]))
	for _, row := range tbl {
		for i, cell := range row {
			if len(cell) > colW[i] {
				colW[i] = len(cell)
			}
		}
	}

	if widthLimit > 0 {
		// make sure the printed width fits; ellipsize the longest columns if needed
		for {
			totalWidth := len(colW) - 1 // one space between columns
			maxWidth := 0
			maxIndex := -1
			for i, w := range colW {
				totalWidth += w
				if w > maxWidth {
					maxWidth = w
					maxIndex = i
				}
			}

			if totalWidth <= widthLimit {
				break
			}

			colW[maxIndex]--
		}
	}

	for _, row := range tbl {
		o := ""
		for i, cell := range row {
			shown := 0
			if len(cell) <= colW[i] {
				o += cell
				shown = len(cell)
			} else {
				if colW[i] > 4 {
					o += cell[:colW[i]-2] + ".."
				} else {
					o += cell[:colW[i]]
				}
				shown = colW[i]
			}
			want := colW[i] + 1
			if i == len(row)-1 {
				want = 0
			}
			for j := shown; j < want; j++ {
				o += " "
			}
		}
		o += "\n"

		w.Write([]byte(o))
	}
}
