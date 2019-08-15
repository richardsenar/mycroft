package sherlock

import (
	"testing"

	attomdb "DG_QA/attomDB"
)

/*
    .                          .
  .o8                        .o8
.o888oo  .ooooo.   .oooo.o .o888oo  .oooo.o
  888   d88' `88b d88(  "8   888   d88(  "8
  888   888ooo888 `"Y88b.    888   `"Y88b.
  888 . 888    .o o.  )88b   888 . o.  )88b
  "888" `Y8bod8P' 8""888P'   "888" 8""888P'
*/

func TestTruncField(t *testing.T) {
	db := attomdb.New()
	lt := loadTruncations(db, nil)
	var tCases = []struct {
		FIPS     string
		input    string
		src      string
		col      int
		expected string
	}{
		// {"06101", "21 230 001 A", "BK", 1, "21 230 001"},
		// {"06101", "21 230 A", "BK", 1, "21 230 A"},
		// {"06101", "21", "BK", 1, "21"},
		// {"06101", "", "BK", 1, ""},
		// {"06101", "21 230 001 A", "BK", 2, "21 230 001 A"},
		// {"06101", "23260099 B", "CL", 1, "23260099"},
		// {"06101", "2326 B", "CL", 1, "2326 B"},
		// {"06101", "23 B", "CL", 1, "23 B"},
		// {"06101", "", "CL", 1, ""},
		// {"06101", "23260099 B", "CL", 2, "23260099 B"},
		// {"56005", "R0010888", "BK", 1, "0010888"},
		// {"56005", "R0010888", "BK", 2, "R0010888"},
		// {"46005", "R0010888", "BK", 1, "R0010888"},
		// {"56005", "R0010888", "CL", 1, "R0010888"},
		// {"06023", "405081044000", "CL", 1, "405081044"},
		// {"06023", "40508104", "CL", 1, "40508104"},
		// {"06023", "40", "CL", 1, "40"},
	}
	for _, tc := range tCases {
		out, _ := truncField(tc.FIPS, tc.input, tc.src, tc.col, lt)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}
