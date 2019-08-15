package sherlock

import (
	"testing"
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
func TestEmptyFields(t *testing.T) {
	var tCases = []struct {
		input    string
		expected bool
	}{
		{"aaa", false},
		{"a a", false},
		{"             ", true},
		{"   ", true},
		{" ", true},
		{"", true},
	}
	for _, tc := range tCases {
		out := emptyFields(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestZeroVal(t *testing.T) {
	var tCases = []struct {
		input    int
		expected bool
	}{
		{0, true},
		{1, false},
	}
	for _, tc := range tCases {
		out := zeroVal(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestNonMatch(t *testing.T) {
	var tCases = []struct {
		input    int
		fields   []int
		expected bool
	}{
		{1, []int{1, 2, 3}, false},
		{4, []int{1, 2, 3}, true},
	}
	for _, tc := range tCases {
		out := notMatchField(tc.input, tc.fields)
		if tc.expected != out {
			t.Errorf("input \"%v\", fields \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.fields, tc.expected, out)
		}
	}
}

func TestTrimS(t *testing.T) {
	var tCases = []struct {
		input    string
		expected string
	}{
		{" abc  ", "abc"},
		{" a b c     ", "a b c"},
		{"   ", ""},
		{"  ", ""},
		{" ", ""},
	}
	for _, tc := range tCases {
		out := trimS(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestSsep(t *testing.T) {
	var tCases = []struct {
		input    string
		expected string
	}{
		{"  090    1F67    3  ", "090 1F67 3"},
		{"10C 13  1  3", "10C 13 1 3"},
		{"2        29A", "2 29A"},
		{"550800 169.31-3115-22", "550800 169 31 3115 22"},
		{"%s", "%s"},
	}
	for _, tc := range tCases {
		out := ssep(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestSsepm(t *testing.T) {
	var tCases = []struct {
		input    string
		expected string
	}{
		{"  090  -  1F67-  -3  ", "0901F673"},
		{"2004F127    A12D0048", "2004F127A12D0048"},
		{"200 4F127 A12D 0048", "2004F127A12D0048"},
	}
	for _, tc := range tCases {
		out := ssepm(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestConvInt(t *testing.T) {
	var tCases = []struct {
		input    string
		expected int
	}{
		{"1", 1},
		{"two", -1},
	}
	for _, tc := range tCases {
		out := convInt(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestConvSliceStrToInt(t *testing.T) {
	var tCases = []struct {
		input    string
		expected []int
	}{
		{"1,2,3", []int{1, 2, 3}},
		{"1,2,four", []int{1, 2, -1}},
	}
	for _, tc := range tCases {
		out := ConvStrInt(tc.input)
		if tc.expected[0] != out[0] {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
		if tc.expected[1] != out[1] {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
		if tc.expected[2] != out[2] {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}
