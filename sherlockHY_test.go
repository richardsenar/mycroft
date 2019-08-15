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
func TestAltLoader(t *testing.T) {
	var tCases = []struct {
		input    string
		expected bool
	}{
		{"51700", true},
		{"31530", false},
	}
	for _, tc := range tCases {
		out := AltLoader(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestJoinCityState(t *testing.T) {
	var tCases = []struct {
		city     string
		state    string
		expected string
	}{
		{"Corona", "CA", "Corona CA"},
		{"", "CA", "CA"},
		{"Corona", "", "Corona"},
		{"", "", ""},
	}
	for _, tc := range tCases {
		out := joinCityState(tc.city, tc.state)
		if tc.expected != out {
			t.Errorf("input \"%v\" \"%v\", expecting \"%v\", got \"%v\"\n", tc.city, tc.state, tc.expected, out)
		}
	}
}

func TestCityStateParser(t *testing.T) {
	var tCases = []struct {
		citystate string
		city      string
		state     string
	}{
		{"Corona CA", "Corona", "CA"},
		{"Corona Hills CA", "Corona Hills", "CA"},
		{"NEWPORT NEWS VA", "NEWPORT NEWS", "VA"},
		{"CA", "", "CA"},
		{" ", "", ""},
	}
	for _, tc := range tCases {
		city, state := CityStateParser(tc.citystate)
		if tc.city != city || tc.state != state {
			t.Errorf("input \"%v\", expecting \"%v\" \"%v\", got \"%v\" \"%v\" \n", tc.citystate, tc.city, tc.state, city, state)
		}
	}
}

func TestParseGCV(t *testing.T) {
	var tCases = []struct {
		input    string
		expected string
	}{
		{"172600 145 11 1 1", "17261451111"},
		{"17260014501100010010000000", "17261451111"},
		{"00000000000000000017260014501100010010000000", "17261451111"},
		{"000000000000000000172600145011000100100000000000000000000", "17261451111"},
		{"00000 00000 00000000172 60014 50110 0010 0100 00000 00000 00000 000    ", "17261451111"},
		{"001 005", "15"},
		{" ", " "},
		{"", ""},
	}
	for _, tc := range tCases {
		out := parseGCV(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}

func TestBedfordCountyExeptionBK(t *testing.T) {
	var tCases = []struct {
		input    string
		expected string
	}{
		{"51515", "51019"},
		{"51019", "51019"},
		{"55555", "55555"},
		{"55", "55"},
		{"", ""},
	}
	for _, tc := range tCases {
		out := AltBKFIPS(tc.input)
		if tc.expected != out {
			t.Errorf("input \"%v\", expecting \"%v\", got \"%v\"\n", tc.input, tc.expected, out)
		}
	}
}
