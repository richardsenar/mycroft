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

func TestGenerateMask(t *testing.T) {
	var tCases = []struct {
		id         int
		rule       int
		bkv        string
		clv        string
		inputMask  string
		outputMask string
	}{
		{1, 5, " ", " ", "", ""},
		{2, 5, "", "", "", ""},
		{3, 5, "002 012 010", "0002000000012010", "ABC DEF GHI", "0ABC000000DEFGHI"},
		{4, 5, "001 005", "0001000000005000", "ABC DEF", "0ABC000000DEF000"},
		{5, 5, "019A 039", "019A000000039000", "ABCD EFG", "ABCD000000EFG000"},
		{6, 5, "019A-039", "019A0000-00039000", "ABCD-EFG", "ABCD0000-00EFG000"},
		{7, 5, "019A.039", "019A0.000.00039000", "ABCD.EFG", "ABCD0.000.00EFG000"},
		{8, 5, "PU 21 GAPOCO", "PU21GAPOCO", "AB CD EFGHIJ", "ABCDEFGHIJ"},
		{9, 5, "PU21 GAPOCO", "00PU21GAPOCO000", "ABCD EFGHIJ", "00ABCDEFGHIJ000"},
		{10, 5, "PU21 GAPOCO", "00-00.00 00PU21GAPOCO000-000.00", "ABCD EFGHIJ", "00-00.00 00ABCDEFGHIJ000-000.00"},
	}
	for _, tc := range tCases {
		inputMask, outputMask := generateMask(tc.bkv, tc.clv, tc.rule)
		if tc.inputMask != inputMask || tc.outputMask != outputMask {
			t.Errorf("[%d] expecting \"%v\" \"%v\", got \"%v\" \"%v\"\n", tc.id, tc.inputMask, tc.outputMask, inputMask, outputMask)
		}
	}
}

func TestInputMask(t *testing.T) {
	var tCases = []struct {
		id       int
		input    string
		expected string
	}{
		{1, "", ""},
		{2, " ", " "},
		{3, "002", "ABC"},
		{4, "00 002", "AB CDE"},
		{5, "001 002 555", "ABC DEF GHI"},
		{6, "001-002-555", "ABC-DEF-GHI"},
		{7, "001.002.555", "ABC.DEF.GHI"},
		{8, "R04 009007 001", "ABC DEFGHI JKL"},
	}
	for _, tc := range tCases {
		output := inputMask(tc.input)
		if tc.expected != output {
			t.Errorf("[%d] expecting %v, got %v\n", tc.id, tc.expected, output)
		}
	}
}

func TestOutputMask(t *testing.T) {
	bkOffset := 1
	var tCases = []struct {
		id       int
		input    string
		expected string
	}{
		{1, "", ""},
		{2, " ", " "},
		{3, "002", "ABC"},
		{4, "00002", "ABCDE"},
		{5, "0A0011-002", "ABCDEF-GHI"},
		{6, "0A-00-11-002", "AB-CD-EF-GHI"},
	}
	for _, tc := range tCases {
		output, offset := outputMask(tc.input, bkOffset)
		if tc.expected != output {
			t.Errorf("[%d] expecting %v %v, got %v %v\n", tc.id, tc.expected, len(tc.expected), output, offset)
		}
	}
}
