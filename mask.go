package sherlock

import "strings"

/*
                                     oooo
                                     `888
ooo. .oo.  .oo.    .oooo.    .oooo.o  888  oooo   .oooo.o
`888P"Y88bP"Y88b  `P  )88b  d88(  "8  888 .8P'   d88(  "8
 888   888   888   .oP"888  `"Y88b.   888888.    `"Y88b.
 888   888   888  d8(  888  o.  )88b  888 `88b.  o.  )88b
o888o o888o o888o `Y888""8o 8""888P' o888o o888o 8""888P'
*/

var maskMap = map[int]string{
	1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F",
	7: "G", 8: "H", 9: "I", 10: "J", 11: "K", 12: "L",
	13: "M", 14: "N", 15: "O", 16: "P", 17: "Q", 18: "R",
	19: "S", 20: "T", 21: "U", 22: "V", 23: "W", 24: "X",
	25: "Y", 26: "Z",
}

// Generate Input and Output Masks for UMS
func generateMask(bkv, clv string, rule int) (string, string) {
	if emptyFields(bkv, clv) {
		return "", ""
	}
	if rule > 5 {
		return "", ""
	}

	bks := strings.FieldsFunc(bkv, func(r rune) bool { return strings.ContainsRune(" -.", r) })
	msk := ""
	off := 1
	newMsk := []string{}

	for len(clv) > 0 {
		if strings.HasPrefix(clv, bks[0]) {
			msk, off = outputMask(bks[0], off)
			newMsk = append(newMsk, msk)
			clv = strings.TrimPrefix(clv, bks[0])
			if len(bks) > 1 {
				bks = bks[1:]
			}
			continue
		}
		newMsk = append(newMsk, clv[:1])
		clv = clv[1:]
	}
	return inputMask(bkv), strings.Join(newMsk, "")
}

// inputMask generates the input mask
func inputMask(s string) string {
	if emptyFields(s) {
		return s
	}
	new := []string{}
	idx := 1
	for _, m := range s {
		if string(m) == " " || string(m) == "-" || string(m) == "." {
			new = append(new, string(m))
			continue
		}
		new = append(new, maskMap[idx])
		idx++
	}
	return strings.Join(new, "")
}

// outputMask generates the output mask
func outputMask(m string, off int) (string, int) {
	if emptyFields(m) {
		return m, off
	}
	new := []string{}
	for _, s := range m {
		if string(s) == " " || string(s) == "-" || string(s) == "." {
			new = append(new, string(s))
			continue
		}
		new = append(new, maskMap[off])
		off++
	}
	return strings.Join(new, ""), off
}

// Generate Input and Output Masks for UMS
func generateMaskPlus(bkv, clv string) (string, string) {
	return bkv, clv
}
