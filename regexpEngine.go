package sherlock

import (
	"regexp"
	"strings"
)

/*
                                                    oooooooooooo
                                                    `888'     `8
oooo d8b  .ooooo.   .oooooooo  .ooooo.  oooo    ooo  888         ooo. .oo.    .oooooooo
`888""8P d88' `88b 888' `88b  d88' `88b  `88b..8P'   888oooo8    `888P"Y88b  888' `88b
 888     888ooo888 888   888  888ooo888    Y888'     888    "     888   888  888   888
 888     888    .o `88bod8P'  888    .o  .o8"'88b    888       o  888   888  `88bod8P'
d888b    `Y8bod8P' `8oooooo.  `Y8bod8P' o88'   888o o888ooooood8 o888o o888o `8oooooo.
                   d"     YD                                                 d"     YD
                   "Y88888P'                                                 "Y88888P'
*/

// regexMatch executes regex match process
func regexMatch(v1, v2 string, isTruncated bool) (int, bool) {
	if emptyFields(v1, v2) {
		return nullInt, false
	}
	// test for low level rule match
	if regexp.MustCompile(regexStringLow(v1)).MatchString(v2) ||
		regexp.MustCompile(regexStringLow(v2)).MatchString(v1) {
		return lowRule, true
	}
	// test for high level rule match
	if regexp.MustCompile(regexStringHigh(v1)).MatchString(v2) ||
		regexp.MustCompile(regexStringHigh(v2)).MatchString(v1) {
		return highRule, true
	}
	// bypass complex rule match is truncated
	if isTruncated {
		return nullInt, false
	}
	// test for complex rule match
	if regexp.MustCompile(regexStringComplex(v1)).MatchString(v2) ||
		regexp.MustCompile(regexStringComplex(v2)).MatchString(v1) {
		return remZeroRule, true
	}
	return nullInt, false
}

// regexStringLow generates low level regex match pattern
func regexStringLow(v string) string {
	if emptyFields(v) {
		return v
	}
	reString := []string{}
	sv := strings.Fields(v)
	reString = append(reString, "^")
	for _, s := range sv {
		reString = append(reString, "[0 ]*")
		reString = append(reString, s)
	}
	reString = append(reString, "$")
	return strings.Join(reString, "")
}

// regexStringHigh generates high level regex match pattern
func regexStringHigh(v string) string {
	if emptyFields(v) {
		return v
	}
	reString := []string{}
	sv := strings.Fields(v)
	reString = append(reString, "^[0 ]*")
	for _, s := range sv {
		reString = append(reString, s)
		reString = append(reString, "[0 ]*")
	}
	reString = append(reString, "$")
	return strings.Join(reString, "")
}

// regexStringComplex generates complex regex match pattern
func regexStringComplex(v string) string {
	if emptyFields(v) {
		return v
	}
	reString := []string{}
	sv := strings.Split(v, "")
	reString = append(reString, "^[0 ]*")
	for _, s := range sv {
		if s == "0" || s == " " {
			continue
		}
		reString = append(reString, s)
		reString = append(reString, "[0 ]*")
	}
	reString = append(reString, "$")
	return strings.Join(reString, "")
}
