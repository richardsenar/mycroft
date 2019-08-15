package sherlock

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

/*
oooo                  oooo                                 .o88o.
`888                  `888                                 888 `"
 888 .oo.    .ooooo.   888  oo.ooooo.   .ooooo.  oooo d8b o888oo  oooo  oooo  ooo. .oo.    .ooooo.   .oooo.o
 888P"Y88b  d88' `88b  888   888' `88b d88' `88b `888""8P  888    `888  `888  `888P"Y88b  d88' `"Y8 d88(  "8
 888   888  888ooo888  888   888   888 888ooo888  888      888     888   888   888   888  888       `"Y88b.
 888   888  888    .o  888   888   888 888    .o  888      888     888   888   888   888  888   .o8 o.  )88b
o888o o888o `Y8bod8P' o888o  888bod8P' `Y8bod8P' d888b    o888o    `V88V"V8P' o888o o888o `Y8bod8P' 8""888P'
                             888
                            o888o
*/

// Config contains configuration settings
type Config struct {
	ConLoad   int
	ConProc   int
	Workers   int
	BatchSize int
	BKFlds    []int
	CLFlds    []int
	MOTbl     string
	MOFTbl    string
	MOFRTbl   string
	Verb      bool
}

// Check for empty fields
func emptyFields(fields ...string) bool {
	for _, field := range fields {
		if len(field) == 0 {
			return true
		}
		if len(strings.Replace(field, " ", "", -1)) == 0 {
			return true
		}
	}
	return false
}

func zeroVal(totals ...int) bool {
	for _, t := range totals {
		if t == 0 {
			return true
		}
	}
	return false
}

// notMatchField checks for invalid match fields by index
func notMatchField(id int, fields []int) bool {
	for _, i := range fields {
		if id == i {
			return false
		}
	}
	return true
}

// trim leading and trailing white spaces
func trimS(s string) string {
	return strings.TrimSpace(s)
}

// stripSep removes separator characters
func ssep(p string) string {
	sep := []string{"'", "#", "$", "-", "+", ".", "*", "(", ")", ":", ";", "{", "}", "|", "&", " ", "_", "/", `\`}
	for _, v := range sep {
		p = strings.Replace(p, v, " ", -1)
	}
	p = strings.Join(strings.Fields(p), " ")
	return p
}

// stripSep removes separator characters and merges updated strings
func ssepm(p string) string {
	sep := []string{"'", "#", "$", "-", "+", ".", "*", "(", ")", ":", ";", "{", "}", "|", "&", " ", "_", "/", `\`}
	for _, v := range sep {
		p = strings.Replace(p, v, "", -1)
	}
	return p
}

// Convert string to int
func convInt(n string) int {
	pnt, err := strconv.Atoi(n)
	if err != nil {
		return -1
	}
	return pnt
}

// ConvStrInt converts slice of strings to slice of ints
func ConvStrInt(str string) []int {
	strNums := strings.Split(str, ",")
	ints := make([]int, 0)
	for _, s := range strNums {
		ints = append(ints, convInt(s))
	}
	return ints
}

// TimeTracker tracks duration of function
func TimeTracker(start time.Time, name string, logger *log.Logger) {
	elapsed := time.Since(start)
	logger.Printf("%s took %v Hr, %v Min, %v Sec", name, int64(elapsed/time.Hour), int64(elapsed/time.Minute), int64(elapsed/time.Second))
}

// FnTracker tracks duration of function
func FnTracker(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %v Min, %v Sec", name, int64(elapsed/time.Minute), int64(elapsed/time.Second))
}

// CityStateParser parses out city state from a combined field
func CityStateParser(citystate string) (string, string) {
	var city, state string
	CityState := strings.Fields(trimS(citystate))
	if len(CityState) >= 1 {
		city = strings.Join(CityState[:len(CityState)-1], " ")
		state = CityState[len(CityState)-1]
	}
	return city, state
}

// joinCityState combines city state fields
func joinCityState(city, state string) string {
	city = trimS(city)
	state = trimS(state)
	switch {
	case city == "" || state == "":
		return fmt.Sprintf("%s%s", city, state)
	default:
		return fmt.Sprintf("%s %s", city, state)
	}
}

// AltLoader executes alternate CL aggregate load
func AltLoader(fips string) bool {
	alts := []string{"51700"}
	for _, alt := range alts {
		if fips == alt {
			return true
		}
	}
	return false
}

// parseGCV parses out greatest commong values excluding " " and "0"
func parseGCV(val string) string {
	if val == "" || val == " " {
		return val
	}
	var nval []string
	sval := strings.Split(val, "")
	for _, s := range sval {
		if s == " " || s == "0" {
			continue
		}
		nval = append(nval, s)
	}
	return strings.Join(nval, "")
}

// TrimDate trims date field
func TrimDate(d string) string {
	if len(d) >= 10 {
		return d[:10]
	}
	return d
}

// ======================================================================================

// ToNullString invalidates a sql.NullString if empty, validates if not empty
// func ToNullString(s string) sql.NullString {
// 	return sql.NullString{String: s, Valid: s != ""}
// }

// ToNullInt64 validates a sql.NullInt64 if incoming string evaluates to an integer, invalidates if it does not
// func ToNullInt64(i int) sql.NullInt64 {
// 	return sql.NullInt64{Int64: int64(i), Valid: i != -1}
// }

// ToNullInt64 validates a sql.NullInt64 if incoming string evaluates to an integer, invalidates if it does not
// func ToNullInt64(s string) sql.NullInt64 {
// 	i, err := strconv.Atoi(s)
// 	return sql.NullInt64{Int64: int64(i), Valid: err == nil}
// }
