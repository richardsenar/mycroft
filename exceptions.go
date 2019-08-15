package sherlock

import (
	attomdb "DG_QA/attomDB"
	"log"
)

/*
                                                         .    o8o
                                                       .o8    `"'
 .ooooo.  oooo    ooo  .ooooo.   .ooooo.  oo.ooooo.  .o888oo oooo   .ooooo.  ooo. .oo.    .oooo.o
d88' `88b  `88b..8P'  d88' `"Y8 d88' `88b  888' `88b   888   `888  d88' `88b `888P"Y88b  d88(  "8
888ooo888    Y888'    888       888ooo888  888   888   888    888  888   888  888   888  `"Y88b.
888    .o  .o8"'88b   888   .o8 888    .o  888   888   888 .  888  888   888  888   888  o.  )88b
`Y8bod8P' o88'   888o `Y8bod8P' `Y8bod8P'  888bod8P'   "888" o888o `Y8bod8P' o888o o888o 8""888P'
                                           888
                                          o888o
*/

// exception defines the exception object
type exception struct {
	FIPS   string
	BKCol  int
	CLCol  int
	BKRule int
	CLRule int
}

// loadExceptions return a map of exceptions values from SQL Server
func loadExceptions(db *attomdb.Server, logger *log.Logger) map[exception]struct{} {
	excep := make(map[exception]struct{})
	loadException := struct {
		FIPS   string `db:"FIPS"`
		BKCol  int    `db:"BKCol"`
		CLCol  int    `db:"CLCol"`
		BKRule int    `db:"BKRule"`
		CLRule int    `db:"CLRule"`
	}{}
	rows, err := db.AzDB5Thunder.AZRadar.Queryx(`
		SELECT 	FIPS,
				BKCol,
				CLCol,
				BKRule,
				CLRule
		FROM	Radar.dbo.Sherlock_Exceptions
	`)
	if err != nil {
		logger.Fatalln(err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.StructScan(&loadException)
		if err != nil {
			logger.Fatalln(err)
		}
		excep[exception{
			FIPS:   loadException.FIPS,
			BKCol:  loadException.BKCol,
			CLCol:  loadException.CLCol,
			BKRule: loadException.BKRule,
			CLRule: loadException.CLRule,
		}] = struct{}{}
	}
	return excep
}

// testException tests if match result has exception rules
func testException(mr *matchResult, exc map[exception]struct{}) (*matchResult, bool) {
	for e := range exc {
		if mr.FIPS == e.FIPS &&
			mr.BKCol == e.BKCol &&
			mr.CLCol == e.CLCol &&
			mr.BKRule == e.BKRule &&
			mr.CLRule == e.CLRule {
			return mr, true
		}
	}
	return nil, false
}

// OLD EXCEPTIONS -- DEPRICATED
// Parcel Boundaries
// exc[exception{zip: "01003", BKCol: 1, CLCol: 1, BKRule: 82, CLRule: 1}] = true
// exc[exception{zip: "06007", BKCol: 1, CLCol: 1, BKRule: 202, CLRule: 1}] = true
// exc[exception{zip: "13153", BKCol: 1, CLCol: 1, BKRule: 69, CLRule: 1}] = true
// exc[exception{zip: "17161", BKCol: -1, CLCol: 1, BKRule: 5000, CLRule: 1}] = true
// exc[exception{zip: "19095", BKCol: 2, CLCol: 2, BKRule: 19, CLRule: 1}] = true
// exc[exception{zip: "26025", BKCol: 1, CLCol: 1, BKRule: 5002, CLRule: 1}] = true
// exc[exception{zip: "31159", BKCol: 1, CLCol: 1, BKRule: 10, CLRule: 1}] = true
// exc[exception{zip: "34023", BKCol: 1, CLCol: 1, BKRule: 9999, CLRule: 9999}] = true
// exc[exception{zip: "36119", BKCol: 1, CLCol: 1, BKRule: 82, CLRule: 1}] = true
// exc[exception{zip: "42011", BKCol: 1, CLCol: 1, BKRule: 9999, CLRule: 6000}] = true
// exc[exception{zip: "42019", BKCol: 1, CLCol: 1, BKRule: 16, CLRule: 1}] = true
// exc[exception{zip: "42019", BKCol: 1, CLCol: 1, BKRule: 21, CLRule: 1}] = true
// exc[exception{zip: "42019", BKCol: 1, CLCol: 1, BKRule: 410, CLRule: 26}] = true
// exc[exception{zip: "48329", BKCol: 1, CLCol: 2, BKRule: 10, CLRule: 3}] = true
// exc[exception{zip: "48329", BKCol: 1, CLCol: 2, BKRule: 10, CLRule: 4}] = true
// exc[exception{zip: "48329", BKCol: 1, CLCol: 5, BKRule: 10, CLRule: 3}] = true
// exc[exception{zip: "55015", BKCol: 1, CLCol: 1, BKRule: 19, CLRule: 1}] = true
// exc[exception{zip: "55035", BKCol: 1, CLCol: 1, BKRule: 10, CLRule: 28}] = true
// exc[exception{zip: "55035", BKCol: 1, CLCol: 2, BKRule: 82, CLRule: 1}] = true
// exc[exception{zip: "55139", BKCol: 1, CLCol: 1, BKRule: 10, CLRule: 1}] = true
// exc[exception{zip: "56005", BKCol: 1, CLCol: 1, BKRule: 5001, CLRule: 1}] = true

// JIRA DM-39
// exc[exception{zip: "36069", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "36069", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}] = true
// exc[exception{zip: "13093", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "13093", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}] = true
// exc[exception{zip: "13017", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "13017", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}] = true
// exc[exception{zip: "36073", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "36073", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}] = true
// exc[exception{zip: "36099", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "36035", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "36043", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
// exc[exception{zip: "13091", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5}] = true
