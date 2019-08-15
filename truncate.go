package sherlock

import (
	attomdb "DG_QA/attomDB"
	"log"
)

/*
    .                                                            .
  .o8                                                          .o8
.o888oo oooo d8b oooo  oooo  ooo. .oo.    .ooooo.   .oooo.   .o888oo  .ooooo.
  888   `888""8P `888  `888  `888P"Y88b  d88' `"Y8 `P  )88b    888   d88' `88b
  888    888      888   888   888   888  888        .oP"888    888   888ooo888
  888 .  888      888   888   888   888  888   .o8 d8(  888    888 . 888    .o
  "888" d888b     `V88V"V8P' o888o o888o `Y8bod8P' `Y888""8o   "888" `Y8bod8P'
*/

type trimKey struct {
	FIPS string
	src  string
}

type trim struct {
	src    string
	col    int
	lt     int
	rt     int
	MaxLen int
}

// loadTruncations returns a map of truncated zips from SQL Server
func loadTruncations(db *attomdb.Server, logger *log.Logger) map[trimKey]trim {
	truncs := make(map[trimKey]trim)
	lt := struct {
		FIPS      string `db:"FIPS"`
		Src       string `db:"Src"`
		Col       int    `db:"Col"`
		LeftTrim  int    `db:"LeftTrim"`
		RightTrim int    `db:"RightTrim"`
		MaxLen    int    `db:"MaxLen"`
	}{}
	rows, err := db.AzDB5Thunder.AZRadar.Queryx(`
		SELECT 	FIPS
				,Src
				,Col
				,LeftTrim
				,RightTrim
				,MaxLen	
		FROM 	Radar.dbo.Sherlock_Truncate
	`)
	if err != nil {
		logger.Fatalln(err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.StructScan(&lt)
		if err != nil {
			logger.Fatalln(err)
		}
		truncs[trimKey{
			FIPS: trimS(lt.FIPS),
			src:  trimS(lt.Src),
		}] = trim{
			src:    trimS(lt.Src),
			col:    lt.Col,
			lt:     lt.LeftTrim,
			rt:     lt.RightTrim,
			MaxLen: lt.MaxLen,
		}
	}
	return truncs
}

// truncField executes truncation based on defined parameters
func truncField(fips, val, src string, col int, trunc map[trimKey]trim) (string, bool) {
	if t, ok := trunc[trimKey{
		FIPS: fips,
		src:  src,
	}]; ok {
		if len(val) < (t.lt + t.rt) {
			return val, false
		}
		if src != t.src {
			return val, false
		}
		if col != t.col {
			return val, false
		}
		if t.MaxLen != 0 && len(val) <= t.MaxLen {
			return val, false
		}
		return val[t.lt : len(val)-t.rt], true
	}
	return val, false
}
