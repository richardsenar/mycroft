package sherlock

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/dustin/go-humanize"

	attomdb "DG_QA/attomDB"
)

/*
                                .             oooo          .oooooo.                   .                              .
                              .o8             `888         d8P'  `Y8b                .o8                            .o8
ooo. .oo.  .oo.    .oooo.   .o888oo  .ooooo.   888 .oo.   888      888 oooo  oooo  .o888oo oo.ooooo.  oooo  oooo  .o888oo
`888P"Y88bP"Y88b  `P  )88b    888   d88' `"Y8  888P"Y88b  888      888 `888  `888    888    888' `88b `888  `888    888
 888   888   888   .oP"888    888   888        888   888  888      888  888   888    888    888   888  888   888    888
 888   888   888  d8(  888    888 . 888   .o8  888   888  `88b    d88'  888   888    888 .  888   888  888   888    888 .
o888o o888o o888o `Y888""8o   "888" `Y8bod8P' o888o o888o  `Y8bood8P'   `V88V"V8P'   "888"  888bod8P'  `V88V"V8P'   "888"
                                                                                            888
                                                                                           o888o
*/
type matchResult struct {
	FIPS       string
	MatchVal   string
	DPID       string
	SAPID      string
	BKCol      int
	CLCol      int
	BKRule     int
	CLRule     int
	BKHseNum   string
	CLHseNum   string
	BKStrName  string
	CLStrName  string
	BKUnitNum  string
	CLUnitNum  string
	BKZip      string
	CLZip      string
	InputMask  string
	OutputMask string
}

// func loads Output records and returns chan of *output
func loadMatchResults(db *attomdb.Server, fips string, logger *log.Logger, cfg *Config) chan *matchResult {
	logger.Println(fmt.Sprintf("%s: Loading MatchOutput Data...", fips))

	totalMO := totalMatchOutput(db, fips, logger)

	out := make(chan *matchResult)
	go func() {
		defer close(out)
		match := struct {
			FIPS       sql.NullString `db:"FIPS"`
			MatchVal   sql.NullString `db:"MatchVal"`
			DPID       sql.NullString `db:"DPID"`
			SAPID      sql.NullString `db:"SAPID"`
			BKCol      sql.NullInt64  `db:"BKCol"`
			CLCol      sql.NullInt64  `db:"CLCol"`
			BKRule     sql.NullInt64  `db:"BKRule"`
			CLRule     sql.NullInt64  `db:"CLRule"`
			BKHseNum   sql.NullString `db:"BKHseNum"`
			CLHseNum   sql.NullString `db:"CLHseNum"`
			BKStrName  sql.NullString `db:"BKStrName"`
			CLStrName  sql.NullString `db:"CLStrName"`
			BKUnitNum  sql.NullString `db:"BKUnitNum"`
			CLUnitNum  sql.NullString `db:"CLUnitNum"`
			BKZip      sql.NullString `db:"BKZip"`
			CLZip      sql.NullString `db:"CLZip"`
			InputMask  sql.NullString `db:"InputMask"`
			OutputMask sql.NullString `db:"OutputMask"`
		}{}
		rows, err := db.AzDB5Thunder.AZRadar.Queryx(
			`SELECT	FIPS
					,MatchVal
					,BKCol
					,DPID
					,BKRule
					,BKHseNum
					,BKStrName
					,CLCol
					,SAPID
					,CLRule
					,CLHseNum
					,CLStrName
					,BKUnitNum
					,CLUnitNum
					,BKZip
					,CLZip
					,InputMask
					,OutputMask
			FROM 	Radar.dbo.Sherlock_MatchOutput
			WHERE	FIPS = $1`,
			fips,
		)
		if err != nil {
			logger.Fatalln(err)
		}
		defer rows.Close()

		ctr := 0
		for rows.Next() {
			err = rows.StructScan(&match)
			if err != nil {
				logger.Fatalln(err)
			}
			out <- &matchResult{
				FIPS:       match.FIPS.String,
				MatchVal:   match.MatchVal.String,
				DPID:       match.DPID.String,
				SAPID:      match.SAPID.String,
				BKCol:      int(match.BKCol.Int64),
				CLCol:      int(match.CLCol.Int64),
				BKRule:     int(match.BKRule.Int64),
				CLRule:     int(match.CLRule.Int64),
				BKHseNum:   match.BKHseNum.String,
				CLHseNum:   match.CLHseNum.String,
				BKStrName:  match.BKStrName.String,
				CLStrName:  match.CLStrName.String,
				BKUnitNum:  match.BKUnitNum.String,
				CLUnitNum:  match.CLUnitNum.String,
				BKZip:      match.BKZip.String,
				CLZip:      match.CLZip.String,
				InputMask:  match.InputMask.String,
				OutputMask: match.OutputMask.String,
			}
			ctr++
		}
		var ctrStr string
		var moStr string
		var mtchPct float64

		ctrStr = humanize.Comma(int64(ctr))
		moStr = humanize.Comma(int64(totalMO))
		mtchPct = (float64(ctr) / float64(totalMO)) * 100

		fmt.Printf("=====> %s Filtered %s of %s (%0.2f %%)\n", fips, ctrStr, moStr, mtchPct)
		logger.Printf("=====> %s Filtered %s of %s (%0.2f %%)\n", fips, ctrStr, moStr, mtchPct)

		if err := rows.Err(); err != nil {
			logger.Fatalln(err)
			rows.Close()
		}
	}()
	return out
}

// =======================================================================================

// totalMatchOutput gets total MatchOutput Count
func totalMatchOutput(db *attomdb.Server, fips string, logger *log.Logger) (count int) {
	err := db.AzDB5Thunder.AZRadar.Get(&count, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_MatchOutput
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	return
}
