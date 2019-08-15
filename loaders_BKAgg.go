package sherlock

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/dustin/go-humanize"

	attomdb "DG_QA/attomDB"
)

/*
oooooooooo.  oooo    oooo       .o.
`888'   `Y8b `888   .8P'       .888.
 888     888  888  d8'        .8"888.      .oooooooo  .oooooooo
 888oooo888'  88888[         .8' `888.    888' `88b  888' `88b
 888    `88b  888`88b.      .88ooo8888.   888   888  888   888
 888    .88P  888  `88b.   .8'     `888.  `88bod8P'  `88bod8P'
o888bood8P'  o888o  o888o o88o     o8888o `8oooooo.  `8oooooo.
                                          d"     YD  d"     YD
                                          "Y88888P'  "Y88888P'
*/
type bkData struct {
	DPID           string // 0
	Apn            string // 1
	OLDAPN         string // 2
	TxACCTNBR      string // 3
	PropHOUSENBR   string // 4
	PropSTREETNAME string // 5
	PropCITY       string // 6
	PropSTATE      string // 7
	PropUNITNBR    string // 8
	PropZIP        string // 9
}

// func loads BK Records and returns chan of *bkData
func loadBKData(db *attomdb.Server, fips string, logger *log.Logger, cfg *Config) chan *bkData {
	logger.Println(fmt.Sprintf("%s: Loading BK Aggregate Data...", fips))

	bkTotal := totalBKAgg(db, fips, logger)

	bkRecs := make(chan *bkData)
	go func() {
		defer close(bkRecs)
		bkLoad := struct {
			DPID           sql.NullString `db:"DPID"`
			Apn            sql.NullString `db:"APN"`
			OLDAPN         sql.NullString `db:"OLD_APN"`
			TxACCTNBR      sql.NullString `db:"TAX_ACCT_NBR"`
			PropHOUSENBR   sql.NullString `db:"PROP_HOUSE_NBR"`
			PropSTREETNAME sql.NullString `db:"PROP_STREET_NAME"`
			PropCITY       sql.NullString `db:"PROP_CITY"`
			PropSTATE      sql.NullString `db:"PROP_STATE"`
			PropUNITNBR    sql.NullString `db:"PROP_UNIT_NBR"`
			PropZIP        sql.NullString `db:"PROP_ZIP"`
		}{}
		rows, err := db.AzDB5Thunder.AZRadar.Queryx(
			`SELECT DPID
					,APN
					,OLD_APN
					,TAX_ACCT_NBR
					,PROP_HOUSE_NBR
					,PROP_STREET_NAME
					,PROP_CITY
					,PROP_STATE
					,PROP_UNIT_NBR
					,PROP_ZIP
			FROM	Radar.dbo.Sherlock_BKaggregate
			WHERE	FIPS = $1`,
			fips,
		)
		if err != nil {
			logger.Fatalln(err)
		}
		defer rows.Close()

		ctr := 0
		for rows.Next() {
			err = rows.StructScan(&bkLoad)
			if err != nil {
				logger.Fatalln("rows.StructScan(&bk)", err)
			}
			bkRecs <- &bkData{
				DPID:           trimS(bkLoad.DPID.String),
				Apn:            trimS(bkLoad.Apn.String),
				OLDAPN:         trimS(bkLoad.OLDAPN.String),
				TxACCTNBR:      trimS(bkLoad.TxACCTNBR.String),
				PropHOUSENBR:   trimS(bkLoad.PropHOUSENBR.String),
				PropSTREETNAME: trimS(bkLoad.PropSTREETNAME.String),
				PropCITY:       trimS(bkLoad.PropCITY.String),
				PropSTATE:      trimS(bkLoad.PropSTATE.String),
				PropUNITNBR:    trimS(bkLoad.PropUNITNBR.String),
				PropZIP:        trimS(bkLoad.PropZIP.String),
			}
			ctr++
		}
		var ctrStr string
		var bkTotalStr string
		var mtchPct float64

		ctrStr = humanize.Comma(int64(ctr))
		bkTotalStr = humanize.Comma(int64(bkTotal))
		mtchPct = (float64(ctr) / float64(bkTotal)) * 100

		fmt.Printf("=====> %s Matched %s of %s (%0.2f %%)\n", fips, ctrStr, bkTotalStr, mtchPct)
		logger.Printf("=====> %s Matched %s of %s (%0.2f %%)\n", fips, ctrStr, bkTotalStr, mtchPct)

		if err := rows.Err(); err != nil {
			logger.Fatalln(err)
			rows.Close()
		}
	}()
	return bkRecs
}

// =======================================================================================

// totalBKAgg gets total BK Aggregate Count
func totalBKAgg(db *attomdb.Server, fips string, logger *log.Logger) (count int) {
	err := db.AzDB5Thunder.AZRadar.Get(&count, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_BKaggregate
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	return
}
