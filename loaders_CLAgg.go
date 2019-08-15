package sherlock

import (
	"database/sql"
	"fmt"
	"log"

	attomdb "DG_QA/attomDB"
)

/*
  .oooooo.   ooooo              .o.
 d8P'  `Y8b  `888'             .888.
888           888             .8"888.      .oooooooo  .oooooooo
888           888            .8' `888.    888' `88b  888' `88b
888           888           .88ooo8888.   888   888  888   888
`88b    ooo   888       o  .8'     `888.  `88bod8P'  `88bod8P'
 `Y8bood8P'  o888ooooood8 o88o     o8888o `8oooooo.  `8oooooo.
                                          d"     YD  d"     YD
                                          "Y88888P'  "Y88888P'
*/
type clData struct {
	SAPID                string // 0
	SaPARCELNBRPRIMARY   string // 1
	SaPARCELNBRPREVIOUS  string // 2
	SaPARCELNBRALT       string // 3
	SaPARCELACCOUNTNBR   string // 4
	SaPARCELNBRREFERENCE string // 5
	SaSITEHOUSENBR       string // 6
	SaSITESTREETNAME     string // 7
	SaSITEUNITVAL        string // 8
	SaSITECITYSTATE      string // 9
	SaSITECITY           string // 10
	SaSITESTATE          string // 11
	SaSITEZIP            string // 12

}

// func loads CL Records and returns chan of *clData
func loadCLData(db *attomdb.Server, fips string, logger *log.Logger) chan *clData {
	logger.Println(fmt.Sprintf("%s: Loading CL Aggregate Data...", fips))
	clRecs := make(chan *clData)
	go func() {
		defer close(clRecs)
		clDLoad := struct {
			SaPROPERTYID         sql.NullString `db:"SA_PROPERTY_ID"`
			SaPARCELNBRPRIMARY   sql.NullString `db:"SA_PARCEL_NBR_PRIMARY"`
			SaPARCELNBRPREVIOUS  sql.NullString `db:"SA_PARCEL_NBR_PREVIOUS"`
			SaPARCELNBRALT       sql.NullString `db:"SA_PARCEL_NBR_ALT"`
			SaPARCELACCOUNTNBR   sql.NullString `db:"SA_PARCEL_ACCOUNT_NBR"`
			SaPARCELNBRREFERENCE sql.NullString `db:"SA_PARCEL_NBR_REFERENCE"`
			SaSITEHOUSENBR       sql.NullString `db:"SA_SITE_HOUSE_NBR"`
			SaSITESTREETNAME     sql.NullString `db:"SA_SITE_STREET_NAME"`
			SaSITEUNITVAL        sql.NullString `db:"SA_SITE_UNIT_VAL"`
			SaSITECITYSTATE      sql.NullString `db:"SA_SITE_CITY_STATE"`
			SaSITEZIP            sql.NullString `db:"SA_SITE_ZIP"`
		}{}
		rows, err := db.AzDB5Thunder.AZRadar.Queryx(
			`SELECT	SA_PROPERTY_ID
					,SA_PARCEL_NBR_PRIMARY
					,SA_PARCEL_NBR_PREVIOUS
					,SA_PARCEL_NBR_ALT
					,SA_PARCEL_ACCOUNT_NBR
					,SA_PARCEL_NBR_REFERENCE
					,SA_SITE_HOUSE_NBR
					,SA_SITE_STREET_NAME
					,SA_SITE_UNIT_VAL
					,SA_SITE_CITY_STATE
					,SA_SITE_ZIP
			FROM	Radar.dbo.Sherlock_CLaggregate
			WHERE	FIPS = $1`,
			fips,
		)
		if err != nil {
			logger.Fatalln(err)
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.StructScan(&clDLoad)
			if err != nil {
				logger.Fatalln(err)
			}
			city, state := CityStateParser(clDLoad.SaSITECITYSTATE.String)
			clRecs <- &clData{
				SAPID:                trimS(clDLoad.SaPROPERTYID.String),
				SaPARCELNBRPRIMARY:   trimS(clDLoad.SaPARCELNBRPRIMARY.String),
				SaPARCELNBRPREVIOUS:  trimS(clDLoad.SaPARCELNBRPREVIOUS.String),
				SaPARCELNBRALT:       trimS(clDLoad.SaPARCELNBRALT.String),
				SaPARCELACCOUNTNBR:   trimS(clDLoad.SaPARCELACCOUNTNBR.String),
				SaPARCELNBRREFERENCE: trimS(clDLoad.SaPARCELNBRREFERENCE.String),
				SaSITEHOUSENBR:       trimS(clDLoad.SaSITEHOUSENBR.String),
				SaSITESTREETNAME:     trimS(clDLoad.SaSITESTREETNAME.String),
				SaSITEUNITVAL:        trimS(clDLoad.SaSITEUNITVAL.String),
				SaSITECITYSTATE:      trimS(clDLoad.SaSITECITYSTATE.String),
				SaSITEZIP:            trimS(clDLoad.SaSITEZIP.String),
				SaSITECITY:           trimS(city),
				SaSITESTATE:          trimS(state),
			}
		}
		if err := rows.Err(); err != nil {
			logger.Fatalln(err)
			rows.Close()
		}
	}()
	return clRecs
}

// =======================================================================================

// totalCLAgg gets total CL Aggregate Count
func totalCLAgg(db *attomdb.Server, fips string, logger *log.Logger) (count int) {
	err := db.AzDB5Thunder.AZRadar.Get(&count, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_CLaggregate
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	return
}
