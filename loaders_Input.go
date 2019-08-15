package sherlock

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	attomdb "DG_QA/attomDB"
)

/*
 o8o                                         .
 `"'                                       .o8
oooo  ooo. .oo.   oo.ooooo.  oooo  oooo  .o888oo
`888  `888P"Y88b   888' `88b `888  `888    888
 888   888   888   888   888  888   888    888
 888   888   888   888   888  888   888    888 .
o888o o888o o888o  888bod8P'  `V88V"V8P'   "888"
                   888
                  o888o
*/

// InputLoader loads FIPS codes from dbo.SHERLOCK_INPUT and returns channel of FIPS codes
func InputLoader(db *attomdb.Server, mode string, logger *log.Logger) chan string {
	fipsChan := make(chan string)
	go func() {
		defer close(fipsChan)
		var fips string
		var rows *sqlx.Rows
		var err error

		switch {
		case mode == "Loader":
			rows, err = db.AzDB5Thunder.AZRadar.Queryx(`
				SELECT	FIPS
				FROM	Radar.dbo.Sherlock_FIPSInput
				WHERE	Loader IS NULL
				OR		Loader = ''
			`)
		case mode == "Matcher":
			rows, err = db.AzDB5Thunder.AZRadar.Queryx(`
				SELECT	FIPS
				FROM	Radar.dbo.Sherlock_FIPSInput
				WHERE	Matcher IS NULL
				OR		Matcher = ''
			`)
		case mode == "Filter":
			rows, err = db.AzDB5Thunder.AZRadar.Queryx(`
				SELECT	FIPS
				FROM	Radar.dbo.Sherlock_FIPSInput
				WHERE	Filter IS NULL
				OR		Filter = ''
			`)
		case mode == "Report":
			rows, err = db.AzDB5Thunder.AZRadar.Queryx(`
				SELECT	FIPS
				FROM	Radar.dbo.Sherlock_FIPSInput
				WHERE	Report IS NULL
				OR		Report = ''
			`)
		}
		if err != nil {
			logger.Fatalln(err)
		}

		fipsMap := make(map[string]struct{})
		var fipsBtch []string

		for rows.Next() {
			err = rows.Scan(&fips)
			if err != nil {
				logger.Fatalln(err)
			}
			fipsMap[fips] = struct{}{}
		}
		for f := range fipsMap {
			fipsBtch = append(fipsBtch, f)
		}

		if len(fipsBtch) > 0 {
			// Bulk Reset
			switch {
			case mode == "Loader":
				err := bulkResetAggregates(db, logger)
				if err != nil {
					logger.Fatalln("Bulk Reset Aggregates", err)
				}
			case mode == "Matcher":
				err := bulkResetMatchOutput(db, logger)
				if err != nil {
					logger.Fatalln("Bulk Reset MatchOutput", err)
				}
			case mode == "Filter":
				err := bulkResetMatchOutputFiltered(db, logger)
				if err != nil {
					logger.Fatalln("Bulk Reset MatchOutputFiltered", err)
				}
			case mode == "Report":
				err := bulkResetMatchOutputFilteredReport(db, logger)
				if err != nil {
					logger.Fatalln("Bulk Reset MatchOutputFiltered Report", err)
				}
			}
			for _, f := range fipsBtch {
				fipsChan <- f
			}
		}
	}()
	return fipsChan
}

// =======================================================================================

// RunLoader runs the aggregarte process for BK & CL datasets
// for New England Townships ('CT','MA','ME','NH','RI','VT'), a custom aggregate script
// is executed
func RunLoader(db *attomdb.Server, fipsChan <-chan string, logger *log.Logger, cfg *Config) {
	// Load New England State Zip codes
	nes := LoadNewEnglandStates(db, logger)

	var wg sync.WaitGroup
	for i := 0; i < cfg.ConLoad; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// loads fips values
			for fips := range fipsChan {
				if _, ok := nes[fips]; ok {
					runLoadersNES(db, fips, logger)
					UpdateLoaderStatus(db, fips, logger)
					continue
				}
				runLoaders(db, fips, logger)
				UpdateLoaderStatus(db, fips, logger)
			}
		}()
	}
	wg.Wait()
}

/*
oooo                            .o8
`888                           "888
 888   .ooooo.   .oooo.    .oooo888   .ooooo.  oooo d8b  .oooo.o
 888  d88' `88b `P  )88b  d88' `888  d88' `88b `888""8P d88(  "8
 888  888   888  .oP"888  888   888  888ooo888  888     `"Y88b.
 888  888   888 d8(  888  888   888  888    .o  888     o.  )88b
o888o `Y8bod8P' `Y888""8o `Y8bod88P" `Y8bod8P' d888b    8""888P'
*/

func runLoaders(db *attomdb.Server, fips string, logger *log.Logger) {
	defer TimeTracker(time.Now(), fmt.Sprintf("%s: Loading", fips), logger)
	logger.Printf("%s: Loading BK-CL Aggregates...\n", fips)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		RunLoaderRDBK(db, AltBKFIPS(fips), logger)
	}()
	go func() {
		defer wg.Done()
		RunLoaderRDCL(db, fips, logger)
	}()
	wg.Wait()
	fmt.Printf("=====> %s: Loaded BK(%d) - CL(%d)\n", fips, totalBK(db, AltBKFIPS(fips)), totalCL(db, fips))
	logger.Printf("=====> %s: Loaded BK(%d) - CL(%d)\n", fips, totalBK(db, AltBKFIPS(fips)), totalCL(db, fips))

	/************************************/
	/************************************/
	/************************************/
	// RunLoaderRDBK(db, AltBKFIPS(fips), logger)
	// fmt.Printf("=====> %s: Loaded BK(%d)\n", fips, totalBK(db, AltBKFIPS(fips)))
	// logger.Printf("=====> %s: Loaded BK(%d)\n", fips, totalBK(db, AltBKFIPS(fips)))
	/************************************/
	/************************************/
	/************************************/
}

// RunLoaderRDBK runs BK & CL SQL Uploader stored proc, suppress DimDeleteAssessor
func RunLoaderRDBK(db *attomdb.Server, fips string, logger *log.Logger) {
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_Aggregate_RemDeletes_BK_sp $1`, fips)
	if err != nil {
		logger.Fatalln("Unable to execute Radar.dbo.Sherlock_Aggregate_RemDeletes_BK_sp", err)
	}
}

// RunLoaderRDCL runs BK & CL SQL Uploader stored proc, suppress DimDeleteAssessor
func RunLoaderRDCL(db *attomdb.Server, fips string, logger *log.Logger) {
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_Aggregate_RemDeletes_CL_sp $1`, fips)
	if err != nil {
		logger.Fatalln("Unable to execute Radar.dbo.Sherlock_Aggregate_RemDeletes_CL_sp", err)
	}
}

/*
ooo. .oo.    .ooooo.   .oooo.o
`888P"Y88b  d88' `88b d88(  "8
 888   888  888ooo888 `"Y88b.
 888   888  888    .o o.  )88b
o888o o888o `Y8bod8P' 8""888P'
*/

func runLoadersNES(db *attomdb.Server, fips string, logger *log.Logger) {
	defer TimeTracker(time.Now(), fmt.Sprintf("%s: Loading", fips), logger)
	logger.Printf("%s: Loading (NES) BK-CL Aggregates...\n", fips)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		RunLoaderNESRemDeleteBK(db, fips, logger)
	}()
	go func() {
		defer wg.Done()
		RunLoaderNESRemDeleteCL(db, fips, logger)
	}()
	wg.Wait()
	fmt.Printf("=====> %s: Loaded BK(%d) - CL(%d)\n", fips, totalBK(db, fips), totalCL(db, fips))
	logger.Printf("=====> %s: Loaded BK(%d) - CL(%d)\n", fips, totalBK(db, fips), totalCL(db, fips))

	/************************************/
	/************************************/
	/************************************/
	// RunLoaderNESRemDeleteBK(db, fips, logger)
	// fmt.Printf("=====> %s: Loaded BK(%d)\n", fips, totalBK(db, fips))
	// logger.Printf("=====> %s: Loaded BK(%d)\n", fips, totalBK(db, fips))
	/************************************/
	/************************************/
	/************************************/
}

// RunLoaderNESRemDeleteBK runs BK & CL SQL Uploader stored proc for NES, suppress DimDeleteAssessor
func RunLoaderNESRemDeleteBK(db *attomdb.Server, fips string, logger *log.Logger) {
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_Aggregate_NES_RemDeletes_BK_sp $1`, fips)
	if err != nil {
		logger.Fatalln(err)
	}
}

// RunLoaderNESRemDeleteCL runs BK & CL SQL Uploader stored proc for NES, suppress DimDeleteAssessor
func RunLoaderNESRemDeleteCL(db *attomdb.Server, fips string, logger *log.Logger) {
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_Aggregate_RemDeletes_CL_sp $1`, fips)
	if err != nil {
		logger.Fatalln(err)
	}
}

// =======================================================================================

// LoadNewEnglandStates gets all New England States zip codes
func LoadNewEnglandStates(db *attomdb.Server, logger *log.Logger) map[string]struct{} {
	logger.Println("Loading NewEngland States Fips codes...")

	neStates := make(map[string]struct{})
	var NESZipCodes []string
	db.AzDB5Thunder.AZRTDomain.Select(&NESZipCodes, `
		SELECT RIGHT('0' + FipsStateCode, 2) + RIGHT('00' + FipsMuniCode, 3)
		FROM RTDomain.dbo.DimJurisdiction
		WHERE StateCode IN ('CT','MA','ME','NH','RI','VT')
		GROUP BY RIGHT('0' + FipsStateCode, 2) + RIGHT('00' + FipsMuniCode, 3)
	`)
	for _, nesZip := range NESZipCodes {
		neStates[nesZip] = struct{}{}
	}
	return neStates
}

// =======================================================================================

// AltBKFIPS test for alternate FIPS code for BK aggregates
func AltBKFIPS(fips string) string {
	// For Bedford County
	if fips == "51515" {
		return "51019"
	}
	return fips
}

// =======================================================================================

func totalBK(db *attomdb.Server, fips string) int {
	var BK int
	db.AzDB5Thunder.AZRadar.Get(&BK, `SELECT COUNT(1) FROM Radar.dbo.Sherlock_BKaggregate WHERE FIPS IN ($1)`, fips)
	return BK
}

func totalCL(db *attomdb.Server, fips string) int {
	var CL int
	db.AzDB5Thunder.AZRadar.Get(&CL, `SELECT COUNT(1) FROM Radar.dbo.Sherlock_CLaggregate WHERE FIPS IN ($1)`, fips)
	return CL
}
