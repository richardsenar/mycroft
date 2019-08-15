package sherlock

import (
	attomdb "DG_QA/attomDB"
	"log"
	"math"
	"sync"
	"time"
)

/*
                                                     .
                                                   .o8
oooo d8b  .ooooo.  oo.ooooo.   .ooooo.  oooo d8b .o888oo
`888""8P d88' `88b  888' `88b d88' `88b `888""8P   888
 888     888ooo888  888   888 888   888  888       888
 888     888    .o  888   888 888   888  888       888 .
d888b    `Y8bod8P'  888bod8P' `Y8bod8P' d888b      "888"
                    888
                   o888o
*/

type matchReport struct {
	fips                     string
	totalBKAggregate         int
	totalCLAggregate         int
	totalMatchOutput         int
	totalMatchOutputFiltered int
	totalRecordsProcessed    int
	totalMatches             int
	totalNoMatches           int
	totalMatchPct            float64
	updateDate               string
}

// RunReporter generates sherlock match report
func RunReporter(db *attomdb.Server, fChan <-chan string, logger *log.Logger, cfg *Config) {
	var wg sync.WaitGroup
	mrCh := make(chan matchReport)

	wg.Add(cfg.ConProc)
	go func() {
		wg.Wait()
		close(mrCh)
	}()

	for i := 0; i < cfg.ConProc; i++ {
		go func() {
			defer wg.Done()
			for fips := range fChan {
				RunMatchReport(db, fips, mrCh, logger, cfg)
				UpdateReportStatus(db, fips, logger)
			}
		}()
	}
	if err := bulkLoadMatchOutputFilteredReport(db, mrCh, cfg.MOFRTbl); err != nil {
		logger.Fatalln(err)
	}
}

// RunMatchReport executes SQL Server stored procedure MatchOutputFilterReport_sp
func RunMatchReport(db *attomdb.Server, fips string, rCh chan<- matchReport, logger *log.Logger, cfg *Config) {
	logger.Printf("%s: Generating MatchOutputFilterReport...\n", fips)

	mreport := matchReport{
		fips:       fips,
		updateDate: time.Now().Format(time.RFC3339),
	}

	var TotalBKaggr int
	err := db.AzDB5Thunder.AZRadar.Get(&TotalBKaggr, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_BKaggregate
		WHERE 	FIPS = $1`,
		AltBKFIPS(fips),
	)
	if err != nil {
		logger.Fatalln(err)
	}
	if zeroVal(TotalBKaggr) {
		logger.Printf("%s: BKaggregate is 0\n", fips)
	}
	mreport.totalBKAggregate = TotalBKaggr

	var TotalCLaggr int
	err = db.AzDB5Thunder.AZRadar.Get(&TotalCLaggr, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_CLaggregate
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	if zeroVal(TotalCLaggr) {
		logger.Printf("%s: CLaggregate is 0\n", fips)
	}
	mreport.totalCLAggregate = TotalCLaggr

	var TotalMtchOuput int
	err = db.AzDB5Thunder.AZRadar.Get(&TotalMtchOuput, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_MatchOutput
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	if zeroVal(TotalMtchOuput) {
		logger.Printf("%s: MatchOutput is 0\n", fips)
	}
	mreport.totalMatchOutput = TotalMtchOuput

	var TotalMtchOuputFil int
	err = db.AzDB5Thunder.AZRadar.Get(&TotalMtchOuputFil, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_MatchOutputFiltered
		WHERE 	FIPS = $1`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	if zeroVal(TotalMtchOuputFil) {
		logger.Printf("%s: MatchOutput is 0\n", fips)
	}
	mreport.totalMatchOutputFiltered = TotalMtchOuputFil

	var TotalMatches int
	err = db.AzDB5Thunder.AZRadar.Get(&TotalMatches, `
		SELECT 	Count(*)
		FROM		Radar.dbo.Sherlock_MatchOutputFiltered
		WHERE 	FIPS = $1 AND MatchVal <> 'nomatch'`,
		fips,
	)
	if err != nil {
		logger.Fatalln(err)
	}
	mreport.totalMatches = TotalMatches

	var recordsProcessed int
	recordsProcessed = TotalCLaggr

	if !zeroVal(TotalMatches, TotalCLaggr) {
		mreport.totalRecordsProcessed = recordsProcessed
		mreport.totalNoMatches = recordsProcessed - TotalMatches
		mreport.totalMatchPct = math.Round((float64(TotalMatches)/float64(recordsProcessed))*100*100) / 100
	}

	rCh <- mreport
}
