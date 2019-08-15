package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	attomdb "DG_QA/attomDB"

	"DG_QA/sherlockHY"
)

/*
                             o8o
                             `"'
ooo. .oo.  .oo.    .oooo.   oooo  ooo. .oo.
`888P"Y88bP"Y88b  `P  )88b  `888  `888P"Y88b
 888   888   888   .oP"888   888   888   888
 888   888   888  d8(  888   888   888   888
o888o o888o o888o `Y888""8o o888o o888o o888o
*/

func main() {
	// sherlock.NewLogger opens the log file with the name you pass in, e.g "SherlockHYLog"
	// This will be created in the same location the binnary is executed.
	// If a log file of a similar name already exists, the log file will be opened instead
	errlog, logger := sherlock.NewLogger("SherlockHYLog")
	defer errlog.Close() // closes the log file when function ternimantes

	defer sherlock.FnTracker(time.Now(), "SherlockHY") // Just a timer to see how long the main function executed

	// GOMAXPROCS sets the maximum processors to run
	// by default, it is set to utilize all available cores but can be scalled down if you want to limit CPU utilization
	runtime.GOMAXPROCS(runtime.NumCPU())

	// attomdb.New() establish SQL Server DB connections to be used
	db := attomdb.New()

	// Define CLI flags
	// set the number of counties to load concurrently
	var ConLoad = flag.Int("ConLoad", 10, "Set concurrent Load processes")
	// set the number of counties to process concurrently
	var ConProc = flag.Int("ConProc", 10, "Set concurrent Match processes")
	// set the number of go routine workers to run
	var Workers = flag.Int("Workers", 500, "Set concurrent match workers")
	// set the number records to upload per batch routine
	var BatchSize = flag.Int("BatchQty", 500000, "Set batch insert size")
	// determine the match fields to be compared on the BK dataset
	var BKFlds = flag.String("MatchFlds", "1,2,3", "BlackKnight match fields")
	// determine the match fields to be compared on the CL dataset
	var CLFlds = flag.String("IndexFlds", "1,2,3,4,5", "CoreLogic match fields")

	// set the MatchOutput table
	var MatchOutput = flag.String("MatchOutput", "Radar.dbo.Sherlock_MatchOutput", "Set MatchOutput table")
	// set the MatchOutputFiltered table
	var MatchOutputFiltered = flag.String("MatchOutputFiltered", "Radar.dbo.Sherlock_MatchOutputFiltered", "Set MatchOutput Filtered table")
	// set the MatchOutputFilteredReport table
	var MatchOutputFilteredReport = flag.String("MatchOutputFilteredReport", "Radar.dbo.Sherlock_MatchOutputFilteredReport", "Set MatchOutput Filtered Report table")
	// set verbose flag
	var Verbose = flag.Bool("v", false, "Activate verbose logging")
	flag.Parse()

	// Generate the configuration file
	cfg := &sherlock.Config{}
	cfg.ConLoad = *ConLoad
	cfg.ConProc = *ConProc
	cfg.Workers = *Workers
	cfg.BatchSize = *BatchSize
	cfg.BKFlds = sherlock.ConvStrInt(*BKFlds)
	cfg.CLFlds = sherlock.ConvStrInt(*CLFlds)
	cfg.MOTbl = *MatchOutput
	cfg.MOFTbl = *MatchOutputFiltered
	cfg.MOFRTbl = *MatchOutputFilteredReport
	cfg.Verb = *Verbose

	// RunLoader aggregates BK and CL data into working tables
	fmt.Println("Running Loaders...")
	FipsToLoad := sherlock.InputLoader(db, "Loader", logger)
	sherlock.RunLoader(db, FipsToLoad, logger, cfg)

	// RunMatcher runs the matching process
	fmt.Println("Running Matchers...")
	FipsToMatch := sherlock.InputLoader(db, "Matcher", logger)
	sherlock.RunMatcher(db, FipsToMatch, logger, cfg)

	// RunFilter runs the filter process
	fmt.Println("Running Filters...")
	FipsToFilter := sherlock.InputLoader(db, "Filter", logger)
	sherlock.RunFilter(db, FipsToFilter, logger, cfg)

	// RunReporter generates the reports
	fmt.Println("Running Report...")
	FipsToReporter := sherlock.InputLoader(db, "Report", logger)
	sherlock.RunReporter(db, FipsToReporter, logger, cfg)
}
