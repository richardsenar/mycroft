package sherlock

import (
	"log"
	"time"

	attomdb "DG_QA/attomDB"
)

/*
             .                 .
           .o8               .o8
 .oooo.o .o888oo  .oooo.   .o888oo oooo  oooo   .oooo.o
d88(  "8   888   `P  )88b    888   `888  `888  d88(  "8
`"Y88b.    888    .oP"888    888    888   888  `"Y88b.
o.  )88b   888 . d8(  888    888 .  888   888  o.  )88b
8""888P'   "888" `Y888""8o   "888"  `V88V"V8P' 8""888P'
*/

// UpdateLoaderStatus updates Status and LastUpdate for processed records on Sherlock_FIPSInput table
func UpdateLoaderStatus(db *attomdb.Server, fips string, logger *log.Logger) {
	logger.Printf("%s: Updating Loader Status...\n", fips)
	db.AzDB5Thunder.AZRadar.MustExec(
		`UPDATE Radar.dbo.Sherlock_FIPSInput
		SET Loader = $1 WHERE FIPS = $2`,
		time.Now().Format(time.RFC3339),
		fips,
	)
}

// UpdateMatcherStatus updates Status and LastUpdate for processed records on Sherlock_FIPSInput table
func UpdateMatcherStatus(db *attomdb.Server, fips string, logger *log.Logger) {
	logger.Printf("%s: Updating Matcher Status...\n", fips)
	db.AzDB5Thunder.AZRadar.MustExec(
		`UPDATE Radar.dbo.Sherlock_FIPSInput
		SET Matcher = $1 WHERE FIPS = $2`,
		time.Now().Format(time.RFC3339),
		fips,
	)
}

// UpdateFilterStatus updates Status and LastUpdate for processed records on Sherlock_FIPSInput table
func UpdateFilterStatus(db *attomdb.Server, fips string, logger *log.Logger) {
	logger.Printf("%s: Updating Filter Status...\n", fips)
	db.AzDB5Thunder.AZRadar.MustExec(
		`UPDATE Radar.dbo.Sherlock_FIPSInput
		SET Filter = $1 WHERE FIPS = $2`,
		time.Now().Format(time.RFC3339),
		fips,
	)
}

// UpdateReportStatus updates Status and LastUpdate for processed records on Sherlock_FIPSInput table
func UpdateReportStatus(db *attomdb.Server, fips string, logger *log.Logger) {
	logger.Printf("%s: Updating Report Status...\n", fips)
	db.AzDB5Thunder.AZRadar.MustExec(
		`UPDATE Radar.dbo.Sherlock_FIPSInput
		SET Report = $1 WHERE FIPS = $2`,
		time.Now().Format(time.RFC3339),
		fips,
	)
}
