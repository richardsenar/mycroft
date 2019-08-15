package sherlock

import (
	attomdb "DG_QA/attomDB"
	"log"
	"sync"
)

/*
 .o8                   oooo  oooo        ooooooooo.                                    .
"888                   `888  `888        `888   `Y88.                                .o8
 888oooo.  oooo  oooo   888   888  oooo   888   .d88'  .ooooo.   .oooo.o  .ooooo.  .o888oo
 d88' `88b `888  `888   888   888 .8P'    888ooo88P'  d88' `88b d88(  "8 d88' `88b   888
 888   888  888   888   888   888888.     888`88b.    888ooo888 `"Y88b.  888ooo888   888
 888   888  888   888   888   888 `88b.   888  `88b.  888    .o o.  )88b 888    .o   888 .
 `Y8bod8P'  `V88V"V8P' o888o o888o o888o o888o  o888o `Y8bod8P' 8""888P' `Y8bod8P'   "888"
*/

// bulkResetAggregates executes BL & CL Bulk reset stored procs
func bulkResetAggregates(db *attomdb.Server, logger *log.Logger) error {
	var wg sync.WaitGroup
	var errBK error
	var errCL error

	logger.Println("Resetting BK-CL Aggregate Table...")
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errBK = db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_BulkResetAggregateBK_sp`)
	}()
	go func() {
		defer wg.Done()
		_, errCL = db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_BulkResetAggregateCL_sp`)
	}()
	wg.Wait()

	if errBK != nil {
		return errBK
	}
	if errCL != nil {
		return errCL
	}
	return nil
}

// =======================================================================================

// bulkResetMatchOutput executes BulkResetMatchOutput reset stored procs
func bulkResetMatchOutput(db *attomdb.Server, logger *log.Logger) error {
	logger.Println("Resetting MatchOutput Table...")
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_BulkResetMatchOutput_sp`)
	if err != nil {
		return err
	}
	return nil
}

// bulkResetMatchOutputFiltered executes BulkResetMatchOutputFiltered reset stored procs
func bulkResetMatchOutputFiltered(db *attomdb.Server, logger *log.Logger) error {
	logger.Println("Resetting MatchOutputFiltered Table...")
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_BulkResetMatchOutputFiltered_sp`)
	if err != nil {
		return err
	}
	return nil
}

// bulkResetMatchOutputFilteredReport executes BulkResetMatchOutputFilteredReport reset stored procs
func bulkResetMatchOutputFilteredReport(db *attomdb.Server, logger *log.Logger) error {
	logger.Println("Resetting MatchOutputFiltered Table...")
	_, err := db.AzDB5Thunder.AZRadar.Exec(`EXEC Radar.dbo.Sherlock_BulkResetMatchOutputFilteredReport_sp`)
	if err != nil {
		return err
	}
	return nil
}
