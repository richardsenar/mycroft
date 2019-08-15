package sherlock

import (
	"time"

	"github.com/denisenkom/go-mssqldb"

	attomdb "DG_QA/attomDB"
)

/*
 .o8                   oooo  oooo        ooooo                                  .o8
"888                   `888  `888        `888'                                 "888
 888oooo.  oooo  oooo   888   888  oooo   888          .ooooo.   .oooo.    .oooo888   .ooooo.  oooo d8b  .oooo.o
 d88' `88b `888  `888   888   888 .8P'    888         d88' `88b `P  )88b  d88' `888  d88' `88b `888""8P d88(  "8
 888   888  888   888   888   888888.     888         888   888  .oP"888  888   888  888ooo888  888     `"Y88b.
 888   888  888   888   888   888 `88b.   888       o 888   888 d8(  888  888   888  888    .o  888     o.  )88b
 `Y8bod8P'  `V88V"V8P' o888o o888o o888o o888ooooood8 `Y8bod8P' `Y888""8o `Y8bod88P" `Y8bod8P' d888b    8""888P'
*/

// bulkLoadMatches initiates Output bulk upload process
func bulkLoadMatches(db *attomdb.Server, matches []*matchResult, matchOutputTable string) error {
	txn, err := db.AzDB5Thunder.AZRadar.Begin()
	if err != nil {
		return err
	}
	stmt, err := txn.Prepare(mssql.CopyIn(matchOutputTable, mssql.BulkOptions{},
		"FIPS",
		"MatchVal",
		"BKCol",
		"DPID",
		"BKRule",
		"BKHseNum",
		"BKStrName",
		"BKUnitNum",
		"CLCol",
		"SAPID",
		"CLRule",
		"CLHseNum",
		"CLStrName",
		"CLUnitNum",
		"BKZip",
		"CLZip",
		"InputMask",
		"OutputMask",
	))
	if err != nil {
		return err
	}
	for _, match := range matches {
		_, err = stmt.Exec(
			match.FIPS,
			match.MatchVal,
			match.BKCol,
			match.DPID,
			match.BKRule,
			match.BKHseNum,
			match.BKStrName,
			match.BKUnitNum,
			match.CLCol,
			match.SAPID,
			match.CLRule,
			match.CLHseNum,
			match.CLStrName,
			match.CLUnitNum,
			match.BKZip,
			match.CLZip,
			match.InputMask,
			match.OutputMask,
		)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

// =======================================================================================

// batchOutputFiltered initiates Output bulk upload process
func bulkLoadMatchesFiltered(db *attomdb.Server, matches []*matchResult, matchOutputFilteredTable string) error {
	txn, err := db.AzDB5Thunder.AZRadar.Begin()
	if err != nil {
		return err
	}
	stmt, err := txn.Prepare(mssql.CopyIn(matchOutputFilteredTable, mssql.BulkOptions{},
		"FIPS",
		"MatchVal",
		"BKCol",
		"DPID",
		"BKRule",
		"BKHseNum",
		"BKStrName",
		"BKUnitNum",
		"CLCol",
		"SAPID",
		"CLRule",
		"CLHseNum",
		"CLStrName",
		"CLUnitNum",
		"BKZip",
		"CLZip",
		"InputMask",
		"OutputMask",
	))
	if err != nil {
		return err
	}
	for _, match := range matches {
		_, err = stmt.Exec(
			match.FIPS,
			match.MatchVal,
			match.BKCol,
			match.DPID,
			match.BKRule,
			match.BKHseNum,
			match.BKStrName,
			match.BKUnitNum,
			match.CLCol,
			match.SAPID,
			match.CLRule,
			match.CLHseNum,
			match.CLStrName,
			match.CLUnitNum,
			match.BKZip,
			match.CLZip,
			match.InputMask,
			match.OutputMask,
		)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

// =======================================================================================

// bulkLoadMatchOutputFilteredReport initiates Output bulk upload process
func bulkLoadMatchOutputFilteredReport(db *attomdb.Server, rCh <-chan matchReport, moRepTbl string) error {
	txn, err := db.AzDB5Thunder.AZRadar.Begin()
	if err != nil {
		return err
	}
	stmt, err := txn.Prepare(mssql.CopyIn(moRepTbl, mssql.BulkOptions{},
		"FIPS",
		"Total_BK_Aggregate",
		"Total_CL_Aggregate",
		"Total_MatchOutput",
		"Total_MatchOutputFiltered",
		"Total_Records_Processed",
		"Total_Matches",
		"Total_NoMatches",
		"Total_MatchPct",
		"UpdateDate",
	))
	if err != nil {
		return err
	}
	for r := range rCh {
		udate, err := time.Parse("2006-01-02", TrimDate(r.updateDate))
		if err != nil {
			return err
		}
		_, err = stmt.Exec(
			r.fips,
			r.totalBKAggregate,
			r.totalCLAggregate,
			r.totalMatchOutput,
			r.totalMatchOutputFiltered,
			r.totalRecordsProcessed,
			r.totalMatches,
			r.totalNoMatches,
			r.totalMatchPct,
			udate,
		)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}
