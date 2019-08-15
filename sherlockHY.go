package sherlock

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	attomdb "DG_QA/attomDB"
)

/*
         oooo                           oooo                      oooo
         `888                           `888                      `888
 .oooo.o  888 .oo.    .ooooo.  oooo d8b  888   .ooooo.   .ooooo.   888  oooo
d88(  "8  888P"Y88b  d88' `88b `888""8P  888  d88' `88b d88' `"Y8  888 .8P'
`"Y88b.   888   888  888ooo888  888      888  888   888 888        888888.
o.  )88b  888   888  888    .o  888      888  888   888 888   .o8  888 `88b.
8""888P' o888o o888o `Y8bod8P' d888b    o888o `Y8bod8P' `Y8bod8P' o888o o888o
*/

const (
	nullInt     = -1
	nullchar    = "-1"
	noMatchVal  = "nomatch"
	primary     = 1
	lowRule     = 1
	highRule    = 5
	truncRule   = 6
	remZeroRule = 7
	addressRule = 9000
)

// RunMatcher executes matcher process
func RunMatcher(db *attomdb.Server, fipsChan <-chan string, logger *log.Logger, cfg *Config) {
	truncs := loadTruncations(db, logger)
	var wg sync.WaitGroup
	for i := 0; i < cfg.ConProc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fips := range fipsChan {
				defer TimeTracker(time.Now(), fmt.Sprintf("%s: RegexMatcher", fips), logger)
				indxr := createIndex(db, fips, truncs, logger, cfg)
				bkChan := loadBKData(db, AltBKFIPS(fips), logger, cfg)
				matchesChan := runMatchers(db, fips, indxr, bkChan, truncs, logger, cfg)
				uploadMatches(db, matchesChan, fips, logger, cfg)
				UpdateMatcherStatus(db, fips, logger)
			}
		}()
	}
	wg.Wait()
}

/*
 o8o                    .o8
 `"'                   "888
oooo  ooo. .oo.    .oooo888   .ooooo.  oooo    ooo  .ooooo.  oooo d8b
`888  `888P"Y88b  d88' `888  d88' `88b  `88b..8P'  d88' `88b `888""8P
 888   888   888  888   888  888ooo888    Y888'    888ooo888  888
 888   888   888  888   888  888    .o  .o8"'88b   888    .o  888
o888o o888o o888o `Y8bod88P" `Y8bod8P' o88'   888o `Y8bod8P' d888b
*/

// Instantiate Indexer and load data from DQuick staging
func createIndex(db *attomdb.Server, fips string, truncs map[trimKey]trim, logger *log.Logger, cfg *Config) *indexerGCV {
	logger.Println(fmt.Sprintf("%s: Generating Index...", fips))
	indxr := newIndexerGCV()
	for cl := range loadCLData(db, fips, logger) {
		loadFieldVals(cl, indxr, truncs, fips, logger, cfg)
		loadAddrVals(cl, indxr, logger, cfg)
	}
	return indxr
}

// Load address fields into Indexer
func loadAddrVals(cl *clData, indxr *indexerGCV, logger *log.Logger, cfg *Config) {
	if emptyFields(cl.SaSITEHOUSENBR, cl.SaSITESTREETNAME, cl.SaSITECITYSTATE) {
		return
	}

	HseNum := cl.SaSITEHOUSENBR
	StrtName := cl.SaSITESTREETNAME
	City := cl.SaSITECITY
	State := cl.SaSITESTATE
	UnitNum := cl.SaSITEUNITVAL

	indxr.Set(gcvKey{
		HseNum:   HseNum,
		StrtName: StrtName,
		City:     City,
		State:    State,
		UnitNum:  UnitNum,
	}, gcvVal{
		Col:    nullInt,
		SAPID:  cl.SAPID,
		HseNum: HseNum,
		Zip:    cl.SaSITEZIP,
	})
}

// loadFieldVals loops through all the CL struct index numbers then checks if field is valid for indexing
func loadFieldVals(cl *clData, indxr *indexerGCV, truncs map[trimKey]trim, fips string, logger *log.Logger, cfg *Config) {
	clrv := reflect.ValueOf(cl).Elem()
	for colID := 0; colID < clrv.NumField(); colID++ {
		if notMatchField(colID, cfg.CLFlds) {
			continue
		}
		loadGCVal(cl, clrv, indxr, colID, truncs, fips, logger, cfg)
	}
}

// loadGCVal extracts the field value through the index ID then passes value to the pattern generator
func loadGCVal(cl *clData, clrv reflect.Value, indxr *indexerGCV, colID int, truncs map[trimKey]trim, fips string, logger *log.Logger, cfg *Config) {
	clv := clrv.Field(colID).String()
	if emptyFields(clv) {
		return
	}
	clv = ssep(clv)

	var tRule int
	clv, ok := truncField(fips, clv, "CL", colID, truncs)
	if ok {
		tRule = truncRule
	}

	indxr.Set(gcvKey{
		GCV: parseGCV(clv),
	}, gcvVal{
		Value:   clv,
		Col:     colID,
		Rule:    tRule,
		SAPID:   cl.SAPID,
		HseNum:  cl.SaSITEHOUSENBR,
		StrName: cl.SaSITESTREETNAME,
		UnitNum: cl.SaSITEUNITVAL,
		Zip:     cl.SaSITEZIP,
	})
}

/*
                                .             oooo
                              .o8             `888
ooo. .oo.  .oo.    .oooo.   .o888oo  .ooooo.   888 .oo.    .ooooo.  oooo d8b
`888P"Y88bP"Y88b  `P  )88b    888   d88' `"Y8  888P"Y88b  d88' `88b `888""8P
 888   888   888   .oP"888    888   888        888   888  888ooo888  888
 888   888   888  d8(  888    888 . 888   .o8  888   888  888    .o  888
o888o o888o o888o `Y888""8o   "888" `Y8bod8P' o888o o888o `Y8bod8P' d888b
*/

// runMatchers loads BK struct values then passes it to the field matchers
func runMatchers(db *attomdb.Server, fips string, indxr *indexerGCV, bkChan <-chan *bkData, truncs map[trimKey]trim, logger *log.Logger, cfg *Config) chan *matchResult {
	logger.Println(fmt.Sprintf("%s: Running RegexMatcher...", fips))
	matches := make(chan *matchResult, cfg.Workers)
	go func() {
		defer close(matches)
		var wg sync.WaitGroup
		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for bk := range bkChan {
					if rmatches, ok := runMatchFinder(bk, indxr, fips, truncs, logger, cfg); ok {
						for _, rmatch := range rmatches {
							matches <- rmatch
						}
						continue
					}
					matches <- &matchResult{
						FIPS:     fips,
						MatchVal: noMatchVal,
						DPID:     bk.DPID,
						SAPID:    nullchar,
						BKHseNum: nullchar,
						CLHseNum: nullchar,
						BKCol:    nullInt,
						CLCol:    nullInt,
						BKRule:   nullInt,
						CLRule:   nullInt,
						BKZip:    nullchar,
						CLZip:    nullchar,
					}
				}
			}()
		}
		wg.Wait()
	}()
	return matches
}

// runMatchFinder loops through all the BK struct index numbers then checks if field is valid for matching
// if no match found, process through addressMatcher func
func runMatchFinder(bk *bkData, indxr *indexerGCV, fips string, truncs map[trimKey]trim, logger *log.Logger, cfg *Config) ([]*matchResult, bool) {
	bkrv := reflect.ValueOf(bk).Elem()
	for colID := 0; colID < bkrv.NumField(); colID++ {
		if notMatchField(colID, cfg.BKFlds) {
			continue
		}
		if regexMatch, ok := regexMatcher(bk, bkrv, indxr, colID, fips, truncs, logger, cfg); ok {
			return regexMatch, true
		}
	}
	if addrMatch, ok := addressMatcher(bk, indxr, fips, logger, cfg); ok {
		return addrMatch, true
	}
	return nil, false
}

// addressMatcher finds match by address fields
func addressMatcher(bk *bkData, indxr *indexerGCV, fips string, logger *log.Logger, cfg *Config) ([]*matchResult, bool) {
	if emptyFields(bk.PropHOUSENBR, bk.PropSTREETNAME, bk.PropCITY, bk.PropSTATE) {
		return nil, false
	}

	HseNum := bk.PropHOUSENBR
	StrtName := bk.PropSTREETNAME
	City := bk.PropCITY
	State := bk.PropSTATE
	UnitNum := bk.PropUNITNBR

	mr := make([]*matchResult, 0)

	if addrVal, ok := indxr.Get(gcvKey{
		HseNum:   HseNum,
		StrtName: StrtName,
		City:     City,
		State:    State,
		UnitNum:  UnitNum,
	}); ok {
		for _, addr := range addrVal {
			mr = append(mr, &matchResult{
				FIPS: fips,
				MatchVal: fmt.Sprintf("%s %s %s %s %s",
					HseNum,
					StrtName,
					City,
					State,
					UnitNum,
				),
				DPID:     bk.DPID,
				SAPID:    addr.SAPID,
				BKCol:    nullInt,
				CLCol:    nullInt,
				BKRule:   addressRule,
				CLRule:   addressRule,
				BKHseNum: HseNum,
				CLHseNum: addr.HseNum,
				BKZip:    bk.PropZIP,
				CLZip:    addr.Zip,
			})
		}
		return mr, true
	}
	return nil, false
}

// ruleMatcher extracts the field value through the index ID then passes value to the pattern generator
func regexMatcher(bk *bkData, bkrv reflect.Value, indxr *indexerGCV, colID int, fips string, truncs map[trimKey]trim, logger *log.Logger, cfg *Config) ([]*matchResult, bool) {
	bkf := bkrv.Field(colID).String()
	if emptyFields(bkf) {
		return nil, false
	}

	bkf = ssep(bkf)
	bkf, isTruncated := truncField(fips, bkf, "BK", colID, truncs)

	mr := make([]*matchResult, 0)

	if gcVals, exists := indxr.Get(gcvKey{
		GCV: parseGCV(bkf),
	}); exists {
		for _, clf := range gcVals {
			if rule, ok := regexMatch(bkf, clf.Value, isTruncated); ok {
				if isTruncated {
					rule = truncRule
				}
				// generate input and output masks for UMS
				InputMask, OutputMask := generateMask(bkf, clf.Value, rule)
				// Check if field was truncated
				mr = append(mr, &matchResult{
					FIPS:       fips,
					MatchVal:   bkf,
					DPID:       bk.DPID,
					SAPID:      clf.SAPID,
					BKCol:      colID,
					CLCol:      clf.Col,
					BKHseNum:   bk.PropHOUSENBR,
					CLHseNum:   clf.HseNum,
					BKStrName:  bk.PropSTREETNAME,
					CLStrName:  clf.StrName,
					BKUnitNum:  bk.PropUNITNBR,
					CLUnitNum:  clf.UnitNum,
					BKRule:     rule,
					CLRule:     clf.Rule,
					BKZip:      bk.PropZIP,
					CLZip:      clf.Zip,
					InputMask:  InputMask,
					OutputMask: OutputMask,
				})
			}
		}
		return mr, true
	}
	return nil, false
}

// --------------------------------------------------

// uploadMatches loads match results to dbwork.dbo.Sherlock_MatchOutput table
func uploadMatches(db *attomdb.Server, matches <-chan *matchResult, fips string, logger *log.Logger, cfg *Config) {
	batch := make([]*matchResult, 0, cfg.BatchSize)
	for match := range matches {
		batch = append(batch, match)
		if len(batch) >= cfg.BatchSize {
			err := bulkLoadMatches(db, batch, cfg.MOTbl)
			if err != nil {
				logger.Fatalln(err)
			}
			batch = make([]*matchResult, 0, cfg.BatchSize)
		}
	}
	if len(batch) > 0 {
		err := bulkLoadMatches(db, batch, cfg.MOTbl)
		if err != nil {
			logger.Fatalln(err)
		}
	}
}
