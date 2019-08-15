package sherlock

import (
	"log"
	"sort"
	"strconv"
	"sync"

	attomdb "DG_QA/attomDB"
)

/*
 .o88o.  o8o  oooo      .
 888 `"  `"'  `888    .o8
o888oo  oooo   888  .o888oo  .ooooo.  oooo d8b
 888    `888   888    888   d88' `88b `888""8P
 888     888   888    888   888ooo888  888
 888     888   888    888 . 888    .o  888
o888o   o888o o888o   "888" `Y8bod8P' d888b
*/

// priorityOutput defines rulePriority object
type priorityOutput struct {
	score    int
	ruleP1   int
	ruleP2   int
	matchRes *matchResult
}

// RunFilter executes the filter process
func RunFilter(db *attomdb.Server, fipsChan <-chan string, logger *log.Logger, cfg *Config) {
	excep := loadExceptions(db, logger)
	truncs := loadTruncations(db, logger)
	nes := LoadNewEnglandStates(db, logger)

	var wg sync.WaitGroup
	for i := 0; i < cfg.ConProc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fips := range fipsChan {
				pout := rulePriorityFilter(db, fips, excep, truncs, nes, logger, cfg)
				filteredOutput(db, pout, logger, cfg)
				UpdateFilterStatus(db, fips, logger)
			}
		}()
	}
	wg.Wait()
}

// rulePriorityFilter creates a hash map where the Key = SA_Property_ID and the Values
// are the corresponding matching rows.  It is possible for SA_Property_IDs to have
// multiple value rows.
func rulePriorityFilter(db *attomdb.Server, fips string, excep map[exception]struct{}, truncs map[trimKey]trim,
	nes map[string]struct{}, logger *log.Logger, cfg *Config) chan *matchResult {

	filtered := make(chan *matchResult)

	go func() {
		defer close(filtered)

		indexSAPID := make(map[string][]priorityOutput)
		indexDPID := make(map[string][]priorityOutput)

		for mresult := range loadMatchResults(db, fips, logger, cfg) {
			indexSAPID[mresult.SAPID] = append(indexSAPID[mresult.SAPID], priorityOutput{
				ruleP1: setPriority(mresult),
				// ruleP2:   setPrioritySecondary(mresult),
				ruleP2:   mresult.BKRule,
				matchRes: mresult,
			})
		}
		for _, poSAPID := range indexSAPID {
			for _, SAPIDFiltered := range setPriorityRules(db, poSAPID, excep, fips, truncs, nes, logger) {
				indexDPID[SAPIDFiltered.DPID] = append(indexDPID[SAPIDFiltered.DPID], priorityOutput{
					ruleP1: setPriority(SAPIDFiltered),
					// ruleP2:   setPrioritySecondary(SAPIDFiltered),
					ruleP2:   SAPIDFiltered.BKRule,
					matchRes: SAPIDFiltered,
				})
			}
		}
		for _, poDPID := range indexDPID {
			for _, DPIDFiltered := range setPriorityRules(db, poDPID, excep, fips, truncs, nes, logger) {
				filtered <- DPIDFiltered
			}
		}
	}()

	return filtered
}

// setPriorityRules defines match filter rules
func setPriorityRules(db *attomdb.Server, pro []priorityOutput, excep map[exception]struct{},
	fips string, truncs map[trimKey]trim, nes map[string]struct{}, logger *log.Logger) []*matchResult {

	cascade := []*matchResult{}

	matchCondition := false
	for _, ro := range sortRulePriorities(pro) {
		mr := ro.matchRes
		// Ignore values that already have "nomatch"
		if mr.MatchVal == noMatchVal {
			cascade = append(cascade, mr)
			continue
		}
		// If single match condition for array has been satisfied, set all remaining
		// MatchVal value elements in the array to "nomatch"
		if matchCondition {
			mr.MatchVal = noMatchVal
			cascade = append(cascade, mr)
			continue
		}

		// If the match criteria is an exception then
		// set matchCondition to True
		if _, ok := testException(mr, excep); ok {
			cascade = append(cascade, mr)
			matchCondition = true
			continue
		}
		/*
			                     oooo
			                     `888
			oooo d8b oooo  oooo   888   .ooooo.   .oooo.o
			`888""8P `888  `888   888  d88' `88b d88(  "8
			 888      888   888   888  888ooo888 `"Y88b.
			 888      888   888   888  888    .o o.  )88b
			d888b     `V88V"V8P' o888o `Y8bod8P' 8""888P'
		*/
		switch {
		// PRIME to PRIME on Low Level Rules
		case primeprimeLow(mr, truncs, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// PRIME to PRIME on High Level Rules
		case primeprimeHigh(mr, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// PRIME to ALT on Low Level Rules
		case primealtLow(mr, truncs, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// PRIME to ALT on High Level Rules
		case primealtHigh(mr, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// ADDRESS
		case addraddr(pro, mr, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// ALT to ALT
		case altalt(mr, truncs, nes, fips):
			cascade = append(cascade, mr)
			matchCondition = true
		// Default, set MatchVal = "nomatch"
		default:
			mr.MatchVal = noMatchVal
			cascade = append(cascade, mr)
		}
	}

	return cascade
}

// ---------------------------------------------------

// PRIME to PRIME on Low Level Rules
func primeprimeLow(mr *matchResult, truncs map[trimKey]trim, nes map[string]struct{}, fips string) bool {
	if isTruncated(fips, truncs) {
		return false
	}
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if mr.BKCol != primary || mr.CLCol != primary {
		return false
	}
	if mr.BKRule >= highRule || mr.CLRule >= highRule {
		return false
	}
	return true
}

// PRIME to PRIME on High Level Rules
func primeprimeHigh(mr *matchResult, nes map[string]struct{}, fips string) bool {
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if mr.BKCol != primary || mr.CLCol != primary {
		return false
	}
	if emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) {
		return false
	}
	if mr.BKHseNum != mr.CLHseNum {
		return false
	}
	if mr.BKStrName != mr.CLStrName {
		return false
	}
	return true
}

// PRIME to ALT on Low Level Rules
func primealtLow(mr *matchResult, truncs map[trimKey]trim, nes map[string]struct{}, fips string) bool {
	if isTruncated(fips, truncs) {
		return false
	}
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if mr.BKCol != primary && mr.CLCol != primary {
		return false
	}
	if mr.BKRule >= highRule || mr.CLRule >= highRule {
		return false
	}
	return true
}

// PRIME to ALT on High Level Rules
func primealtHigh(mr *matchResult, nes map[string]struct{}, fips string) bool {
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if mr.BKCol != primary && mr.CLCol != primary {
		return false
	}
	if emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) {
		return false
	}
	if mr.BKHseNum != mr.CLHseNum {
		return false
	}
	if mr.BKStrName != mr.CLStrName {
		return false
	}
	return true
}

// ADDR to ADDR Rules
func addraddr(pro []priorityOutput, mr *matchResult, nes map[string]struct{}, fips string) bool {
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if mr.BKRule != addressRule || mr.CLRule != addressRule {
		return false
	}
	if checkMultiAddress(pro) {
		if emptyFields(mr.BKUnitNum, mr.CLUnitNum) {
			return false
		}
		if mr.BKUnitNum != mr.CLUnitNum {
			return false
		}
	}
	return true
}

// ALT to ALT Rules
func altalt(mr *matchResult, truncs map[trimKey]trim, nes map[string]struct{}, fips string) bool {
	if isTruncated(fips, truncs) {
		return false
	}
	if isNEState(fips, nes) && unequalZipCodes(mr.BKZip, mr.CLZip) {
		return false
	}
	if emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) {
		return false
	}
	if mr.BKHseNum != mr.CLHseNum {
		return false
	}
	if mr.BKStrName != mr.CLStrName {
		return false
	}
	return true
}

// ---------------------------------------------------

// unequal validates if BK and CL House Numbers are unequal
func unequal(a, b string) bool {
	if emptyFields(a, b) {
		return true
	}
	if trimS(a) != trimS(b) {
		return true
	}
	return false
}

// unequalZipCodes validates if BK and CL Zip Codes are equal
func unequalZipCodes(BKZip, CLZip string) bool {
	if emptyFields(BKZip, CLZip) {
		return true
	}
	ZipA, err := validateZip(trimS(BKZip))
	if err != nil {
		return true
	}
	ZipB, err := validateZip(trimS(CLZip))
	if err != nil {
		return true
	}
	if trimS(ZipA) != trimS(ZipB) {
		return true
	}
	return false
}

func validateZip(zip string) (string, error) {
	z, err := strconv.Atoi(zip)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(z), nil
}

// sortRulePriorities sorts priorityOutput slice by rulePriority in descending order
func sortRulePriorities(po []priorityOutput) []priorityOutput {
	sort.Sort(byP1P2(po))
	return po
}

// Implement Sorting rule, sort by rulePriority1 then rulePriority2
type byP1P2 []priorityOutput

func (o byP1P2) Len() int      { return len(o) }
func (o byP1P2) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o byP1P2) Less(i, j int) bool {
	if o[i].ruleP1 == o[j].ruleP1 {
		return o[i].ruleP2 < o[j].ruleP2
	}
	return o[i].ruleP1 < o[j].ruleP1
}

// Order or Priority:
// Priority 1 = Primary == Primary, SMC ["StrNumber", "StrName", "UnitNumber"]
// Priority 2 = Primary == Primary, SMC ["StrNumber", "StrName"]
// Priority 3 = Primary == Primary, SMC ["StrNumber"]
// Priority 4 = Primary == Primary
// Priority 5 = Primary == Alternate, SMC ["StrNumber", "StrName", "UnitNumber"]
// Priority 6 = Primary == Alternate, SMC ["StrNumber", "StrName"]
// Priority 7 = Primary == Alternate, SMC ["StrNumber"]
// Priority 8 = Primary == Alternate
// Priority 9 = Address == Address, SMC ["UnitNumber"]
// Priority 10 = Address == Address
// Priority 11 = Alternate == Alternate, SMC ["StrNumber", "StrName", "UnitNumber"]
// Priority 12 = Alternate == Alternate, SMC ["StrNumber", "StrName"]
// Priority 13 = Alternate == Alternate, SMC ["StrNumber"]
// Priority 14 = Alternate == Alternate
// Priority 15 = NoMatch

// setPriority assigns rule priority numbers
func setPriority(mr *matchResult) int {
	switch {
	case mr.BKCol == primary && mr.CLCol == primary &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName && mr.BKUnitNum == mr.CLUnitNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName, mr.BKUnitNum, mr.CLUnitNum):
		return 1
	case mr.BKCol == primary && mr.CLCol == primary &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 2
	case mr.BKCol == primary && mr.CLCol == primary &&
		mr.BKHseNum == mr.CLHseNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 3
	case mr.BKCol == primary && mr.CLCol == primary &&
		emptyFields(mr.BKHseNum) &&
		emptyFields(mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 4
	case (mr.BKCol == primary || mr.CLCol == primary) &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName && mr.BKUnitNum == mr.CLUnitNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName, mr.BKUnitNum, mr.CLUnitNum):
		return 5
	case (mr.BKCol == primary || mr.CLCol == primary) &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 6
	case (mr.BKCol == primary || mr.CLCol == primary) &&
		mr.BKHseNum == mr.CLHseNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 7
	case (mr.BKCol == primary || mr.CLCol == primary) &&
		emptyFields(mr.BKHseNum) &&
		emptyFields(mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 8
	case mr.BKRule == addressRule && mr.CLRule == addressRule &&
		mr.BKUnitNum == mr.CLUnitNum &&
		!emptyFields(mr.BKUnitNum, mr.CLUnitNum):
		return 9
	case mr.BKRule == addressRule && mr.CLRule == addressRule:
		return 10
	case mr.BKCol != primary && mr.CLCol != primary &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName && mr.BKUnitNum == mr.CLUnitNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName, mr.BKUnitNum, mr.CLUnitNum):
		return 11
	case mr.BKCol != primary && mr.CLCol != primary &&
		mr.BKHseNum == mr.CLHseNum && mr.BKStrName == mr.CLStrName &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum, mr.BKStrName, mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 12
	case mr.BKCol != primary && mr.CLCol != primary &&
		mr.BKHseNum == mr.CLHseNum &&
		!emptyFields(mr.BKHseNum, mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 13
	case mr.BKCol != primary && mr.CLCol != primary &&
		emptyFields(mr.BKHseNum) &&
		emptyFields(mr.CLHseNum) &&
		emptyFields(mr.BKStrName) &&
		emptyFields(mr.CLStrName) &&
		emptyFields(mr.BKUnitNum) &&
		emptyFields(mr.CLUnitNum):
		return 14
	default:
		return 15
	}
}

func setPrioritySecondary(mr *matchResult) int {
	if mr.BKHseNum == mr.CLHseNum && !emptyFields(mr.BKHseNum) && !emptyFields(mr.CLHseNum) {
		return 1
	}
	if mr.BKStrName == mr.CLStrName && !emptyFields(mr.BKStrName) && !emptyFields(mr.CLStrName) {
		return 2
	}
	return 3
}

// checkMultiAddress checks if there are multiple address match conditions
func checkMultiAddress(pro []priorityOutput) bool {
	chk := make(map[int]int)
	for _, p := range pro {
		chk[p.matchRes.BKRule]++
	}
	if chk[addressRule] > 1 {
		return true
	}
	return false
}

// filteredOutput performs a batch output of filtered results
func filteredOutput(db *attomdb.Server, out <-chan *matchResult, logger *log.Logger, cfg *Config) {
	batch := make([]*matchResult, 0, cfg.BatchSize)
	for mres := range out {
		batch = append(batch, mres)
		if len(batch) >= cfg.BatchSize {
			err := bulkLoadMatchesFiltered(db, batch, cfg.MOFTbl)
			if err != nil {
				logger.Fatalln(err)
			}
			batch = make([]*matchResult, 0, cfg.BatchSize)
		}
	}
	if len(batch) > 0 {
		err := bulkLoadMatchesFiltered(db, batch, cfg.MOFTbl)
		if err != nil {
			logger.Fatalln(err)
		}
	}
}

func isTruncated(fips string, truncs map[trimKey]trim) bool {
	var ok bool
	_, ok = truncs[trimKey{FIPS: fips, src: "BK"}]
	if ok {
		return true
	}
	_, ok = truncs[trimKey{FIPS: fips, src: "CL"}]
	if ok {
		return true
	}
	return false
}

func isNEState(fips string, nes map[string]struct{}) bool {
	var ok bool
	_, ok = nes[fips]
	if ok {
		return true
	}
	return false
}
