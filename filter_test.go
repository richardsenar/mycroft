package sherlock

import (
	"testing"

	attomdb "DG_QA/attomDB"
)

/*
    .                          .
  .o8                        .o8
.o888oo  .ooooo.   .oooo.o .o888oo  .oooo.o
  888   d88' `88b d88(  "8   888   d88(  "8
  888   888ooo888 `"Y88b.    888   `"Y88b.
  888 . 888    .o o.  )88b   888 . o.  )88b
  "888" `Y8bod8P' 8""888P'   "888" 8""888P'
*/
func TestSetPriorityRules(t *testing.T) {
	errlog, logger := NewLogger("SherlockHYLog")
	defer errlog.Close()
	db := attomdb.New()
	exceptions := loadExceptions(db, logger)

	var a []*matchResult
	a = append(a, &matchResult{MatchVal: "aaa", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1})
	a = append(a, &matchResult{MatchVal: "nomatch"})
	a = append(a, &matchResult{MatchVal: "nomatch"})
	a = append(a, &matchResult{MatchVal: "nomatch"})

	var b []*matchResult
	b = append(b, &matchResult{MatchVal: "bbb", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"})
	b = append(b, &matchResult{MatchVal: "nomatch"})
	b = append(b, &matchResult{MatchVal: "nomatch"})
	b = append(b, &matchResult{MatchVal: "nomatch"})

	// var c []*matchResult
	// c = append(c, &matchResult{MatchVal: "ccc", BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 1})
	// c = append(c, &matchResult{MatchVal: "nomatch"})
	// c = append(c, &matchResult{MatchVal: "nomatch"})
	// c = append(c, &matchResult{MatchVal: "nomatch"})

	// var d []*matchResult
	// d = append(d, &matchResult{MatchVal: "ddd", BKCol: 1, CLCol: 2, BKRule: 10, CLRule: 10, BKHseNum: "11", CLHseNum: "11"})
	// d = append(d, &matchResult{MatchVal: "nomatch"})
	// d = append(d, &matchResult{MatchVal: "nomatch"})
	// d = append(d, &matchResult{MatchVal: "nomatch"})

	// var e []*matchResult
	// e = append(e, &matchResult{MatchVal: "eee", BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0})
	// e = append(e, &matchResult{MatchVal: "nomatch"})
	// e = append(e, &matchResult{MatchVal: "nomatch"})
	// e = append(e, &matchResult{MatchVal: "nomatch"})

	// var f []*matchResult
	// f = append(f, &matchResult{MatchVal: "fff", BKCol: 2, CLCol: 1, BKRule: 0, CLRule: 1})
	// f = append(f, &matchResult{MatchVal: "nomatch"})
	// f = append(f, &matchResult{MatchVal: "nomatch"})
	// f = append(f, &matchResult{MatchVal: "nomatch"})

	// var g []*matchResult
	// g = append(g, &matchResult{MatchVal: "Addressggg", BKRule: 9000, CLRule: 9000})
	// g = append(g, &matchResult{MatchVal: "nomatch"})
	// g = append(g, &matchResult{MatchVal: "nomatch"})
	// g = append(g, &matchResult{MatchVal: "nomatch"})

	// var h []*matchResult
	// h = append(h, &matchResult{MatchVal: "Addresshhh", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// h = append(h, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// h = append(h, &matchResult{MatchVal: "nomatch"})
	// h = append(h, &matchResult{MatchVal: "nomatch"})

	// var i []*matchResult
	// i = append(i, &matchResult{MatchVal: "iii", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1})
	// i = append(i, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// i = append(i, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// i = append(i, &matchResult{MatchVal: "nomatch"})

	// var j []*matchResult
	// j = append(j, &matchResult{MatchVal: "jjj", BKCol: 1, CLCol: 3, BKRule: 1, CLRule: 20, BKHseNum: "22", CLHseNum: "22"})
	// j = append(j, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// j = append(j, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// j = append(j, &matchResult{MatchVal: "nomatch"})

	// var k []*matchResult
	// k = append(k, &matchResult{MatchVal: "kkk", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 0})
	// k = append(k, &matchResult{MatchVal: "nomatch", BKCol: 1, CLCol: 3, BKRule: 1, CLRule: 20, BKHseNum: "22", CLHseNum: ""})
	// k = append(k, &matchResult{MatchVal: "nomatch", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"})
	// k = append(k, &matchResult{MatchVal: "nomatch"})

	// var o []*matchResult
	// o = append(o, &matchResult{MatchVal: "90090400204000", BKCol: 2, CLCol: 2, BKRule: 11, CLRule: 0, BKHseNum: "301", CLHseNum: "301"})

	tcase := []struct {
		input    []priorityOutput
		expected []*matchResult
	}{
		{[]priorityOutput{
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
			priorityOutput{ruleP1: 1, matchRes: &matchResult{MatchVal: "aaa", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}},
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		}, a},
		{[]priorityOutput{
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
			priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
			priorityOutput{ruleP1: 1, matchRes: &matchResult{MatchVal: "bbb", BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}},
		}, b},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "ccc", BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 1}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// }, c},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "ddd", BKCol: 1, CLCol: 2, BKRule: 10, CLRule: 10, BKHseNum: "11", CLHseNum: "11"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// }, d},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "eee", BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// }, e},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "fff", BKCol: 2, CLCol: 1, BKRule: 0, CLRule: 1}},
		// }, f},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addressggg", BKRule: 9000, CLRule: 9000}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "bbb"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// }, g},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "ccc"}},
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addresshhh", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addresshhh", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "aaa"}},
		// }, h},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Address", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Address", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 1, matchRes: &matchResult{MatchVal: "iii", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "nomatch"}},
		// }, i},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addressjjj", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addressjjj", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "jjj", BKCol: 1, CLCol: 3, BKRule: 1, CLRule: 20, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "nomatch"}},
		// }, j},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 3, matchRes: &matchResult{MatchVal: "Addresskkk", BKRule: 9000, CLRule: 9000, BKHseNum: "22", CLHseNum: "22"}},
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "ddd", BKCol: 1, CLCol: 3, BKRule: 1, CLRule: 20, BKHseNum: "22", CLHseNum: ""}},
		// 	priorityOutput{ruleP1: 1, matchRes: &matchResult{MatchVal: "kkk", BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 0}},
		// 	priorityOutput{ruleP1: 4, matchRes: &matchResult{MatchVal: "nomatch"}},
		// }, k},
		// {[]priorityOutput{
		// 	priorityOutput{ruleP1: 2, matchRes: &matchResult{MatchVal: "90090400204000", BKCol: 2, CLCol: 2, BKRule: 11, CLRule: 0, BKHseNum: "301", CLHseNum: "301"}},
		// }, o},
	}

	for _, p := range tcase {
		got := make([]*matchResult, 0)
		for _, y := range setPriorityRules(db, p.input, exceptions, "", nil, nil, logger) {
			got = append(got, y)
		}
		for i, g := range got {
			if *g != *p.expected[i] {
				t.Errorf("expected \"%v\", got \"%v\"\n", *g, *p.expected[i])
			}
		}
	}
}

func TestRuleOrder(t *testing.T) {
	tcase := []struct {
		id       int
		po       []priorityOutput
		expected []priorityOutput
	}{
		{1, []priorityOutput{
			priorityOutput{ruleP1: 2, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 3, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 4, ruleP2: 2},
			priorityOutput{ruleP1: 4, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 1},
		}, []priorityOutput{
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 2, ruleP2: 2},
			priorityOutput{ruleP1: 3, ruleP2: 2},
			priorityOutput{ruleP1: 4, ruleP2: 1},
			priorityOutput{ruleP1: 4, ruleP2: 2},
		}},
		{2, []priorityOutput{
			priorityOutput{ruleP1: 2, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 3, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 4, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 1},
		}, []priorityOutput{
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 1, ruleP2: 2},
			priorityOutput{ruleP1: 2, ruleP2: 1},
			priorityOutput{ruleP1: 3, ruleP2: 2},
			priorityOutput{ruleP1: 4, ruleP2: 1},
		}},
		{3, []priorityOutput{
			priorityOutput{ruleP1: 2, ruleP2: 5},
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 3, ruleP2: 5},
			priorityOutput{ruleP1: 1, ruleP2: 7},
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 4, ruleP2: 5},
			priorityOutput{ruleP1: 1, ruleP2: 5},
		}, []priorityOutput{
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 5},
			priorityOutput{ruleP1: 1, ruleP2: 7},
			priorityOutput{ruleP1: 2, ruleP2: 5},
			priorityOutput{ruleP1: 3, ruleP2: 5},
			priorityOutput{ruleP1: 4, ruleP2: 5},
		}},
		{4, []priorityOutput{
			priorityOutput{ruleP1: 13, ruleP2: 5},
			priorityOutput{ruleP1: 12, ruleP2: 1},
			priorityOutput{ruleP1: 11, ruleP2: 5},
			priorityOutput{ruleP1: 8, ruleP2: 7},
			priorityOutput{ruleP1: 5, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 5},
		}, []priorityOutput{
			priorityOutput{ruleP1: 1, ruleP2: 1},
			priorityOutput{ruleP1: 1, ruleP2: 5},
			priorityOutput{ruleP1: 5, ruleP2: 1},
			priorityOutput{ruleP1: 8, ruleP2: 7},
			priorityOutput{ruleP1: 11, ruleP2: 5},
			priorityOutput{ruleP1: 12, ruleP2: 1},
			priorityOutput{ruleP1: 13, ruleP2: 5},
		}},
	}
	for _, p := range tcase {
		exp := sortRulePriorities(p.po)
		for i, n := range exp {
			if p.expected[i].ruleP1 != n.ruleP1 || p.expected[i].ruleP2 != n.ruleP2 {
				t.Errorf("[%d] expected (%d) (%d), got (%d) (%d)\n", p.id, p.expected[i].ruleP1, p.expected[i].ruleP2, n.ruleP1, n.ruleP2)
			}
		}
	}
}

func TestSetPriority(t *testing.T) {
	tcase := []struct {
		id       int
		input    *matchResult
		expected int
	}{
		{1, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 1},
		{2, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main"}, 2},
		{3, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "123"}, 3},
		{4, &matchResult{BKCol: 1, CLCol: 1}, 4},
		{5, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 5},
		{6, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main"}, 6},
		{7, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "123"}, 7},
		{8, &matchResult{BKCol: 1, CLCol: 2}, 8},
		{9, &matchResult{BKCol: -1, CLCol: -1, BKRule: 9000, CLRule: 9000, BKUnitNum: "101", CLUnitNum: "101"}, 9},
		{10, &matchResult{BKCol: -1, CLCol: -1, BKRule: 9000, CLRule: 9000}, 10},
		{11, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 11},
		{12, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main"}, 12},
		{13, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "123"}, 13},
		{14, &matchResult{BKCol: 2, CLCol: 2}, 14},
		{15, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "102"}, 15},
		{16, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "mains", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{17, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "123", CLHseNum: "1234", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{18, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "", CLHseNum: "", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{19, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{20, &matchResult{BKCol: 1, CLCol: 1, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "", CLUnitNum: ""}, 4},
		{21, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "102"}, 15},
		{22, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "mains", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{23, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "123", CLHseNum: "1234", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{24, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{25, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{26, &matchResult{BKCol: 1, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "", CLUnitNum: ""}, 8},
		{27, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "102"}, 15},
		{28, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "123", BKStrName: "main", CLStrName: "mains", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{29, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "123", CLHseNum: "1234", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{30, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "main", CLStrName: "main", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{31, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "101", CLUnitNum: "101"}, 15},
		{32, &matchResult{BKCol: 2, CLCol: 2, BKHseNum: "", CLHseNum: "", BKStrName: "", CLStrName: "", BKUnitNum: "", CLUnitNum: ""}, 14},
	}
	for _, p := range tcase {
		out := setPriority(p.input)
		if p.expected != out {
			t.Errorf("[%d] expected \"%v\", got \"%v\"\n", p.id, p.expected, out)
		}
	}
}

func TestSetPrioritySecondary(t *testing.T) {
	tcase := []struct {
		id       int
		input    *matchResult
		expected int
	}{
		{1, &matchResult{BKHseNum: "22", CLHseNum: "22"}, 1},
		{2, &matchResult{BKHseNum: "22", CLHseNum: "23"}, 3},
		{3, &matchResult{BKHseNum: "22", CLHseNum: ""}, 3},
		{4, &matchResult{BKHseNum: "", CLHseNum: "22"}, 3},
		{5, &matchResult{BKHseNum: "", CLHseNum: ""}, 3},
		{6, &matchResult{BKHseNum: " ", CLHseNum: " "}, 3},
	}
	for _, p := range tcase {
		out := setPrioritySecondary(p.input)
		if p.expected != out {
			t.Errorf("[%d] expected \"%v\", got \"%v\"\n", p.id, p.expected, out)
		}
	}
}

func TestCheckMultiAddress(t *testing.T) {
	tcase := []struct {
		id       int
		input    []priorityOutput
		expected bool
	}{
		{1, []priorityOutput{
			priorityOutput{ruleP1: 4, matchRes: &matchResult{BKRule: 9000}},
			priorityOutput{ruleP1: 3, matchRes: &matchResult{BKRule: 9000}},
			priorityOutput{ruleP1: 1, matchRes: &matchResult{BKRule: 9000}},
			priorityOutput{ruleP1: 2, matchRes: &matchResult{}},
		}, true},
		{2, []priorityOutput{
			priorityOutput{ruleP1: 4, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 3, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 1, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 2, matchRes: &matchResult{BKRule: 9000}},
		}, false},
		{3, []priorityOutput{
			priorityOutput{ruleP1: 4, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 3, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 1, matchRes: &matchResult{}},
			priorityOutput{ruleP1: 2, matchRes: &matchResult{}},
		}, false},
	}
	for _, p := range tcase {
		out := checkMultiAddress(p.input)
		if p.expected != out {
			t.Errorf("[%d] expected \"%v\", got \"%v\"\n", p.id, p.expected, out)
		}
	}
}

func TestUnequal(t *testing.T) {
	var tCases = []struct {
		id       int
		inputA   string
		inputB   string
		expected bool
	}{
		{1, "14", "14", false},
		{2, "14", "15", true},
		{3, " ", "", true},
		{4, "", " ", true},
		{5, " ", " ", true},
		{6, "", "", true},
		{7, "14", "", true},
	}
	for _, tc := range tCases {
		out := unequal(tc.inputA, tc.inputB)
		if tc.expected != out {
			t.Errorf("[%d] input \"%v\" \"%v\", expecting \"%v\", got \"%v\"\n", tc.id, tc.inputA, tc.inputB, tc.expected, out)
		}
	}
}

func TestUnequalZipCodes(t *testing.T) {
	var tCases = []struct {
		id       int
		ZipA     string
		ZipB     string
		expected bool
	}{
		{1, "06018", "6018", false},
		{2, "  06018  ", "  6018  ", false},
		{3, "  06017  ", "  6018  ", true},
		{4, " ", " ", true},
		{5, "", "", true},
		{6, " ", "", true},
		{7, "", " ", true},
	}
	for _, tc := range tCases {
		out := unequalZipCodes(tc.ZipA, tc.ZipB)
		if tc.expected != out {
			t.Errorf("input \"%v\" \"%v\", expecting \"%v\", got \"%v\"\n", tc.ZipA, tc.ZipB, tc.expected, out)
		}
	}
}

func TestPrimePrimeLow(t *testing.T) {
	trunc := make(map[trimKey]trim)
	trunc[trimKey{
		FIPS: trimS("92882"),
		src:  trimS("BK"),
	}] = trim{
		src:    "BK",
		col:    1,
		lt:     0,
		rt:     0,
		MaxLen: 0,
	}
	nes := make(map[string]struct{})
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}, true},
		{2, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 1}, false},
		{3, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 1}, false},
		{4, "92880", &matchResult{BKCol: 2, CLCol: 1, BKRule: 5, CLRule: 1}, false},
		{5, "92882", &matchResult{BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1}, false},                                 // Truncated
		{6, "90608", &matchResult{BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1, BKZip: "12345", CLZip: "12345"}, true},  //isNES
		{7, "90608", &matchResult{BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 1, BKZip: "12345", CLZip: "54321"}, false}, //isNotNES
	}
	for _, tc := range tCases {
		out := primeprimeLow(tc.mr, trunc, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}

func TestPrimePrimeHigh(t *testing.T) {
	nes := make(map[string]struct{})
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: "abc"}, true},
		{2, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
		{3, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: ""}, false},
		{4, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "", CLStrName: ""}, false},
		{5, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 5, CLRule: 5, BKHseNum: "", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
	}
	for _, tc := range tCases {
		out := primeprimeHigh(tc.mr, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}

func TestPrimeAltLow(t *testing.T) {
	trunc := make(map[trimKey]trim)
	trunc[trimKey{
		FIPS: trimS("92882"),
		src:  trimS("BK"),
	}] = trim{
		src:    "BK",
		col:    1,
		lt:     0,
		rt:     0,
		MaxLen: 0,
	}
	nes := make(map[string]struct{})
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKCol: 1, CLCol: 1, BKRule: 1, CLRule: 0}, true},
		{2, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0}, true},
		{3, "92880", &matchResult{BKCol: 2, CLCol: 1, BKRule: 1, CLRule: 0}, true},
		{4, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 0}, false},
		{5, "92880", &matchResult{BKCol: 2, CLCol: 1, BKRule: 5, CLRule: 0}, false},
		{6, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 1, CLRule: 0}, false},
		{7, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 0}, false},
		{8, "92882", &matchResult{BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0}, false},
		{9, "90608", &matchResult{BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0, BKZip: "12345", CLZip: "12345"}, true},
		{10, "90608", &matchResult{BKCol: 1, CLCol: 2, BKRule: 1, CLRule: 0, BKZip: "12345", CLZip: "54321"}, false},
	}
	for _, tc := range tCases {
		out := primealtLow(tc.mr, trunc, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}

func TestPrimeAltHigh(t *testing.T) {
	nes := make(map[string]struct{})
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: "abc"}, true},
		{2, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
		{3, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: ""}, false},
		{4, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "", CLStrName: ""}, false},
		{5, "92880", &matchResult{BKCol: 1, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
	}
	for _, tc := range tCases {
		out := primealtHigh(tc.mr, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}

func TestAddrAddr(t *testing.T) {
	nes := make(map[string]struct{})
	pro := []priorityOutput{}
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKRule: 9000, CLRule: 9000}, true},
		{2, "92880", &matchResult{BKRule: 9000, CLRule: 1}, false},
		{3, "92880", &matchResult{BKRule: 1, CLRule: 9000}, false},
		{4, "92880", &matchResult{BKRule: 1, CLRule: 1}, false},
		{5, "90608", &matchResult{BKRule: 9000, CLRule: 9000, BKZip: "12345", CLZip: "12345"}, true},
		{6, "90608", &matchResult{BKRule: 9000, CLRule: 9000, BKZip: "12345", CLZip: "54321"}, false},
	}
	for _, tc := range tCases {
		out := addraddr(pro, tc.mr, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}

func TestAltAlt(t *testing.T) {
	trunc := make(map[trimKey]trim)
	trunc[trimKey{
		FIPS: trimS("92882"),
		src:  trimS("BK"),
	}] = trim{
		src:    "BK",
		col:    1,
		lt:     0,
		rt:     0,
		MaxLen: 0,
	}
	nes := make(map[string]struct{})
	nes["90608"] = struct{}{}

	var tCases = []struct {
		id       int
		fips     string
		mr       *matchResult
		expected bool
	}{
		{1, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: "abc"}, true},
		{2, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
		{3, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "abc", CLStrName: ""}, false},
		{4, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "22", CLHseNum: "22", BKStrName: "", CLStrName: ""}, false},
		{5, "92880", &matchResult{BKCol: 2, CLCol: 2, BKRule: 5, CLRule: 5, BKHseNum: "", CLHseNum: "", BKStrName: "abc", CLStrName: "abc"}, false},
	}
	for _, tc := range tCases {
		out := altalt(tc.mr, trunc, nes, tc.fips)
		if tc.expected != out {
			t.Errorf("[%d] expecting \"%v\", got \"%v\"\n", tc.id, tc.expected, out)
		}
	}
}
