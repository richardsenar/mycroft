package sherlock

import "sync"

/*
 o8o                    .o8
 `"'                   "888
oooo  ooo. .oo.    .oooo888   .ooooo.  oooo    ooo
`888  `888P"Y88b  d88' `888  d88' `88b  `88b..8P'
 888   888   888  888   888  888ooo888    Y888'
 888   888   888  888   888  888    .o  .o8"'88b
o888o o888o o888o `Y8bod88P" `Y8bod8P' o88'   888o
*/

type gcvKey struct {
	GCV      string
	HseNum   string
	StrtName string
	City     string
	State    string
	UnitNum  string
}

type gcvVal struct {
	Value   string
	SAPID   string
	Col     int
	Rule    int
	HseNum  string
	StrName string
	UnitNum string
	Zip     string
}

type indexerGCV struct {
	idxr map[gcvKey][]gcvVal
	sync.RWMutex
}

func newIndexerGCV() *indexerGCV {
	return &indexerGCV{
		idxr: make(map[gcvKey][]gcvVal),
	}
}

// define method to get values based on key
func (i *indexerGCV) Get(key gcvKey) ([]gcvVal, bool) {
	i.RLock()
	result, ok := i.idxr[key]
	i.RUnlock()
	return result, ok
}

// define method to set values on key
func (i *indexerGCV) Set(key gcvKey, val gcvVal) {
	i.Lock()
	i.idxr[key] = append(i.idxr[key], val)
	i.Unlock()
}
