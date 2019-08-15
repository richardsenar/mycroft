package sherlock

import (
	"fmt"
	"log"
	"os"
)

/*
oooo
`888
 888   .ooooo.   .oooooooo  .oooooooo  .ooooo.  oooo d8b
 888  d88' `88b 888' `88b  888' `88b  d88' `88b `888""8P
 888  888   888 888   888  888   888  888ooo888  888
 888  888   888 `88bod8P'  `88bod8P'  888    .o  888
o888o `Y8bod8P' `8oooooo.  `8oooooo.  `Y8bod8P' d888b
                d"     YD  d"     YD
                "Y88888P'  "Y88888P'
*/

// NewLogger creates a custom log file
func NewLogger(name string) (*os.File, *log.Logger) {
	errorlog, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}
	// logger := log.New(errorlog, fmt.Sprintf("%s: ", name), log.Lshortfile|log.LstdFlags)
	logger := log.New(errorlog, fmt.Sprintf("%s: ", name), log.LstdFlags)
	return errorlog, logger
}
