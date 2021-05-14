package dynamo

import (
	"fmt"
	"log"
)

// Debugging
const Debug = InfoLevel

// Enumerated lookup for log level type
const (
	None = iota
	ShiVizLevel
	InfoLevel
	NoticeLevel
	DebugLevel
	WarningLevel
	ErrorLevel
)

// Configure the color suffix and prefix for each log level
const (
	ShiVizColor  = "ShiViz:\t%s"
	InfoColor    = "\033[1;34mInfo:\t%s\033[0m"
	NoticeColor  = "\033[1;36mNotice:\t%s\033[0m"
	DebugColor   = "\033[0;36mDebug:\t%s\033[0m"
	WarningColor = "\033[1;33mWarning:\t%s\033[0m"
	ErrorColor   = "\033[1;31mError:\t%s\033[0m"
)

func DPrintfNew(logLevel int, format string, a ...interface{}) (n int, err error) {
	if (logLevel >= Debug) && (Debug > None) {
		if logLevel == ShiVizLevel {
			output := fmt.Sprintf(ShiVizColor, format)
			log.Printf(output, a...)
		}
		if logLevel == InfoLevel {
			output := fmt.Sprintf(InfoColor, format)
			log.Printf(output, a...)
		}
		if logLevel == NoticeLevel {
			output := fmt.Sprintf(NoticeColor, format)
			log.Printf(output, a...)
		}
		if logLevel == DebugLevel {
			output := fmt.Sprintf(DebugColor, format)
			log.Printf(output, a...)
		}
		if logLevel == WarningLevel {
			output := fmt.Sprintf(WarningColor, format)
			log.Printf(output, a...)
		}
		if logLevel == ErrorLevel {
			output := fmt.Sprintf(ErrorColor, format)
			log.Printf(output, a...)
		}
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
