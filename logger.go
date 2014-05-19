package bagman

import (
	"time"
	"log"
	"fmt"
	"os"
	"path/filepath"
)

type LogLevel int

const (
	Fatal LogLevel = iota
	Error
	Warning
	Info
	Debug
)

// InitLoggers creates and returns a JSON logger, which can be used to save
// serialized JSON data, and a message logger for plain text messages, warnings,
// errors, etc.
//
// Param dirname is the name of the directory in which to create the log file.
// Param processName will be prefixed to the name of the log file.
func InitLoggers(dirname string, processName string) (jsonLog *log.Logger, messageLog *log.Logger) {
	jsonLog = makeLogger(dirname, processName, "json", false)
	messageLog = makeLogger(dirname, processName, "message", true)
	return jsonLog, messageLog
}

func makeLogger(dirname string, processName string, logType string, includeTimestamp bool) (logger *log.Logger) {
	const timeFormat = "20060102.150405"
	filename := fmt.Sprintf("%s_%s_%s.log", processName,
		logType, time.Now().Format(timeFormat))
	filename = filepath.Join(dirname, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		msg := fmt.Sprintf("Cannot open log file at %s: %v\n", filename, err)
		panic(msg)
	}
	if includeTimestamp {
		return log.New(file, "", log.Ldate|log.Ltime)
	} else {
		return log.New(file, "", 0)
	}
}
