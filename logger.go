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

func InitLoggers(dirname string) (jsonLog *log.Logger, messageLog *log.Logger) {
	jsonLog = makeLogger(dirname, "json", false)
	messageLog = makeLogger(dirname, "message", true)
	return jsonLog, messageLog
}

func makeLogger(dirname string, logType string, includeTimestamp bool) (logger *log.Logger) {
	const timeFormat = "20060102.150405"
	filename := fmt.Sprintf("bagman_%s_%s.log", logType, time.Now().Format(timeFormat))
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
