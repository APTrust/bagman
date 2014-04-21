package bagman

import (
	"time"
	"log"
	"fmt"
	"os"
)

type LogLevel int

const (
	Fatal LogLevel = iota
	Error
	Warning
	Info
	Debug
)

var filename string
var logLevel LogLevel
var logger *log.Logger

func init() {
	const timeFormat = "20060102.150405"
	filename := fmt.Sprintf("bagman_%s.log", time.Now().Format(timeFormat))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Cannot open log file")
	}
	logLevel = Info
	logger = log.New(file, "", log.Ldate|log.Ltime)
}

func SetLogLevel(level LogLevel) {
	logLevel = level
}

func GetLogLevel() (level LogLevel) {
	return logLevel
}

func LogDebug(a ...interface{}) {
	if logLevel >= Debug {
		logger.SetPrefix("[DEBUG] ")
		logger.Println(a ...)
		logger.SetPrefix("")
	}
}

func LogInfo(a ...interface{}) {
	if logLevel >= Info {
		logger.SetPrefix("[INFO] ")
		logger.Println(a ...)
		logger.SetPrefix("")
	}
}

func LogWarning(a ...interface{}) {
	if logLevel >= Warning {
		logger.SetPrefix("[WARNING] ")
		logger.Println(a ...)
		logger.SetPrefix("")
	}
}

func LogError(a ...interface{}) {
	if logLevel >= Error {
		logger.SetPrefix("[ERROR] ")
		logger.Println(a ...)
		logger.SetPrefix("")
	}
}

func LogFatal(a ...interface{}) {
	if logLevel >= Fatal {
		logger.SetPrefix("[Fatal] ")
		logger.Fatalln(a ...)
		logger.SetPrefix("")
	}
}

func LogPanic(a ...interface{}) {
	logger.Panicln(a ...)
}
