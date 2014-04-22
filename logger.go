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

var filename string
var logLevel LogLevel
var logger *log.Logger

func InitLogger(dirname string) (path string) {
	const timeFormat = "20060102.150405"
	filename := fmt.Sprintf("bagman_%s.log", time.Now().Format(timeFormat))
	filename = filepath.Join(dirname, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		msg := fmt.Sprintf("Cannot open log file at %s: %v\n", filename, err)
		panic(msg)
	}
	logLevel = Info
	logger = log.New(file, "", 0) //log.Ldate|log.Ltime)
	return filename
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
