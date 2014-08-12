package bagman

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"github.com/mipearson/rfw"
	stdlog "log"
	"github.com/op/go-logging"
)

/*
InitLogger creates and returns a logger suitable for logging
human-readable message.
*/
func InitLogger(config Config) (*logging.Logger) {
	processName := path.Base(os.Args[0])
	filename := fmt.Sprintf("%s.log", processName)
	filename = filepath.Join(config.AbsLogDirectory(), filename)
	writer := getRotatingFileWriter(filename)

	log := logging.MustGetLogger(processName)
	format := logging.MustStringFormatter("[%{level}] [%{time}] %{message}")
	logging.SetFormatter(format)
    logging.SetLevel(config.LogLevel, processName)

	logBackend := logging.NewLogBackend(writer, "", 0)
	if config.LogToStderr {
		// Log to BOTH file and stderr
		stderrBackend := logging.NewLogBackend(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)
		stderrBackend.Color = true
		logging.SetBackend(logBackend, stderrBackend)
	} else {
		// Log to file only
		logging.SetBackend(logBackend)
	}

	return log
}

/*
InitLogger creates and returns a logger suitable for logging JSON
data. Bagman JSON logs consist of a single JSON object per line,
with no extraneous data. Because all of the data in the file is
pure JSON, with one record per line, these files are easy to parse.
*/
func InitJsonLogger(config Config) (*stdlog.Logger) {
	processName := path.Base(os.Args[0])
	filename := fmt.Sprintf("%s.json", processName)
	filename = filepath.Join(config.AbsLogDirectory(), filename)
	writer := getRotatingFileWriter(filename)
	return stdlog.New(writer, "", 0)
}

/*
getRotatingFileWriter returns a Writer suitable for writing to files
that may be deleted or renamed by outside processes, such as logrotate.
If the underlying file disappears, the rotating file writer will
recreate it and resume logging.
*/
func getRotatingFileWriter(filename string) (*rfw.Writer) {
	writer, err := rfw.Open(filename, 0644)
	if err != nil {
		msg := fmt.Sprintf("Cannot open log file at %s: %v\n", filename, err)
		panic(msg)
	}
	return writer
}
