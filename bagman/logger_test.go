package bagman_test

import (
	"testing"
	"github.com/APTrust/bagman/bagman"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)


func setupLoggerTest() {
	requestedConfig := "test"
	config = bagman.LoadRequestedConfig(&requestedConfig)
	_ = os.Mkdir(config.LogDirectory, 0755)
}

// Teardown to run after tests. This deletes the directories
// that were created when tar files were unpacked.
func teardownLoggerTest() {
	os.RemoveAll(config.AbsLogDirectory())
}

func TestInitLogger(t *testing.T) {
	setupLoggerTest()
	defer teardownLoggerTest()
	log := bagman.InitLogger(config)
	log.Error("Test Message")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".log")
	if !bagman.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	data, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Error(err)
	}
	if false == strings.HasSuffix(string(data), "Test Message\n") {
		t.Error("Expected message was not in the message log.")
	}
}

func TestInitJsonLogger(t *testing.T) {
	setupLoggerTest()
	defer teardownLoggerTest()
	log := bagman.InitJsonLogger(config)
	log.Println("{a:100}")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".json")
	if !bagman.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	data, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Error(err)
	}
	if string(data) != "{a:100}\n" {
		t.Error("Expected message was not in the json log.")
	}
}

func TestDiscardLogger(t *testing.T) {
	log := bagman.DiscardLogger("logger_test")
	if log == nil {
		t.Error("DiscardLogger returned nil")
	}
	log.Info("This should not cause an error!")
}
