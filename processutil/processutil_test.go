package processutil_test

import (
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"path"
	"path/filepath"
	"os"
	"testing"
)

func deleteTestLogs(config bagman.Config) {
	processName := path.Base(os.Args[0])
	jsonLog := fmt.Sprintf("%s.json", processName)
	jsonLog = filepath.Join(config.AbsLogDirectory(), jsonLog)
	os.Remove(jsonLog)

	messageLog := fmt.Sprintf("%s.log", processName)
	messageLog = filepath.Join(config.AbsLogDirectory(), messageLog)
	os.Remove(messageLog)
}

func TestNewProcessUtil(t *testing.T) {
	procUtil := processutil.NewProcessUtil("test")
	defer deleteTestLogs(procUtil.Config)
	if procUtil.Config.ActiveConfig != "test" {
		t.Errorf("NewProcessUtil did not load the test config")
	}
	if procUtil.JsonLog == nil {
		t.Errorf("NewProcessUtil did not initialize JsonLog")
	}
	if procUtil.MessageLog == nil {
		t.Errorf("NewProcessUtil did not initialize MessageLog")
	}
	if procUtil.Volume == nil {
		t.Errorf("NewProcessUtil did not initialize Volume")
	}
	if procUtil.S3Client == nil {
		t.Errorf("NewProcessUtil did not initialize S3Client")
	}
	if procUtil.FluctusClient == nil {
		t.Errorf("NewProcessUtil did not initialize FluctusClient")
	}
	if procUtil.Succeeded() != 0 {
		t.Errorf("NewProcessUtil did not initialize succeeded to zero.")
	}
	if procUtil.Failed() != 0 {
		t.Errorf("NewProcessUtil did not initialize failed to zero.")
	}
}

func TestIncrementSucceededAndFailed(t *testing.T) {
	procUtil := processutil.NewProcessUtil("test")
	defer deleteTestLogs(procUtil.Config)
	initialValue := procUtil.Succeeded()
	for i:=0; i < 3; i++ {
		procUtil.IncrementSucceeded()
	}
	if procUtil.Succeeded() - initialValue != 3 {
		t.Errorf("Succeeded() returned %d, expected 3", procUtil.Succeeded() - initialValue)
	}
	initialValue = procUtil.Failed()
	for i:=0; i < 3; i++ {
		procUtil.IncrementFailed()
	}
	if procUtil.Failed() - initialValue != 3 {
		t.Errorf("Failed() returned %d, expected 3", procUtil.Failed() - initialValue)
	}
}
