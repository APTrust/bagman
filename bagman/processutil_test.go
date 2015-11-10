package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"github.com/crowdmob/goamz/s3"
	"path"
	"path/filepath"
	"os"
	"testing"
)

var testConfig string = "test"

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
	procUtil := bagman.NewProcessUtil(&testConfig)
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
	procUtil := bagman.NewProcessUtil(&testConfig)
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

func TestMessageIdString(t *testing.T) {
	procUtil := bagman.NewProcessUtil(&testConfig)
	defer deleteTestLogs(procUtil.Config)

	messageId := nsq.MessageID{'s', 'i', 'x', 't', 'e', 'e', 'n', 's', 'i', 'x', 't', 'e', 'e', 'n', '1', '6'}
	if procUtil.MessageIdString(messageId) != "sixteensixteen16" {
		t.Errorf("MessageIdString should have returned 'sixteensixteen16', but returned '%s'",
			procUtil.MessageIdString(messageId))
	}
}

func TestSyncMapFunctions(t *testing.T) {
	procUtil := bagman.NewProcessUtil(&testConfig)
	defer deleteTestLogs(procUtil.Config)

	messageId1 := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	messageId2 := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '9', '8', '7', '1', 'x', 'y'}
	messageId1String := procUtil.MessageIdString(messageId1)
	messageId2String := procUtil.MessageIdString(messageId2)

	err := procUtil.RegisterItem("Item1", messageId1)
	if err != nil {
		t.Errorf("RegisterItem returned an unexpected error: %v", err)
	}

	// Trying to register the same key with a different messageId should cause an error.
	err = procUtil.RegisterItem("Item1", messageId2)
	if err == nil {
		t.Errorf("RegisterItem should have returned an error but did not")
	}

	// Register new key with new messageId
	err = procUtil.RegisterItem("Item2", messageId2)
	if err != nil {
		t.Errorf("RegisterItem returned an unexpected error: %v", err)
	}

	// Make sure it's all there.
	if procUtil.MessageIdFor("Item1") != messageId1String {
		t.Errorf("Expected messageId '%s' for Item1, but got '%s'",
			messageId1String, procUtil.MessageIdFor("Item1"))
	}
	if procUtil.MessageIdFor("Item2") != messageId2String {
		t.Errorf("Expected messageId '%s' for Item2, but got '%s'",
			messageId2String, procUtil.MessageIdFor("Item2"))
	}

	// Make sure Unregister works
	procUtil.UnregisterItem("Item1")
	if procUtil.MessageIdFor("Item1") != "" {
		t.Errorf("Item1 was not unregistered")
	}

	procUtil.UnregisterItem("Item2")
	if procUtil.MessageIdFor("Item2") != "" {
		t.Errorf("Item2 was not unregistered")
	}
}

func TestBagAlreadyInProgress(t *testing.T) {
	procUtil := bagman.NewProcessUtil(&testConfig)
	defer deleteTestLogs(procUtil.Config)

	s3File := &bagman.S3File {
		BucketName: "aptrust.receiving.miami.edu",
		Key: s3.Key {
			Key: "big_ol_file.tar",
		},
	}
	messageId := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	messageIdString := procUtil.MessageIdString(messageId)

	if procUtil.BagAlreadyInProgress(s3File, messageIdString) == true {
		t.Errorf("BagAlreadyInProgress() should have returned false")
	}

	_ = procUtil.RegisterItem(s3File.BagName(), messageId)
	if procUtil.BagAlreadyInProgress(s3File, messageIdString) == false {
		t.Errorf("BagAlreadyInProgress() should have returned true")
	}
	if procUtil.BagAlreadyInProgress(s3File, "SomeRandomString") == true {
		t.Errorf("BagAlreadyInProgress() should have returned false")
	}
	procUtil.UnregisterItem(s3File.BagName())

	tarFile := filepath.Join(procUtil.Config.TarDirectory, s3File.Key.Key)
	file, err := os.Create(tarFile)
	if err != nil {
		t.Errorf("Could not create file necessary for testing: %v", err)
	}

	if procUtil.BagAlreadyInProgress(s3File, messageIdString) == false {
		t.Errorf("BagAlreadyInProgress() should have returned true")
	}

	file.Close()
	os.Remove(tarFile)
}
