package bagman_test

import (
	"testing"
	"time"
	"fmt"
	"strings"
	"launchpad.net/goamz/s3"
	"github.com/APTrust/bagman"
)

func TestOwnerOf(t *testing.T) {
	if bagman.OwnerOf("aptrust.receiving.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if bagman.OwnerOf("aptrust.restore.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified restoration bucket owner")
	}
}

func TestReceivingBucketFor(t *testing.T) {
	if bagman.ReceivingBucketFor("unc.edu") != "aptrust.receiving.unc.edu" {
		t.Error("ReceivingBucketFor returned incorrect receiving bucket name")
	}
}

func TestRestorationBucketFor(t *testing.T) {
	if bagman.RestorationBucketFor("unc.edu") != "aptrust.restore.unc.edu" {
		t.Error("RestorationBucketFor returned incorrect restoration bucket name")
	}
}

// Returns a basic ProcessResult that can be altered for
// specific tests.
func baseResult() (result *bagman.ProcessResult) {
	result = &bagman.ProcessResult{}
	result.S3File = &bagman.S3File{}
	result.S3File.BucketName = "aptrust.receiving.unc.edu"
	result.S3File.Key = s3.Key{}
	result.S3File.Key.Key = "sample.tar"
	result.S3File.Key.ETag = "\"0123456789ABCDEF\""
	result.S3File.Key.LastModified = "2014-05-28T16:22:24.016Z"
	return result
}

// Returns a result with Stage set to stage. If successful is false,
// the result will include an error message.
func getResult(stage string, successful bool) (result *bagman.ProcessResult) {
	result = baseResult()
	if successful == false {
		result.ErrorMessage = fmt.Sprintf("Sample error message. Sumpin went rawng!")
	}
	result.Stage = stage
	return result
}

// Returns a result that shows processing failed in unpack stage.
func resultFailedUnpack() (result *bagman.ProcessResult) {
	return result
}

// Returns a result that shows processing failed in store stage.
func resultFailedStore() (result *bagman.ProcessResult) {
	return result
}

// Returns a result that shows processing failed in record-to-fedora stage.
func resultFailedRecord() (result *bagman.ProcessResult) {
	return result
}

// Make sure ProcessStatus.IngestStatus() returns the correct
// ProcessStatus data.
func TestIngestStatus(t *testing.T) {
	passedFetch := getResult("Fetch", true)
	assertCorrectSummary(t, passedFetch, "Processing")
	failedFetch := getResult("Fetch", false)
	assertCorrectSummary(t, failedFetch, "Failed")

	passedUnpack := getResult("Unpack", true)
	assertCorrectSummary(t, passedUnpack, "Processing")
	failedUnpack := getResult("Unpack", false)
	assertCorrectSummary(t, failedUnpack, "Failed")

	passedStore := getResult("Store", true)
	assertCorrectSummary(t, passedStore, "Processing")
	failedStore := getResult("Store", false)
	assertCorrectSummary(t, failedStore, "Failed")

	passedRecord := getResult("Record", true)
	// TODO: Change Processing to Succeeded when Record step is working.
	assertCorrectSummary(t, passedRecord, "Processing")
	failedRecord := getResult("Record", false)
	assertCorrectSummary(t, failedRecord, "Failed")
}

func assertCorrectSummary(t *testing.T, result *bagman.ProcessResult, expectedStatus string) {
	status := result.IngestStatus()
	emptyTime := time.Time{}
	expectedBagDate := "2014-05-28 16:22:24.016 +0000 UTC"
	if status.Date == emptyTime {
		t.Error("ProcessStatus.Date was not set")
	}
	if status.Action != "Ingest" {
		t.Error("ProcessStatus.Type is incorrect. Should be Ingest.")
	}
	if status.Name != result.S3File.Key.Key {
		t.Errorf("ProcessStatus.Name: Expected %s, got %s",
			result.S3File.Key.Key,
			status.Name)
	}
	if status.Bucket != result.S3File.BucketName {
		t.Errorf("ProcessStatus.Bucket: Expected %s, got %s",
			result.S3File.BucketName,
			status.Bucket)
	}
	if status.BagDate.String() != expectedBagDate {
		t.Errorf("ProcessStatus.BagDate: Expected %s, got %s",
			expectedBagDate,
			status.BagDate)
	}
	if status.ETag != strings.Replace(result.S3File.Key.ETag, "\"", "", 2) {
		t.Errorf("ProcessStatus.ETag: Expected %s, got %s",
			result.S3File.Key.ETag,
			status.ETag)
	}
	if status.Stage != result.Stage {
		t.Errorf("ProcessStatus.Stage: Expected %s, got %s",
			result.Stage,
			status.Stage)
	}
	if status.Institution != bagman.OwnerOf(result.S3File.BucketName) {
		t.Errorf("ProcessStatus.Institution: Expected %s, got %s",
			bagman.OwnerOf(result.S3File.BucketName),
			status.Institution)
	}
	if result.ErrorMessage == "" && status.Note != "No problems" {
		t.Error("ProcessStatus.Note should be '%s', but it's '%s'.",
			"No problems", status.Note)
	}
	if result.ErrorMessage != "" && status.Note == "" {
		t.Error("ProcessStatus.Note should have a value, but it's empty.")
	}
	if result.ErrorMessage != "" && status.Note != result.ErrorMessage {
		t.Errorf("ProcessStatus.Note: Expected %s, got %s",
			result.ErrorMessage,
			status.Note)
	}
	if status.Status != expectedStatus {
		t.Errorf("ProcessStatus.Status: Expected %s, got %s",
			expectedStatus,
			status.Status)
		t.Errorf("This failure may be due to a temporary demo setting that considers Validation the final step.")
	}
}
