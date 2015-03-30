package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/crowdmob/goamz/aws"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

var fluctusAPIVersion string = "v1"
var skipMessagePrinted bool = false
var testBucket string = "aptrust.test"
var objId string = "test.edu/virginia.edu.uva-lib_2278801"

func canRunTests() bool {
	if skipMessagePrinted {
		return false
	}
	if awsEnvAvailable() == false {
		fmt.Println("Skipping fetcher tests because environment variables "+
			"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set.")
		skipMessagePrinted = true
		return false
	}
	_, err := http.Get(fluctusUrl)
	if err != nil {
		if skipMessagePrinted == false {
			skipMessagePrinted = true
			fmt.Printf("Skipping integration tests: "+
				"Fluctus server is not running at %s\n", fluctusUrl)
		}
		return false
	}
	return true
}

func getFluctusClient(t *testing.T) *bagman.FluctusClient {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	logger := bagman.DiscardLogger("client_test")
	fluctusClient, err := bagman.NewFluctusClient(
		fluctusUrl,
		fluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		logger)
	if err != nil {
		t.Errorf("Error constructing fluctus client: %v", err)
	}
	return fluctusClient
}

func getS3Client(t *testing.T) *bagman.S3Client {
	client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	return client
}


func TestFetchObjectFiles(t *testing.T) {
	if canRunTests() == false {
		return
	}
	fluctusClient := getFluctusClient(t)
	s3Client := getS3Client(t)
	if fluctusClient == nil || s3Client == nil {
		t.Error("Need both FluctusClient and S3Client to run tests")
		return
	}
	obj, err := fluctusClient.IntellectualObjectGet(objId, true)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
	}
	if obj == nil {
		t.Error("IntellectualObjectGet did not return the expected object.\n" +
			"You can load this object into your local Fluctus instance by running " +
			"`bundle exec rake fluctus:reset_data` in the Rails directory, and then " +
			"running `./scripts/process_items.sh` from the top-level bagman directory.")
	}
	tmpDir := os.TempDir()
	defer os.RemoveAll(tmpDir)
	dpnFetchResults, err := dpn.FetchObjectFiles(s3Client, obj, tmpDir)
	if err != nil {
		t.Errorf("FetchObjectFiles returned error %v", err)
		return
	}
	if len(dpnFetchResults) != 4 {
		t.Errorf("Expected 4 fetch results, got %d", len(dpnFetchResults))
		return
	}
	for _, result := range dpnFetchResults {
		if result.FetchResult.Md5Verified == false {
			t.Errorf("Md5 verfication failed for file %s", result.FetchResult.LocalFile)
		}
		if result.FetchResult.ErrorMessage != "" {
			t.Errorf(result.FetchResult.ErrorMessage)
		}
		origPath, _ := result.GenericFile.OriginalPath()
		expectedLocalPath := filepath.Join(tmpDir, origPath)
		if result.FetchResult.LocalFile != expectedLocalPath {
			t.Errorf("File saved to '%s' should have been saved in '%s'",
				result.FetchResult.LocalFile, expectedLocalPath)
		}
	}
}
