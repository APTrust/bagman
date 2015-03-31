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
	"time"
)

var fluctusAPIVersion string = "v1"
var skipMessagePrinted bool = false
var testBucket string = "aptrust.test"
var objId string = "test.edu/virginia.edu.uva-lib_2278801"

var	md5Sum = "12345678"
var gfIdentifier = "test.edu/my_bag/file1.jpg"


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

func getGenericFile() (*bagman.GenericFile) {
	checksumAttributes := make([]*bagman.ChecksumAttribute, 1)
	checksumAttributes[0] = &bagman.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: md5Sum,
	}
	return &bagman.GenericFile{
		Identifier: gfIdentifier,
		ChecksumAttributes: checksumAttributes,
	}
}

func getFetchResult() (*bagman.FetchResult) {
	return &bagman.FetchResult{
		RemoteMd5: md5Sum,
		LocalMd5: md5Sum,
		Md5Verified: true,
		ErrorMessage: "",
	}
}

func getDPNFetchResult() (*dpn.DPNFetchResult) {
	return &dpn.DPNFetchResult{
		FetchResult: getFetchResult(),
		GenericFile: getGenericFile(),
	}
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
	dpnFetchResults, err := dpn.FetchObjectFiles(s3Client, obj.GenericFiles, tmpDir)
	if err != nil {
		t.Errorf("FetchObjectFiles returned error %v", err)
		return
	}
	if len(dpnFetchResults.Items) != 4 {
		t.Errorf("Expected 4 fetch results, got %d", len(dpnFetchResults.Items))
		return
	}
	for _, result := range dpnFetchResults.Items {
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

func TestFetchResultSucceeded(t *testing.T) {
	genericFile := getGenericFile()
	fetchResult := getFetchResult()
	resultFailed1 := &dpn.DPNFetchResult{
		FetchResult: nil,
		GenericFile: nil,
	}
	if resultFailed1.Succeeded() == true {
		t.Errorf("resultFailed1 should have failed")
	}
	resultFailed2 := &dpn.DPNFetchResult{
		FetchResult: &bagman.FetchResult{},
		GenericFile: &bagman.GenericFile{},
	}
	if resultFailed2.Succeeded() == true {
		t.Errorf("resultFailed2 should have failed")
	}
	resultFailed3 := &dpn.DPNFetchResult{
		FetchResult: fetchResult,
		GenericFile: &bagman.GenericFile{},
	}
	if resultFailed3.Succeeded() == true {
		t.Errorf("resultFailed3 should have failed")
	}
	resultFailed4 := &dpn.DPNFetchResult{
		FetchResult: &bagman.FetchResult{},
		GenericFile: genericFile,
	}
	if resultFailed4.Succeeded() == true {
		t.Errorf("resultFailed4 should have failed")
	}

	// Here's our one good case
	resultSucceeded1 := &dpn.DPNFetchResult{
		FetchResult: fetchResult,
		GenericFile: genericFile,
	}
	if resultSucceeded1.Succeeded() == false {
		t.Errorf("resultSucceeded1 should have succeeded")
	}

	// Try with mismatched checksums
	fetchResult.LocalMd5 = "--BadChecksum--"
	resultFailed5 := &dpn.DPNFetchResult{
		FetchResult: fetchResult,
		GenericFile: genericFile,
	}
	if resultFailed5.Succeeded() == true {
		t.Errorf("resultFailed5 should have failed")
	}

	// Try with error message
	fetchResult.LocalMd5 = md5Sum
	fetchResult.ErrorMessage = "Oops, I broke the interwebs."
	resultFailed6 := &dpn.DPNFetchResult{
		FetchResult: fetchResult,
		GenericFile: genericFile,
	}
	if resultFailed6.Succeeded() == true {
		t.Errorf("resultFailed6 should have failed")
	}

}

func TestNewFetchResultCollection(t *testing.T) {
	collection := dpn.NewFetchResultCollection()
	if collection.Items == nil || len(collection.Items) != 0 {
		t.Errorf("NewFetchResultCollection() items missing or invalid")
	}
}

func TestFetchResultCollectionAdd(t *testing.T) {
	collection := dpn.NewFetchResultCollection()
	collection.Add(getDPNFetchResult())
	if len(collection.Items) != 1 {
		t.Errorf("There should be one item in the collection")
	}
	anotherResult := getDPNFetchResult()
	anotherResult.GenericFile.Identifier = "test.edu/my_bag/file2.pdf"
	collection.Add(anotherResult)
	if len(collection.Items) != 2 {
		t.Errorf("There should be two items in the collection")
	}
}

func TestFindByIdentifier(t *testing.T) {
	collection := dpn.NewFetchResultCollection()
	collection.Add(getDPNFetchResult())
	anotherResult := getDPNFetchResult()
	anotherResult.GenericFile.Identifier = "test.edu/my_bag/file2.pdf"
	collection.Add(anotherResult)

	result1 := collection.FindByIdentifier(gfIdentifier)
	if result1 == nil {
		t.Errorf("First identifier was not found")
	}
	result2 := collection.FindByIdentifier(anotherResult.GenericFile.Identifier)
	if result2 == nil {
		t.Errorf("Second identifier was not found")
	}
	result3 := collection.FindByIdentifier("I yam what I yam and that's all what I yam.")
	if result3 != nil {
		t.Errorf("FindByIdenfier is trippin'")
	}
}
