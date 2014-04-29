package bagman_test

import (
	"testing"
	"fmt"
	"os"
	"path/filepath"
	"github.com/APTrust/bagman"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

var skipMessagePrinted bool = false
var testBucket string = "aptrust.test"

// Returns true if the AWS environment variables
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
// are set, false if not.
func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}

// This prints a message saying S3 integration tests
// will be skipped.
func printSkipMessage() {
	if !skipMessagePrinted {
		fmt.Fprintln(os.Stderr,
			"Skipping S3 integration tests because environment variables " +
				"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set.")
		skipMessagePrinted = true
	}
}

// Test that we can get an S3 client.
func TestGetClient(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage()
		return
	}
	_, err := bagman.GetClient(aws.APNortheast)
	if err != nil {
		t.Error("Cannot create S3 client: %v\n", err)
	}
}

// Test that we can list the contents of an S3 bucket.
// TODO: Test listing a bucket with >1000 items.
func TestListBucket(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage()
		return
	}
	s3Client, err := bagman.GetClient(aws.USEast)
	if err != nil {
		t.Error("Cannot create S3 client: %v\n", err)
	}
	bucket := s3Client.Bucket(testBucket)
	keys, err := bagman.ListBucket(bucket, 20)
	if err != nil {
		t.Error("Cannot get list of S3 bucket contents: %v\n", err)
	}
	if len(keys) < 1 {
		t.Error("ListBucket returned empty list")
	}
}

// Test that we can save an S3 file to the local filesystem,
// and that the data in the FetchResult is what we expect.
// TODO: Test case where md5 sum does not match.
// TODO: Test case where md5 sum cannot be verified.
func TestFetchToFile(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage()
		return
	}
	s3Client, err := bagman.GetClient(aws.USEast)
	if err != nil {
		t.Error("Cannot create S3 client: %v\n", err)
	}
	bucket := s3Client.Bucket(testBucket)
	keys, err := bagman.ListBucket(bucket, 20)
	if len(keys) < 1 {
		t.Error("ListBucket returned empty list")
	}

	var keyToFetch s3.Key
	for _, key := range(keys) {
		if key.Key == "sample_good.tar" {
			keyToFetch = key
			break
		}
	}
	if &keyToFetch == nil {
		t.Error("Can't run s3 fetch test because aptrust.test/sample_good.tar is missing")
	}

	// Fetch the first file from the test bucket and store
	// it in the testdata directory. Note that testDataPath
	// is defined in bag_test.go, which is part of the
	// bagman_test package.
	outputDir := filepath.Join(testDataPath, "tmp")
	os.MkdirAll(outputDir, 0755)
	outputFile := filepath.Join(outputDir, keyToFetch.Key)
	outputFileAbs, _ := filepath.Abs(outputFile)
	result := bagman.FetchToFile(bucket, keyToFetch, outputFile)
	defer os.Remove(filepath.Join(outputDir, keyToFetch.Key))
	if result.Error != nil {
		t.Error("FetchToFile returned an error: %v", result.Error)
	}
	if result.BucketName != bucket.Name {
		t.Error("Expected bucket name %s, got %s", bucket.Name, result.BucketName)
	}
	if result.Key != keyToFetch.Key {
		t.Error("Expected key name %s, got %s", keyToFetch.Key, result.Key)
	}
	if result.LocalTarFile != outputFileAbs {
		t.Error("Expected local file name %s, got %s",
			outputFileAbs, result.LocalTarFile)
	}
	if result.RemoteMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Error("Expected remote md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.RemoteMd5)
	}
	if result.LocalMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Error("Expected local md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.LocalMd5)
	}
	if result.Md5Verified == false {
		t.Error("md5 sum should have been verified but was not")
	}
	if result.Md5Verifiable == false {
		t.Error("md5 sum incorrectly marked as not verifiable")
	}
	if result.Warning != "" {
		t.Error("Fetch result returned warning: %s", result.Warning)
	}
}

func TestFetchNonExistentFile(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage()
		return
	}
	s3Client, err := bagman.GetClient(aws.USEast)
	if err != nil {
		t.Error("Cannot create S3 client: %v\n", err)
	}
	bucket := s3Client.Bucket(testBucket)
	keys, err := bagman.ListBucket(bucket, 20)
	if len(keys) < 1 {
		t.Error("ListBucket returned empty list")
	}
	// trickery!
	keys[0].Key = "non_existent_file.tar"
	outputDir := filepath.Join(testDataPath, "tmp")
	os.MkdirAll(outputDir, 0755)
	outputFile := filepath.Join(outputDir, keys[0].Key)
	result := bagman.FetchToFile(bucket, keys[0], outputFile)

	// Make sure we have the bucket name and file name, because we
	// want to know what we failed to fetch.
	if result.BucketName != bucket.Name {
		t.Error("Expected bucket name %s, got %s", bucket.Name, result.BucketName)
	}
	if result.Key != keys[0].Key {
		t.Error("Expected key name %s, got %s", keys[0].Key, result.Key)
	}
	if result.Error == nil {
		t.Error("FetchToFile should have returned a 'not found' error, but did not.")
	}
	if result.Error.Error() != "The specified key does not exist." {
		t.Error("Got unexpected error message: %v", result.Error)
	}
}
