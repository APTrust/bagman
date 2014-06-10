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
var testPreservationBucket string = "aptrust.test.preservation"

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
    if result.ErrorMessage != "" {
        t.Error("FetchToFile returned an error: %s", result.ErrorMessage)
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
    // Retry should be true, unless file does not exist.
    if result.Retry == false {
        t.Error("Fetch result retry was false, but should be true.")
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
    if result.ErrorMessage == "" {
        t.Error("FetchToFile should have returned a 'not found' error, but did not.")
    }
    if result.ErrorMessage != "Error retrieving file from receiving bucket: The specified key does not exist." {
        t.Error("Got unexpected error message: %v", result.ErrorMessage)
    }
    // Retry should be false, because file does not exist and we don't
    // want to waste any more time on it.
    if result.Retry == true {
        t.Error("Fetch result retry was true, but should be false.")
    }
}

func TestSaveToS3(t *testing.T) {
    bagmanHome, err := bagman.BagmanHome()
    if err != nil {
        t.Error(err)
    }
    path := filepath.Join(bagmanHome, "testdata", "sample_good.tar")
    file, err := os.Open(path)
    if err != nil {
        t.Errorf("Error opening local test file: %v", err)
    }
    defer file.Close()
    fileInfo, err := file.Stat()
    if err != nil {
        t.Errorf("Can't stat local test file: %v", err)
    }
    err = bagman.SaveToS3(testPreservationBucket, "test_file.tar",
        "application/binary", file, fileInfo.Size())
    if err != nil {
        t.Error(err)
    }
}

func TestGetKey(t *testing.T) {
    key, err := bagman.GetKey(testPreservationBucket, "test_file.tar")
    if err != nil {
        t.Error(err)
    }
    expectedETag := "\"7d5c7c1727fd538888f3eb89658abfdf\""
    if key.ETag != expectedETag {
        t.Errorf("Expected ETag %s, got %s", expectedETag, key.ETag)
    }
    if key.Size != int64(23552) {
        t.Errorf("Expected Size %d, got %d", int64(23552), key.Size)
    }
}
