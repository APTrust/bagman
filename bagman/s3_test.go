package bagman_test

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var s3SkipMessagePrinted bool = false
var testBucket string = "aptrust.test"
var testPreservationBucket string = "aptrust.test.preservation"


// This prints a message saying S3 integration tests
// will be skipped.
func printSkipMessage(testname string) {
	if !s3SkipMessagePrinted {
		msg := fmt.Sprintf("Skipping %s because environment variables "+
			"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set.", testname)
		fmt.Fprintln(os.Stderr, msg)
		s3SkipMessagePrinted = true
	}
}

// Test that we can get an S3 client.
func TestNewS3Client(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	_, err := bagman.NewS3Client(aws.APNortheast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
}

// Test that we can get an S3 client with explicit auth params.
func TestNewS3ClientExplicitAuth(t *testing.T) {
	client, err := bagman.NewS3ClientExplicitAuth(aws.APNortheast, "Ax-S-Kee", "SeekritKee")
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	if client.S3.Auth.AccessKey != "Ax-S-Kee" {
		t.Errorf("S3Client access key was not set correctly.")
	}
	if client.S3.Auth.SecretKey != "SeekritKee" {
		t.Errorf("S3Client secret key was not set correctly.")
	}
}


// Test that we can list the contents of an S3 bucket.
// TODO: Test listing a bucket with >1000 items.
func TestListBucket(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	keys, err := s3Client.ListBucket(testBucket, 20)
	if err != nil {
		t.Errorf("Cannot get list of S3 bucket contents: %v\n", err)
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
		printSkipMessage("s3_test.go")
		return
	}
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	keys, err := s3Client.ListBucket(testBucket, 20)
	if len(keys) < 1 {
		t.Error("ListBucket returned empty list")
	}

	var keyToFetch s3.Key
	for _, key := range keys {
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
	result := s3Client.FetchToFile(testBucket, keyToFetch, outputFile)
	defer os.Remove(filepath.Join(outputDir, keyToFetch.Key))
	if result.ErrorMessage != "" {
		t.Errorf("FetchToFile returned an error: %s", result.ErrorMessage)
	}
	if result.BucketName != testBucket {
		t.Errorf("Expected bucket name %s, got %s", testBucket, result.BucketName)
	}
	if result.Key != keyToFetch.Key {
		t.Errorf("Expected key name %s, got %s", keyToFetch.Key, result.Key)
	}
	if result.LocalFile != outputFileAbs {
		t.Errorf("Expected local file name %s, got %s",
			outputFileAbs, result.LocalFile)
	}
	if result.RemoteMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Errorf("Expected remote md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.RemoteMd5)
	}
	if result.LocalMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Errorf("Expected local md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.LocalMd5)
	}
	if result.Md5Verified == false {
		t.Error("md5 sum should have been verified but was not")
	}
	if result.Md5Verifiable == false {
		t.Error("md5 sum incorrectly marked as not verifiable")
	}
	if result.Warning != "" {
		t.Errorf("Fetch result returned warning: %s", result.Warning)
	}
	// Retry should be true, unless file does not exist.
	if result.Retry == false {
		t.Error("Fetch result retry was false, but should be true.")
	}
}

func TestFetchURLToFile(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	url := "https://s3.amazonaws.com/aptrust.test/sample_good.tar"


	// Fetch the first file from the test bucket and store
	// it in the testdata directory. Note that testDataPath
	// is defined in bag_test.go, which is part of the
	// bagman_test package.
	outputDir := filepath.Join(testDataPath, "tmp")
	outputFile := filepath.Join(outputDir, "sample_good.tar")
	outputFileAbs, _ := filepath.Abs(outputFile)
	result := s3Client.FetchURLToFile(url, outputFile)
	defer os.Remove(outputFileAbs)
	if result.ErrorMessage != "" {
		t.Errorf("FetchURLToFile returned an error: %s", result.ErrorMessage)
	}
	if result.BucketName != testBucket {
		t.Errorf("Expected bucket name %s, got %s", testBucket, result.BucketName)
	}
	if result.Key != "sample_good.tar" {
		t.Errorf("Expected key name 'sample_good.tar', got %s", result.Key)
	}
	if result.LocalFile != outputFileAbs {
		t.Errorf("Expected local file name %s, got %s",
			outputFileAbs, result.LocalFile)
	}
	if result.RemoteMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Errorf("Expected remote md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.RemoteMd5)
	}
	if result.LocalMd5 != "22ecc8c4146ad65bd0f9ddb0db32e8b9" {
		t.Errorf("Expected local md5 sum %s, got %s",
			"22ecc8c4146ad65bd0f9ddb0db32e8b9", result.LocalMd5)
	}
	if result.Md5Verified == false {
		t.Error("md5 sum should have been verified but was not")
	}
	if result.Md5Verifiable == false {
		t.Error("md5 sum incorrectly marked as not verifiable")
	}
	if result.Warning != "" {
		t.Errorf("Fetch result returned warning: %s", result.Warning)
	}
	// Retry should be true, unless file does not exist.
	if result.Retry == false {
		t.Error("Fetch result retry was false, but should be true.")
	}
}

func TestFetchNonExistentFile(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	keys, err := s3Client.ListBucket(testBucket, 20)
	if len(keys) < 1 {
		t.Error("ListBucket returned empty list")
	}
	// trickery!
	keys[0].Key = "non_existent_file.tar"
	outputDir := filepath.Join(testDataPath, "tmp")
	os.MkdirAll(outputDir, 0755)
	outputFile := filepath.Join(outputDir, keys[0].Key)
	result := s3Client.FetchToFile(testBucket, keys[0], outputFile)

	// Make sure we have the bucket name and file name, because we
	// want to know what we failed to fetch.
	if result.BucketName != testBucket {
		t.Errorf("Expected bucket name %s, got %s", testBucket, result.BucketName)
	}
	if result.Key != keys[0].Key {
		t.Errorf("Expected key name %s, got %s", keys[0].Key, result.Key)
	}
	if result.ErrorMessage == "" {
		t.Error("FetchToFile should have returned a 'not found' error, but did not.")
	}
	if result.ErrorMessage != "Error retrieving file aptrust.test/non_existent_file.tar: The specified key does not exist." {
		t.Errorf("Got unexpected error message: %v", result.ErrorMessage)
	}
	// Retry should be false, because file does not exist and we don't
	// want to waste any more time on it.
	if result.Retry == true {
		t.Error("Fetch result retry was true, but should be false.")
	}
}

func TestSaveToS3(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	// Copy this file from the testdata directory to the
	// test preservation bucket.
	err := SaveToS3("example.edu.sample_good.tar", testPreservationBucket)
	if err != nil {
		t.Error(err)
	}
}

func TestGetKey(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	key, err := s3Client.GetKey(testPreservationBucket, "example.edu.sample_good.tar")
	if err != nil {
		t.Error(err)
	}
	if key == nil {
		t.Error("s3Client.GetKey returned nil")
		return
	}
	expectedETag := "\"05e68e69767c772d36bd8a2baf693428\""
	if key.ETag != expectedETag {
		t.Errorf("Expected ETag %s, got %s", expectedETag, key.ETag)
	}
	if key.Size != int64(23552) {
		t.Errorf("Expected Size %d, got %d", int64(23552), key.Size)
	}
}

func TestDeleteFromS3(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}
	// Make sure we have a file there to delete.
	err := SaveToS3("example.edu.sample_good.tar", testPreservationBucket)
	if err != nil {
		t.Error(err)
	}

	// Now make sure the delete function works.
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}
	err = s3Client.Delete(testPreservationBucket, "test_file.tar")
	if err != nil {
		t.Error(err)
	}
}

// Copies localFile to bucketName on S3. localFile is assumed
// to be inside the testdata directory.
func SaveToS3(localFile, bucketName string) error {
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		return fmt.Errorf("Cannot create S3 client: %v\n", err)
	}
	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		return err
	}
	path := filepath.Join(bagmanHome, "testdata", localFile)
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("Error opening local test file: %v", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("Can't stat local test file: %v", err)
	}
	fileBytes := make([]byte, fileInfo.Size())
	_, _ = file.Read(fileBytes)
	_, _ = file.Seek(0, 0)
	md5Bytes := md5.Sum(fileBytes)
	base64md5 := base64.StdEncoding.EncodeToString(md5Bytes[:])
	options := s3Client.MakeOptions(base64md5, nil)
	url, err := s3Client.SaveToS3(bucketName, localFile,
		"application/binary", file, fileInfo.Size(), options)
	if err != nil {
		return err
	}
	expectedUrl := fmt.Sprintf("https://s3.amazonaws.com/%s/%s",
		bucketName, localFile)
	if url != expectedUrl {
		return fmt.Errorf("Expected url '%s' but got '%s'", expectedUrl, url)
	}
	return nil
}

func TestSaveLargeFileToS3(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Copy this local file to remote bucket.
	localFile := "example.edu.multi_mb_test_bag.tar"
	bucketName := testPreservationBucket

	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	path := filepath.Join(bagmanHome, "testdata", localFile)

	// Our multi-megabyte test file is not in the github repo
	// and we don't want to perform this test all the time anyway.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Printf("Skipping TestSaveLargeFileToS3 because test file "+
			"%s does not exist", path)
		return
	}

	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	// Delete the file if it's already there.
	_ = s3Client.Delete(bucketName, localFile)

	file, err := os.Open(path)
	if err != nil {
		t.Errorf("Error opening local test file: %v", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		t.Errorf("Can't stat local test file: %v", err)
	}
	fileBytes := make([]byte, fileInfo.Size())
	_, _ = file.Read(fileBytes)
	_, _ = file.Seek(0, 0)
	md5Bytes := md5.Sum(fileBytes)
	base64md5 := base64.StdEncoding.EncodeToString(md5Bytes[:])

	s3Metadata := make(map[string][]string)
	s3Metadata["md5"] = []string{"Test12345678"}
	s3Metadata["institution"] = []string{"aptrust.org"}
	s3Metadata["bag"] = []string{"test_bag"}
	s3Metadata["bagpath"] = []string{"data/test_file.pdf"}

	options := s3Client.MakeOptions(base64md5, s3Metadata)

	// Send the file up in 6mb chunks.
	url, err := s3Client.SaveLargeFileToS3(bucketName, localFile,
		"application/binary", file, fileInfo.Size(), options, int64(6000000))
	if err != nil {
		t.Error(err)
	}

	expectedUrl := fmt.Sprintf("https://s3.amazonaws.com/%s/%s",
		bucketName, localFile)
	if url != expectedUrl {
		t.Errorf("Expected url '%s' but got '%s'", expectedUrl, url)
	}
}

func TestFetchAndCalculateSha256(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Get an S3Client
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	// Create a GenericFile object that points to some of
	// our S3 fixture data.
	checksums := make([]*bagman.ChecksumAttribute, 2)
	checksums[0] = &bagman.ChecksumAttribute {
		Algorithm: "md5",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: "some bogus value",
	}
	// This is the actual sha256 checksum for the file we'll fetch...
	sha256sum := "fffcc1e5ca88b288fd4143ecf5ac2f184de1fe8a151d65eead4510748e57ecfa"
	checksums[1] = &bagman.ChecksumAttribute {
		Algorithm: "sha256",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: sha256sum,
	}
	genericFile := &bagman.GenericFile {
		URI: "https://s3.amazonaws.com/aptrust.test.fixtures/sample_good.tar",
		ChecksumAttributes: checksums,
	}

	// Check the SHA256 checksum on that file, but don't download it
	fixityResult := bagman.NewFixityResult(genericFile)
	err = s3Client.FetchAndCalculateSha256(fixityResult, "")

	if fixityResult.ErrorMessage != "" {
		t.Errorf("FetchAndCalculateSha256() resulted in an error: %s",
			fixityResult.ErrorMessage)
	}
	if fixityResult.Sha256 != sha256sum {
		t.Errorf("Expected sha256 '%s' but got '%s'", sha256sum, fixityResult.Sha256)
	}

	// Run the checksu AND save the file
	localPath := filepath.Join(
		config.ReplicationDirectory,
		"DownloadTestFile.tar")
	defer os.Remove(localPath)
	err = s3Client.FetchAndCalculateSha256(fixityResult, "")
	if err != nil {
		t.Error(err)
	}
	if fixityResult.Sha256 != sha256sum {
		t.Errorf("Expected sha256 '%s' but got '%s'", sha256sum, fixityResult.Sha256)
	}

}

func TestFetchToFileWithoutChecksum(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Get an S3Client
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	testConfig := "test"
	config := bagman.LoadRequestedConfig(&testConfig)
	localPath := filepath.Join(
		config.ReplicationDirectory,
		"DownloadTestFile.tar")
	defer os.Remove(localPath)

	err = s3Client.FetchToFileWithoutChecksum(
		"aptrust.test.fixtures",
		"sample_good.tar",
		localPath)

	if err != nil {
		t.Error(err)
		return
	}
	fileStat, err := os.Stat(localPath)
	if err != nil {
		t.Error(err)
	}
	// This is one of our fixture files.
	// We know it's size. It's on S3 and in
	// out testdata directory.
	if fileStat.Size() != int64(23552) {
		t.Errorf("Downloaded file %s is %d bytes. Expected 23552.",
			localPath, fileStat.Size())
	}
}

func TestExists(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Get an S3Client
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	exists, err := s3Client.Exists("aptrust.test.fixtures", "sample_good.tar")
	if err != nil {
		t.Error(err)
	}
	if exists == false {
		t.Errorf("s3Client.Exists() says sample_good.tar does not exist.")
	}

	exists, err = s3Client.Exists("aptrust.test.fixtures", "_no_such_file_")
	if err != nil {
		t.Error(err)
	}
	if exists == true {
		t.Errorf("s3Client.Exists() says _no_such_file_ exists.")
	}
}

func TestGetReader(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Get an S3Client
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	reader, err := s3Client.GetReader("aptrust.test.fixtures", "sample_good.tar")
	if err != nil {
		t.Error(err)
	}
	if reader == nil {
		t.Errorf("s3Client.GetReader() did not return a reader.")
	} else {
		// Read to end and close, or you'll leave an HTTP
		// connection to S3 hanging open.
		io.Copy(ioutil.Discard, reader)
		reader.Close()
	}

	reader, err = s3Client.GetReader("aptrust.test.fixtures", "_no_such_file_")
	if err == nil {
		t.Errorf("s3Client.GetReader() should have returned an error.")
	}
	if reader != nil {
		t.Errorf("s3Client.GetReader() returned a reader when it shouldn't have.")
		io.Copy(ioutil.Discard, reader)
		reader.Close()
	}

}

func TestHead(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("s3_test.go")
		return
	}

	// Get an S3Client
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Errorf("Cannot create S3 client: %v\n", err)
	}

	httpResp, err := s3Client.Head("aptrust.test.fixtures", "sample_good.tar")
	if err != nil {
		t.Error(err)
	}
	if httpResp == nil {
		t.Errorf("s3Client.Head() did not return an HTTP response.")
	} else {
		// Don't leave the connection hanging.
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}
}
