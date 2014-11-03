package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"strings"
	"testing"
	"time"
)

var md5sum = "1234567890"
var sha256sum = "fedcba9876543210"

func getGenericFile() (*bagman.GenericFile) {
	checksums := make([]*bagman.ChecksumAttribute, 2)
	checksums[0] = &bagman.ChecksumAttribute {
		Algorithm: "md5",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: md5sum,
	}
	checksums[1] = &bagman.ChecksumAttribute {
		Algorithm: "sha256",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: sha256sum,
	}
	return &bagman.GenericFile{
		URI: "https://s3.amazonaws.com/aptrust.preservation.storage/52a928da-89ef-48c6-4627-826d1858349b",
		ChecksumAttributes: checksums,
	}
}

func TestBucketAndKey(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	bucket, key, err := result.BucketAndKey()
	if err != nil {
		t.Errorf("BucketAndKey() returned error: %v", err)
	}
	if bucket != "aptrust.preservation.storage" {
		t.Errorf("BucketAndKey() returned bucket name '%s', expected 'aptrust.preservation.storage'", bucket)
	}
	if key != "52a928da-89ef-48c6-4627-826d1858349b" {
		t.Errorf("BucketAndKey() returned key '%s', expected '52a928da-89ef-48c6-4627-826d1858349b'", key)
	}
}

func TestBucketAndKeyWithBadUri(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	result.GenericFile.URI = "http://example.com"
	_, _, err := result.BucketAndKey()
	if err == nil {
		t.Errorf("BucketAndKey() should have returned an error for invalid URI")
		return
	}
	if result.ErrorMessage != "GenericFile URI 'http://example.com' is invalid" {
		t.Errorf("BucketAndKey() did not set descriptive error message for bad URI")
	}
}

func TestMd5Matches(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	result.Md5 = md5sum
	if result.Md5Matches() == false {
		t.Errorf("Md5Matches should have returned true")
	}
	result.Md5 = "some random string"
	if result.Md5Matches() == true {
		t.Errorf("Md5Matches should have returned false")
	}
	expectedMessage := fmt.Sprintf(
		"Current md5 digest 'some random string' does not match Fedora digest '%s'",
		md5sum)
	if result.ErrorMessage != expectedMessage {
		t.Errorf("Expected ErrorMessage '%s' but got '%s' instead",
			expectedMessage, result.ErrorMessage)
	}
}

func TestSha256Matches(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	if result.Sha256Matches() == false {
		t.Errorf("Sha256Matches should have returned true")
	}
	result.Sha256 = "some random string"
	if result.Sha256Matches() == true {
		t.Errorf("Sha256Matches should have returned false")
	}
	expectedMessage := fmt.Sprintf(
		"Current sha256 digest 'some random string' does not match Fedora digest '%s'",
		sha256sum)
	if result.ErrorMessage != expectedMessage {
		t.Errorf("Expected ErrorMessage '%s' but got '%s' instead",
			expectedMessage, result.ErrorMessage)
	}
}

// We have to know WHY things failed!
func TestMissingChecksums(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	if result.Sha256Matches() == true {
		t.Errorf("Sha256Matches should have returned false")
	}
	if strings.Index(result.ErrorMessage, "FixityResult object is missing") < 0 {
		t.Errorf("Descriptive error message is missing or incorrect")
	}

	result.ErrorMessage = ""
	if result.Md5Matches() == true {
		t.Errorf("Md5Matches should have returned false")
	}
	if strings.Index(result.ErrorMessage, "FixityResult object is missing") < 0 {
		t.Errorf("Descriptive error message is missing or incorrect")
	}

	// Make sure we get specific message when the GenericFile
	// object does not include the expected checksums.
	result.Md5 = md5sum
	result.Sha256 = sha256sum
	result.GenericFile.ChecksumAttributes = make([]*bagman.ChecksumAttribute, 2)
	result.ErrorMessage = ""
	expectedError := "GenericFile record from Fedora is missing sha256 digest!"
	if result.Sha256Matches() == true {
		t.Errorf("Sha256Matches should have returned false")
	}
	if result.ErrorMessage != expectedError {
		t.Errorf("Expected error message '%s' but got '%s'", expectedError, result.ErrorMessage)
	}

	result.ErrorMessage = ""
	expectedError = "GenericFile record from Fedora is missing md5 digest!"
	if result.Md5Matches() == true {
		t.Errorf("Md5Matches should have returned false")
	}
	if result.ErrorMessage != expectedError {
		t.Errorf("Expected error message '%s' but got '%s'", expectedError, result.ErrorMessage)
	}
}
