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
	if result.Retry == true {
		t.Errorf("Retry should have been set to false after fatal error.")
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

	// Make sure we get specific message when the GenericFile
	// object does not include the expected checksums.
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
}

func TestBuildPremisEvent_Success(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	premisEvent, err := result.BuildPremisEvent()
	if err != nil {
		t.Errorf("BuildPremisEvent() returned an error: %v", err)
	}
	if len(premisEvent.Identifier) != 36 {
		t.Errorf("PremisEvent.Identifier '%s' is not a valid UUID", premisEvent.Identifier)
	}
	if premisEvent.EventType != "fixity_check" {
		t.Errorf("PremisEvent.EventType '%s' should be 'fixity_check'", premisEvent.EventType)
	}
	if time.Now().Unix() - premisEvent.DateTime.Unix() > 5 {
		t.Errorf("PremisEvent.DateTime should be close to current time, but it's not.")
	}
	if premisEvent.Detail != "Fixity check against registered hash" {
		t.Errorf("Unexpected PremisEvent.Detail '%s'", premisEvent.Detail)
	}
	if premisEvent.Outcome != "success" {
		t.Errorf("PremisEvent.Outcome expected 'success' but got '%s'", premisEvent.Outcome)
	}
	if premisEvent.OutcomeDetail != sha256sum {
		t.Errorf("PremisEvent.OutcomeDetail expected '%s' but got '%s'",
			sha256sum, premisEvent.OutcomeDetail)
	}
	if premisEvent.Object != "Go language cryptohash" {
		t.Errorf("PremisEvent.Outcome expected 'Go language cryptohash' but got '%s'",
			premisEvent.Object)
	}
	if premisEvent.Agent != "http://golang.org/pkg/crypto/sha256/" {
		t.Errorf("PremisEvent.Outcome expected 'http://golang.org/pkg/crypto/sha256/' but got '%s'",
			premisEvent.Agent)
	}
	if premisEvent.OutcomeInformation != "Fixity matches" {
		t.Errorf("PremisEvent.OutcomeInformation expected 'Fixity matches' but got '%s'",
			premisEvent.OutcomeInformation)
	}
}

func TestBuildPremisEvent_Failure(t *testing.T) {
	result := bagman.NewFixityResult(getGenericFile())
	result.Sha256 = "xxx-xxx-xxx"
	premisEvent, err := result.BuildPremisEvent()
	if err != nil {
		t.Errorf("BuildPremisEvent() returned an error: %v", err)
	}
	if len(premisEvent.Identifier) != 36 {
		t.Errorf("PremisEvent.Identifier '%s' is not a valid UUID", premisEvent.Identifier)
	}
	if premisEvent.EventType != "fixity_check" {
		t.Errorf("PremisEvent.EventType '%s' should be 'fixity_check'", premisEvent.EventType)
	}
	if time.Now().Unix() - premisEvent.DateTime.Unix() > 5 {
		t.Errorf("PremisEvent.DateTime should be close to current time, but it's not.")
	}
	if premisEvent.Detail != "Fixity does not match expected value" {
		t.Errorf("Unexpected PremisEvent.Detail '%s'", premisEvent.Detail)
	}
	if premisEvent.Outcome != "failure" {
		t.Errorf("PremisEvent.Outcome expected 'failure' but got '%s'", premisEvent.Outcome)
	}
	if premisEvent.OutcomeDetail != result.Sha256 {
		t.Errorf("PremisEvent.OutcomeDetail expected '%s' but got '%s'",
			sha256sum, premisEvent.OutcomeDetail)
	}
	if premisEvent.Object != "Go language cryptohash" {
		t.Errorf("PremisEvent.Outcome expected 'Go language cryptohash' but got '%s'",
			premisEvent.Object)
	}
	if premisEvent.Agent != "http://golang.org/pkg/crypto/sha256/" {
		t.Errorf("PremisEvent.Outcome expected 'http://golang.org/pkg/crypto/sha256/' but got '%s'",
			premisEvent.Agent)
	}
	if premisEvent.OutcomeInformation != result.ErrorMessage {
		t.Errorf("PremisEvent.OutcomeInformation expected '%s' but got '%s'",
			result.ErrorMessage, premisEvent.OutcomeInformation)
	}
}
