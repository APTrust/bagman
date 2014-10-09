package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"github.com/crowdmob/goamz/s3"
	"testing"
)

func testFile() (*bagman.S3File) {
	return &bagman.S3File{
		BucketName: "aptrust.receiving.uc.edu",
		Key: s3.Key{
			Key: "cin.675812.tar",
		},
	}
}

func TestS3BagName(t *testing.T) {
	s3File := testFile()
	bagname := s3File.BagName()
	if bagname != "uc.edu/cin.675812.tar" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.675812.tar'", bagname)
	}
}

func TestObjectName(t *testing.T) {
	s3File := testFile()

	// Test with single-part bag
	objname, err := s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	if objname != "uc.edu/cin.675812" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.675812'", objname)
	}

	// Test with multi-part bag
	s3File.Key.Key = "cin.1234.b003.of191.tar"
	objname, err = s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	if objname != "uc.edu/cin.1234" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.1234'", objname)
	}
}
