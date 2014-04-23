package bagman_test

import (
	"testing"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagins"
	"errors"
	"os"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

var gopath string = os.Getenv("GOPATH")
var testDataPath = filepath.Join(gopath, "src/github.com/APTrust/bagman/testdata")
var sampleBadChecksums string = filepath.Join(testDataPath, "sample_bad_checksums.tar")
var sampleGood string = filepath.Join(testDataPath, "sample_good.tar")
var sampleMissingDataFile string = filepath.Join(testDataPath, "sample_missing_data_file.tar")
var sampleNoBagInfo string = filepath.Join(testDataPath, "sample_no_bag_info.tar")
var sampleNoBagit string = filepath.Join(testDataPath, "sample_no_bagit.tar")
var badFiles []string = []string{
	sampleBadChecksums,
	sampleMissingDataFile,
	sampleNoBagInfo,
	sampleNoBagit,
}
var goodFiles []string = []string{
	sampleGood,
}
var allFiles []string = append(badFiles, goodFiles ...)

// Setup to run before tests
func setup() {

}

// Teardown to run after tests. This deletes the directories
// that were created when tar files were unpacked.
func teardown() {
	files, err := ioutil.ReadDir(testDataPath)
	if err != nil {
		fmt.Errorf("Can't cleanup %s: %s", testDataPath, err.Error())
		return
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			subDir := filepath.Join(testDataPath, fileInfo.Name())
			err := os.RemoveAll(subDir)
			if err != nil {
				fmt.Errorf("Test cleanup was unable delete %s", subDir)
			}
		}
	}
}

// Check to see if the label and value of a tag match what
// we're expecting. If the label or value does not match
// what's expected, return an error. Otherwise return nil.
func assertTagMatch(tag bagins.TagField, expectedLabel string, expectedValue string) (err error) {
	if tag.Label() != expectedLabel || tag.Value() != expectedValue {
		return errors.New(fmt.Sprintf("Expected tag '%s: %s', got '%s: %s'",
			expectedLabel, expectedValue, tag.Label(), tag.Value()))
	}
	return nil
}

// Make sure we can parse a bag that is known to be good, and that we
// get the right data in the results. This is not a strict unit test,
// since it depends on bagman.Untar succeeding.
func TestGoodBagParsesCorrectly(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(sampleGood)
	result := bagman.ReadBag(tarResult.OutputDir)
	if result.Path != tarResult.OutputDir {
		t.Errorf("Result path %s is incorrect, expected %s", result.Path, tarResult.OutputDir)
	}
	if len(result.Files) != 8 {
		t.Errorf("Unpacked %d files, expected %d", len(result.Files), 8)
	}
	if result.Error != nil {
		t.Errorf("Unexpected error in read result: %v", result.Error)
	}
	// Note that we're testing to see not only that the tags are present,
	// but that they are in the correct order.
	//
	// TODO: Bagins is returning one extra empty tag as the first element in every tag list
	// from every bag file. We need to fix that.
	if len(result.Tags) != 6 {
		t.Errorf("Expected 6 tags, got %d", len(result.Tags))
	}
	// TODO: This empty tag should not be here.
	// if err := assertTagMatch(result.Tags[0], "", ""); err != nil { t.Error(err) }

	err := assertTagMatch(result.Tags[0], "Source-Organization", "virginia.edu")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[1], "Bagging-Date", "2014-04-14T11:55:26.17-0400")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[2], "Bag-Count", "1 of 1")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[3], "Bag-Group-Identifier", "")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[4], "Internal-Sender-Description", "")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[5], "Internal-Sender-Identifier", "")
	if err != nil { t.Error(err) }

	if len(result.ChecksumErrors) != 0 {
		t.Errorf("Bag read result contained %d checksum errors; it should have none",
			len(result.ChecksumErrors))
	}
}

func TestBadBagReturnsError(t *testing.T) {
	setup()
	defer teardown()
	for _, tarFile := range badFiles {
		tarResult := bagman.Untar(tarFile)
		result := bagman.ReadBag(tarResult.OutputDir)
		if result.Error == nil {
			t.Errorf("Bag unpacked from %s should have produced an error, but did not",
				tarResult.OutputDir)
		}
	}
}
