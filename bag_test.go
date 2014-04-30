package bagman_test

import (
	"testing"
	"github.com/APTrust/bagman"
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
var invalidTarFile string = filepath.Join(testDataPath, "not_a_tar_file.tar")
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
func assertTagMatch(tag bagman.Tag, expectedLabel string, expectedValue string) (err error) {
	if tag.Label != expectedLabel || tag.Value != expectedValue {
		return errors.New(fmt.Sprintf("Expected tag '%s: %s', got '%s: %s'",
			expectedLabel, expectedValue, tag.Label, tag.Value))
	}
	return nil
}

// Make sure we can untar properly formatted tar files.
func TestUntarWorksOnGoodFiles(t *testing.T) {
	setup()
	defer teardown()
	for _, tarFile := range allFiles {
		result := bagman.Untar(tarFile)
		if result.Error != nil {
			t.Errorf("Error untarring %s: %v", tarFile, result.Error)
		}
		if len(result.FilesUnpacked) == 0 {
			t.Errorf("Untar did not seem to unpack anything from %s", tarFile)
		}
	}
}

// Make sure Untar doesn't blow up when it gets an invalid
// or corrupt tar file. It should return a TarResult with an
// Error property.
func TestUntarSetsErrorOnBadFile(t *testing.T) {
	result := bagman.Untar(invalidTarFile)
	if result.Error == nil {
		t.Errorf("Untar should have reported an error about a bad tar file, but did not.")
	}
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

	// Files contains a list of ALL files unpacked from the bag,
	// including manifests and tag files.
	if len(result.Files) != 8 {
		t.Errorf("Unpacked %d files, expected %d", len(result.Files), 8)
	}

	// Generic files contains info about files in the /data directory
	if len(result.GenericFiles) != 4 {
		t.Errorf("Unpacked %d generic files, expected %d", len(result.GenericFiles), 4)
	}
	for _, gf := range result.GenericFiles {
		if len(gf.Path) <= len(result.Path) {
			t.Errorf("GenericFile path '%s' is incorrect", gf.Path)
		}
		if len(gf.Md5) != 32 {
			t.Errorf("GenericFile md5 sum '%s' should be 32 characters", gf.Md5)
		}
		if len(gf.Sha256) != 64 {
			t.Errorf("GenericFile sha256 sum '%s' should be 64 characters", gf.Sha256)
		}
		if len(gf.Uuid) != 36 {
			t.Errorf("GenericFile UUID '%s' should be 36 characters", gf.Uuid)
		}
	}

	if result.Error != nil {
		t.Errorf("Unexpected error in read result: %v", result.Error)
	}

	// All tags should be present and in the correct order
	if len(result.Tags) != 10 {
		t.Errorf("Expected 10 tags, got %d", len(result.Tags))
	}

	err := assertTagMatch(result.Tags[0], "BagIt-Version", "0.97")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[1], "Tag-File-Character-Encoding", "UTF-8")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[2], "Source-Organization", "virginia.edu")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[3], "Bagging-Date", "2014-04-14T11:55:26.17-0400")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[4], "Bag-Count", "1 of 1")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[5], "Bag-Group-Identifier", "")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[6], "Internal-Sender-Description", "")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[7], "Internal-Sender-Identifier", "")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[8], "Title", "Strabo De situ orbis.")
	if err != nil { t.Error(err) }

	err = assertTagMatch(result.Tags[9], "Rights", "Institution")
	if err != nil { t.Error(err) }

	if len(result.ChecksumErrors) != 0 {
		t.Errorf("Bag read result contained %d checksum errors; it should have none",
			len(result.ChecksumErrors))
	}
}

// Make sure each of the bad bags produces an error in the BagReadResult.
// The underlying bagins library prints some warnings to stderr, so we
// include a note that those are expected.
func TestBadBagReturnsError(t *testing.T) {
	setup()
	defer teardown()
	fmt.Fprintf(os.Stderr, "Warnings below about missing bag-info/bagit files are expected.\n")
	fmt.Fprintf(os.Stderr, "Tests are checking to see if the bag reader handles these cases.\n\n")
	for _, tarFile := range badFiles {
		tarResult := bagman.Untar(tarFile)
		result := bagman.ReadBag(tarResult.OutputDir)
		if result.Error == nil {
			t.Errorf("Bag unpacked from %s should have produced an error, but did not",
				tarResult.OutputDir)
		}
	}
	fmt.Fprintf(os.Stderr, "\n")
}
