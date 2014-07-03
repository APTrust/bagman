package bagman_test

import (
	"testing"
	"github.com/APTrust/bagman"
	"errors"
	"os"
	"fmt"
	"time"
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
		result := bagman.Untar(tarFile, "ncsu.edu", "ncsu.1840.16-2928.tar")
		if result.ErrorMessage != "" {
			t.Errorf("Error untarring %s: %v", tarFile, result.ErrorMessage)
		}
		if len(result.FilesUnpacked) == 0 {
			t.Errorf("Untar did not seem to unpack anything from %s", tarFile)
		}
	}
}

func TestUntarCreatesGenericFiles(t *testing.T) {
	tarResult := bagman.Untar(sampleGood, "ncsu.edu", "ncsu.1840.16-2928.tar")

	// Generic files contains info about files in the /data directory
	expectedPath := []string{
		"data/datastream-DC",
		"data/datastream-descMetadata",
		"data/datastream-MARC",
		"data/datastream-RELS-EXT",
	}
	expectedMd5 := []string{
		"44d85cf4810d6c6fe87750117633e461",
		"4bd0ad5f85c00ce84a455466b24c8960",
		"93e381dfa9ad0086dbe3b92e0324bae6",
		"ff731b9a1758618f6cc22538dede6174",
	}
	expectedSha256 := []string{
		"248fac506a5c46b3c760312b99827b6fb5df4698d6cf9a9cdc4c54746728ab99",
		"cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7",
		"8e3634d207017f3cfc8c97545b758c9bcd8a7f772448d60e196663ac4b62456a",
		"299e1c23e398ec6699976cae63ef08167201500fa64bcf18062111e0c81d6a13",
	}
	expectedType := []string{
		"text/plain",
		"application/xml",
		"text/plain",
		"application/xml",
	}
	expectedSize := []int64{ 2388, 6191, 4663, 579 }
	t0, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-04-14 11:55:25 -0400 EDT")
	t1, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-04-14 11:55:25 -0400 EDT")
	t2, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-04-14 11:55:26 -0400 EDT")
	t3, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-04-14 11:55:26 -0400 EDT")
	expectedModTime := []time.Time {t0, t1, t2, t3}

	if len(tarResult.GenericFiles) != 4 {
		t.Errorf("Unpacked %d generic files, expected %d", len(tarResult.GenericFiles), 4)
	}

	emptyTime := time.Time{}
	for index, gf := range tarResult.GenericFiles {
		if gf.Path != expectedPath[index] {
			t.Errorf("GenericFile path '%s' is incorrect, expected '%s'", gf.Path, expectedPath[index])
		}
		if gf.Md5 != expectedMd5[index] {
			t.Errorf("GenericFile md5 sum '%s' should be '%s'", gf.Md5, expectedMd5[index])
		}
		if gf.Sha256 != expectedSha256[index] {
			t.Errorf("GenericFile sha256 sum '%s' should be '%s'", gf.Sha256, expectedSha256[index])
		}
		if len(gf.Uuid) != 36 {
			t.Errorf("GenericFile UUID '%s' should be 36 characters", gf.Uuid)
		}
		if gf.Size != expectedSize[index] {
			t.Errorf("GenericFile size %d should be %d", gf.Size, expectedSize[index])
		}
		if gf.MimeType != expectedType[index] {
			t.Errorf("GenericFile type '%s' should be '%s'", gf.MimeType, expectedType[index])
		}
		if gf.Sha256Generated == emptyTime {
			t.Error("GenericFile.Sha256Generated timestamp is missing")
		}
		if gf.UuidGenerated == emptyTime {
			t.Error("GenericFile.UuidGenerated timestamp is missing")
		}
		if gf.Modified.UTC() != expectedModTime[index].UTC() {
			t.Errorf("GenericFile modtime '%v' should be '%v'", gf.Modified.UTC(), expectedModTime[index].UTC())
		}
	}

}

// Make sure Untar doesn't blow up when it gets an invalid
// or corrupt tar file. It should return a TarResult with an
// Error property.
func TestUntarSetsErrorOnBadFile(t *testing.T) {
	result := bagman.Untar(invalidTarFile, "ncsu.edu", "ncsu.1840.16-2928.tar")
	if result.ErrorMessage == "" {
		t.Errorf("Untar should have reported an error about a bad tar file, but did not.")
	}
}

// Make sure we can parse a bag that is known to be good, and that we
// get the right data in the results. This is not a strict unit test,
// since it depends on bagman.Untar succeeding.
func TestGoodBagParsesCorrectly(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(sampleGood, "ncsu.edu", "ncsu.1840.16-2928.tar")
	result := bagman.ReadBag(tarResult.OutputDir)
	if result.Path != tarResult.OutputDir {
		t.Errorf("Result path %s is incorrect, expected %s", result.Path, tarResult.OutputDir)
	}

	// Files contains a list of ALL files unpacked from the bag,
	// including manifests and tag files.
	if len(result.Files) != 8 {
		t.Errorf("Unpacked %d files, expected %d", len(result.Files), 8)
	}


	if result.ErrorMessage != "" {
		t.Errorf("Unexpected error in read result: %v", result.ErrorMessage)
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

	err = assertTagMatch(result.Tags[9], "Access", "Institution")
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
		tarResult := bagman.Untar(tarFile, "ncsu.edu", "ncsu.1840.16-2928.tar")
		result := bagman.ReadBag(tarResult.OutputDir)
		if result.ErrorMessage == "" {
			t.Errorf("Bag unpacked from %s should have produced an error, but did not",
				tarResult.OutputDir)
		}
	}
	fmt.Fprintf(os.Stderr, "\n")
}
