package bagman_test

import (
	"errors"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var gopath string = os.Getenv("GOPATH")
var testDataPath = filepath.Join(gopath, "src/github.com/APTrust/bagman/testdata")
var sampleBadChecksums string = filepath.Join(testDataPath, "example.edu.sample_bad_checksums.tar")
var sampleGood string = filepath.Join(testDataPath, "example.edu.sample_good.tar")
var sampleMultipart1 string = filepath.Join(testDataPath, "example.edu.multipart.b01.of02.tar")
var sampleMultipart2 string = filepath.Join(testDataPath, "example.edu.multipart.b02.of02.tar")
var sampleGoodUntarred string = filepath.Join(testDataPath, "example.edu.sample_good")
var sampleMissingDataFile string = filepath.Join(testDataPath, "example.edu.sample_missing_data_file.tar")
var sampleNoBagInfo string = filepath.Join(testDataPath, "example.edu.sample_no_bag_info.tar")
var sampleNoBagit string = filepath.Join(testDataPath, "example.edu.sample_no_bagit.tar")
var sampleWrongFolderName string = filepath.Join(testDataPath, "example.edu.sample_wrong_folder_name.tar")
var sampleNoTitle string = filepath.Join(testDataPath, "example.edu.sample_no_title.tar")
var sampleBadAccess string = filepath.Join(testDataPath, "example.edu.sample_bad_access.tar")
var sampleNoMd5Manifest string = filepath.Join(testDataPath, "example.edu.sample_no_md5_manifest.tar")
var sampleNoAPTrustInfo string = filepath.Join(testDataPath, "example.edu.sample_no_aptrust_info.tar")
var sampleNoDataDir string = filepath.Join(testDataPath, "example.edu.sample_no_data_dir.tar")
var invalidTarFile string = filepath.Join(testDataPath, "example.edu.not_a_tar_file.tar")

var tagSampleGood string = filepath.Join(testDataPath, "example.edu.tagsample_good.tar")
var tagSampleBad string = filepath.Join(testDataPath, "example.edu.tagsample_bad.tar")

var badFiles []string = []string{
	sampleBadChecksums,
	sampleMissingDataFile,
	sampleNoBagInfo,
	sampleNoBagit,
	sampleWrongFolderName,
	sampleNoMd5Manifest,
	sampleNoAPTrustInfo,
	sampleNoDataDir,
	tagSampleBad,
}
var goodFiles []string = []string{
	sampleGood,
	tagSampleGood,
}
var allFiles []string = append(badFiles, goodFiles...)

// Setup to run before tests
func setup() {

}

// Teardown to run after tests. This deletes the directories
// that were created when tar files were unpacked. We don't
// delete example.edu.sample_good, because that's a fixture
// used in validate_test.go.
func teardown() {
	files, err := ioutil.ReadDir(testDataPath)
	if err != nil {
		fmt.Errorf("Can't cleanup %s: %s", testDataPath, err.Error())
		return
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() && !strings.Contains(fileInfo.Name(), "example.edu.sample_good") {
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
	// Untar with ingest data (sha256 and mime type)
	for _, tarFile := range goodFiles {
		result := bagman.Untar(tarFile, "test.edu", "good_test_bag.tar", true)
		if result.ErrorMessage != "" {
			t.Errorf("Error untarring %s: %v", tarFile, result.ErrorMessage)
		}
		if len(result.FilesUnpacked) == 0 {
			t.Errorf("Untar did not seem to unpack anything from %s", tarFile)
		}
	}
	// Untar without ingest data
	for _, tarFile := range goodFiles {
		result := bagman.Untar(tarFile, "test.edu", "good_test_bag.tar", false)
		if result.ErrorMessage != "" {
			t.Errorf("Error untarring %s: %v", tarFile, result.ErrorMessage)
		}
		if len(result.FilesUnpacked) == 0 {
			t.Errorf("Untar did not seem to unpack anything from %s", tarFile)
		}
	}
}

func TestUntarCreatesFiles(t *testing.T) {
	tarResult := bagman.Untar(sampleGood, "ncsu.edu", "ncsu.1840.16-2928.tar", true)

	// Generic files contains info about files in the /data directory
	expectedPath := []string{
		"aptrust-info.txt",
		"bag-info.txt",
		"data/datastream-DC",
		"data/datastream-descMetadata",
		"data/datastream-MARC",
		"data/datastream-RELS-EXT",
	}
	expectedIdentifier := []string{
		"ncsu.1840.16-2928/aptrust-info.txt",
		"ncsu.1840.16-2928/bag-info.txt",
		"ncsu.1840.16-2928/data/datastream-DC",
		"ncsu.1840.16-2928/data/datastream-descMetadata",
		"ncsu.1840.16-2928/data/datastream-MARC",
		"ncsu.1840.16-2928/data/datastream-RELS-EXT",
	}
	expectedMd5 := []string{
		"32cea37b3bd418dd0028d6197aefc487",
		"82c47e0acbf13e3259cb19f88f6304d1",
		"44d85cf4810d6c6fe87750117633e461",
		"4bd0ad5f85c00ce84a455466b24c8960",
		"93e381dfa9ad0086dbe3b92e0324bae6",
		"ff731b9a1758618f6cc22538dede6174",
	}
	expectedSha256 := []string{
		"4f662a48a65e6502a56d851d1b1398fd726d28badb63310430d57f7eede8e9de",
		"3fafc2c1685fb9db715b884a1ef33eb5061c17f3fb671fa7cc99037e174b7553",
		"248fac506a5c46b3c760312b99827b6fb5df4698d6cf9a9cdc4c54746728ab99",
		"cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7",
		"8e3634d207017f3cfc8c97545b758c9bcd8a7f772448d60e196663ac4b62456a",
		"299e1c23e398ec6699976cae63ef08167201500fa64bcf18062111e0c81d6a13",
	}
	expectedType := []string{
		"text/plain",
		"text/plain",
		"text/plain",
		"application/xml",
		"text/plain",
		"application/xml",
	}
	expectedSize := []int64{49, 223, 2388, 6191, 4663, 579}
	t0, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 16:51:53 -0400 EDT")
	t1, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 20:54:13 +0000 UTC")
	t2, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 16:51:53 -0400 EDT")
	t3, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 16:51:53 -0400 EDT")
	t4, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 16:51:53 -0400 EDT")
	t5, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2014-12-12 16:51:53 -0400 EDT")
	expectedModTime := []time.Time{t0, t1, t2, t3, t4, t5}

	if len(tarResult.Files) != 6 {
		t.Errorf("Unpacked %d generic files, expected %d", len(tarResult.Files), 4)
	}

	emptyTime := time.Time{}
	for index, file := range tarResult.Files {
		if file.Path != expectedPath[index] {
			t.Errorf("File path '%s' is incorrect, expected '%s'", file.Path, expectedPath[index])
		}
		if file.Identifier != expectedIdentifier[index] {
			t.Errorf("File identifier '%s' is incorrect, expected '%s'",
				file.Identifier, expectedIdentifier[index])
		}
		if file.Md5 != expectedMd5[index] {
			t.Errorf("File md5 sum '%s' should be '%s'", file.Md5, expectedMd5[index])
		}
		if file.Sha256 != expectedSha256[index] {
			t.Errorf("File sha256 sum '%s' should be '%s'", file.Sha256, expectedSha256[index])
		}
		if len(file.Uuid) != 36 {
			t.Errorf("File UUID '%s' should be 36 characters", file.Uuid)
		}
		if file.Size != expectedSize[index] {
			t.Errorf("File size %d should be %d", file.Size, expectedSize[index])
		}
		if file.MimeType != expectedType[index] {
			t.Errorf("File type '%s' should be '%s'", file.MimeType, expectedType[index])
		}
		if file.Sha256Generated == emptyTime {
			t.Error("File.Sha256Generated timestamp is missing")
		}
		if file.UuidGenerated == emptyTime {
			t.Error("File.UuidGenerated timestamp is missing")
		}
		if file.Modified.UTC() != expectedModTime[index].UTC() {
			t.Errorf("File modtime '%v' should be '%v'", file.Modified.UTC(), expectedModTime[index].UTC())
		}
	}

}

// Make sure Untar doesn't blow up when it gets an invalid
// or corrupt tar file. It should return a TarResult with an
// Error property.
func TestUntarSetsErrorOnBadFile(t *testing.T) {
	result := bagman.Untar(invalidTarFile, "ncsu.edu", "ncsu.1840.16-2928.tar", true)
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
	tarResult := bagman.Untar(sampleGood, "ncsu.edu", "ncsu.1840.16-2928.tar", true)
	result := bagman.ReadBag(tarResult.OutputDir)
	if result.Path != tarResult.OutputDir {
		t.Errorf("Result path %s is incorrect, expected %s", result.Path, tarResult.OutputDir)
	}

	// Files contains a list of ALL files unpacked from the bag,
	// including manifests and tag files.
	if len(result.Files) != 8 {
		t.Errorf("Unpacked %d files, expected %d", len(result.Files), 8)
		for _, f := range result.Files {
			fmt.Println(f)
		}
	}

	if result.ErrorMessage != "" {
		t.Errorf("Unexpected error in read result: %v", result.ErrorMessage)
	}

	// All tags should be present and in the correct order
	if len(result.Tags) != 10 {
		t.Errorf("Expected 10 tags, got %d", len(result.Tags))
	}

	err := assertTagMatch(result.Tags[0], "BagIt-Version", "0.97")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[1], "Tag-File-Character-Encoding", "UTF-8")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[2], "Source-Organization", "virginia.edu")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[3], "Bagging-Date", "2014-04-14T11:55:26.17-0400")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[4], "Bag-Count", "1 of 1")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[5], "Bag-Group-Identifier", "Charley Horse")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[6], "Internal-Sender-Description", "Bag of goodies")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[7], "Internal-Sender-Identifier", "uva-internal-id-0001")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[8], "Title", "Strabo De situ orbis.")
	if err != nil {
		t.Error(err)
	}

	err = assertTagMatch(result.Tags[9], "Access", "Institution")
	if err != nil {
		t.Error(err)
	}

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
	for _, tarFile := range badFiles {
		tarResult := bagman.Untar(tarFile, "test.edu", "bad_test_bag.tar", true)
		result := bagman.ReadBag(tarResult.OutputDir)
		if result.ErrorMessage == "" {
			t.Errorf("Bag unpacked from %s should have produced an error, but did not",
				tarResult.OutputDir)
		}
	}
}

// If the top-level directory of the untarred file does not
// match the name of the tar file minus the .tar extension,
// we should get an error message in the TarResult. (E.g.
// If my_file.tar does not untar to a dir called my_file,
// we should get an error message.)
func TestErrorOnBadFolderName(t *testing.T) {
	setup()
	defer teardown()
	result := bagman.Untar(sampleWrongFolderName, "ncsu.edu", "ncsu.1840.16-2928.tar", true)
	if !strings.Contains(result.ErrorMessage, "should untar to a folder named") {
		t.Errorf("Untarring file '%s' should have generated an 'incorrect file name' error.",
			sampleWrongFolderName)
	}
}

func TestErrorOnBadAccessValue(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(sampleBadAccess, "ncsu.edu", "ncsu.1840.16-2928.tar", true)
	readResult := bagman.ReadBag(tarResult.OutputDir)
	if !strings.Contains(readResult.ErrorMessage, "access (rights) value") {
		t.Errorf("File '%s' should have generated an 'invalid access value' error.",
			sampleBadAccess)
	}
}

func TestErrorOnMissingTitle(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(sampleNoTitle, "ncsu.edu", "ncsu.1840.16-2928.tar", true)
	readResult := bagman.ReadBag(tarResult.OutputDir)
	if !strings.Contains(readResult.ErrorMessage, "Title is missing") {
		t.Errorf("File '%s' should have generated a missing title error.",
			sampleNoTitle)
	}
}


func TestGoodCustomTags(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(tagSampleGood, "test.edu", "tag_sample_good.tar", true)
	readResult := bagman.ReadBag(tarResult.OutputDir)
	if readResult.ErrorMessage != "" {
		t.Errorf("ReadBag() should have found no errors in %s, " +
			"but it found the following:\n%s", tagSampleGood, readResult.ErrorMessage)
	}
}

func TestBadCustomTags(t *testing.T) {
	setup()
	defer teardown()
	tarResult := bagman.Untar(tagSampleBad, "test.edu", "tag_sample_bad.tar", true)
	readResult := bagman.ReadBag(tarResult.OutputDir)

	if !strings.Contains(readResult.ErrorMessage, "checksum 00000000000000000000000000000000 is not valid") {
		t.Errorf("Validator missed invalid checksum on custom_tags/tracked_tag_file.txt")
	}
	if !strings.Contains(readResult.ErrorMessage, "checksum 99999999999999999999999999999999 is not valid") {
		t.Errorf("Validator missed invalid checksum on custom_tags/tag_file_xyz.pdf")
	}
	if !strings.Contains(readResult.ErrorMessage,
		"checksum 0000000000000000000000000000000000000000000000000000000000000000 is not valid") {
		t.Errorf("Validator missed invalid checksum on custom_tags/tracked_tag_file.txt")
	}
	if !strings.Contains(readResult.ErrorMessage,
		"checksum 9999999999999999999999999999999999999999999999999999999999999999 is not valid") {
		t.Errorf("Validator missed invalid checksum on custom_tags/tag_file_xyz.pdf")
	}
	if !strings.Contains(readResult.ErrorMessage, "tag_file_xyz.pdf: no such file or directory") {
		t.Errorf("Validator did not report missing file custom_tags/tag_file_xyz.pdf")
	}
}
