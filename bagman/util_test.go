package bagman_test

import (
	"archive/tar"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBagmanHome(t *testing.T) {
	bagmanHome := os.Getenv("BAGMAN_HOME")
	goHome := os.Getenv("GOPATH")
	defer os.Setenv("BAGMAN_HOME", bagmanHome)
	defer os.Setenv("GOPATH", goHome)

	// Should use BAGMAN_HOME, if it's set...
	os.Setenv("BAGMAN_HOME", "/bagman_home")
	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/bagman_home" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/bagman_home")
	}
	os.Setenv("BAGMAN_HOME", "")

	// Otherwise, should use GOPATH
	os.Setenv("GOPATH", "/go_home")
	bagmanHome, err = bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/go_home/src/github.com/APTrust/bagman" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/go_home")
	}
	os.Setenv("GOPATH", "")

	// Without BAGMAN_HOME and GOPATH, we should get an error
	bagmanHome, err = bagman.BagmanHome()
	if err == nil {
		t.Error("BagmanHome should have an thrown exception.")
	}
}

func TestLoadRelativeFile(t *testing.T) {
	path := filepath.Join("testdata", "result_good.json")
	data, err := bagman.LoadRelativeFile(path)
	if err != nil {
		t.Error(err)
	}
	if data == nil || len(data) == 0 {
		t.Errorf("Read no data out of file '%s'", path)
	}
}

func TestFileExists(t *testing.T) {
	if bagman.FileExists("util_test.go") == false {
		t.Errorf("FileExists returned false for util_test.go")
	}
	if bagman.FileExists("NonExistentFile.xyz") == true {
		t.Errorf("FileExists returned true for NonExistentFile.xyz")
	}
}

func TestLoadEnv(t *testing.T) {
	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	absPath := filepath.Join(bagmanHome, "testdata", "load_env_test.txt")
	vars, err := bagman.LoadEnv(absPath)
	if err != nil {
		t.Error(err)
	}
	if os.Getenv("VAR1") != "Some value" {
		t.Errorf("Env var VAR1: expected 'Some value' but got '%s'", os.Getenv("VAR1"))
	}
	if os.Getenv("VAR2") != "533" {
		t.Errorf("Env var VAR2: expected '533' but got '%s'", os.Getenv("VAR2"))
	}
	if os.Getenv("VAR3") != "Value with trailing space" {
		t.Errorf("Env var VAR3: expected 'Value with trailing space' but got '%s'", os.Getenv("VAR3"))
	}
	if vars["VAR1"] != "Some value" {
		t.Errorf("Map var VAR1: expected 'Some value' but got '%s'", vars["VAR1"])
	}
	if vars["VAR2"] != "533" {
		t.Errorf("Map var VAR2: expected '533' but got '%s'", vars["VAR2"])
	}
	if vars["VAR3"] != "Value with trailing space" {
		t.Errorf("Map var VAR3: expected 'Value with trailing space' but got '%s'", vars["VAR3"])
	}
}

func TestOwnerOf(t *testing.T) {
	if bagman.OwnerOf("aptrust.receiving.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if bagman.OwnerOf("aptrust.receiving.test.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if bagman.OwnerOf("aptrust.restore.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified restoration bucket owner")
	}
}

func TestRestorationBucketFor(t *testing.T) {
	if bagman.RestorationBucketFor("unc.edu") != "aptrust.restore.unc.edu" {
		t.Error("RestorationBucketFor returned incorrect restoration bucket name")
	}
}

func TestCleanBagName(t *testing.T) {
	expected := "some.file"
	actual, _ := bagman.CleanBagName("some.file.b001.of200.tar")
	if actual != expected {
		t.Error("CleanBagName should have returned '%s', but returned '%s'",
			expected, actual)
	}
	actual, _ = bagman.CleanBagName("some.file.b1.of2.tar")
	if actual != expected {
		t.Error("CleanBagName should have returned '%s', but returned '%s'",
			expected, actual)
	}
}

func TestMin(t *testing.T) {
	if bagman.Min(10, 12) != 10 {
		t.Error("Min() thinks 12 is less than 10")
	}
}

func TestBase64EncodeMd5(t *testing.T) {
	digest := "4d66f1ec9491addded54d17b96df8c96"
	expectedResult := "TWbx7JSRrd3tVNF7lt+Mlg=="
	encodedDigest, err := bagman.Base64EncodeMd5(digest)
	if err != nil {
		t.Error(err)
	}
	if encodedDigest != expectedResult {
		t.Errorf("Base64EncodeMd5() returned '%s'. Expected '%s'",
			encodedDigest, expectedResult)
	}
}

func TestLooksLikeURL(t *testing.T) {
	if bagman.LooksLikeURL("http://s3.amazonaws.com/bucket/key") == false {
		t.Error("That was a valid URL!")
	}
	if bagman.LooksLikeURL("https://s3.amazonaws.com/bucket/key") == false {
		t.Error("That was a valid URL!")
	}
	if bagman.LooksLikeURL("tpph\\backslash\\slackbash\\iaintnourl!") == true {
		t.Error("That was not a valid URL!")
	}
	if bagman.LooksLikeURL("") == true {
		t.Error("That was not a valid URL! That was an empty string!")
	}
}

func TestLooksLikeUUID(t *testing.T) {
	if bagman.LooksLikeUUID("1552abf5-28f3-46a5-ba63-95302d08e209") == false {
		t.Error("That was a valid UUID!")
	}
	if bagman.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f607ebdcca3") == false {
		t.Error("That was a valid UUID!")
	}
	if bagman.LooksLikeUUID("88198C5A-EC91-4CE1-BFCC-0F607EBDCCA3") == false {
		t.Error("That was a valid UUID!")
	}
	if bagman.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f607ebdccx3") == true {
		t.Error("That was not a valid UUID!")
	}
	if bagman.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f6c") == true {
		t.Error("That was not a valid UUID!")
	}
	if bagman.LooksLikeUUID("") == true {
		t.Error("That was not a valid UUID! That was an empty string!")
	}
}


func TestExpandTilde(t *testing.T) {
	expanded, err := bagman.ExpandTilde("~/tmp")
	if err != nil {
		t.Error(err)
	}
	// Testing this cross-platform is pain. Different home dirs
	// on Windows, Linux, Mac. Different separators ("/" vs "\").
	if len(expanded) <= 5 || !strings.HasSuffix(expanded, "tmp") {
		t.Errorf("~/tmp expanded to unexpected value %s", expanded)
	}

	expanded, err = bagman.ExpandTilde("/nothing/to/expand")
	if err != nil {
		t.Error(err)
	}
	if expanded != "/nothing/to/expand" {
		t.Errorf("/nothing/to/expand expanded to unexpected value %s", expanded)
	}
}

func TestCleanString(t *testing.T) {
	clean := bagman.CleanString("  spaces \t\n ")
	if clean != "spaces" {
		t.Error("Expected to receive string 'spaces'")
	}
	clean = bagman.CleanString("  ' embedded spaces 1 '   ")
	if clean != " embedded spaces 1 " {
		t.Error("Expected to receive string ' embedded spaces 1 '")
	}
	clean = bagman.CleanString("  \" embedded spaces 2 \"   ")
	if clean != " embedded spaces 2 " {
		t.Error("Expected to receive string ' embedded spaces '")
	}
}

func TestBucketNameAndKey(t *testing.T) {
	url := "https://s3.amazonaws.com/aptrust.test.restore/ncsu.1840.16-1004.tar"
	expectedBucket := "aptrust.test.restore"
	expectedKey := "ncsu.1840.16-1004.tar"
	bucketName, key := bagman.BucketNameAndKey(url)
	if bucketName != expectedBucket {
		t.Errorf("Expected bucket name %s, got %s", expectedBucket, bucketName)
	}
	if key != expectedKey {
		t.Errorf("Expected key %s, got %s", expectedKey, key)
	}
}

func TestAddToArchive(t *testing.T) {
	tarFile, err := ioutil.TempFile("", "util_test.tar")
	if err != nil {
		t.Errorf("Error creating temp file for tar archive: %v", err)
	}
	defer os.Remove(tarFile.Name())
	tarWriter := tar.NewWriter(tarFile)
	bagmanHome, _ := bagman.BagmanHome()
	testfilePath := filepath.Join(bagmanHome, "testdata")
	files, _ := filepath.Glob(filepath.Join(testfilePath, "*.json"))
	for _, filePath := range files {
		pathWithinArchive := fmt.Sprintf("data/%s", filePath)
		err = bagman.AddToArchive(tarWriter, filePath, pathWithinArchive)
		if err != nil {
			t.Errorf("Error adding %s to tar file: %v", filePath, err)
		}
	}
}

func getPath(filename string) (string) {
	bagmanHome, _ := bagman.BagmanHome()
	return filepath.Join(bagmanHome, filename)
}

func TestRecursiveFileList(t *testing.T) {
	bagmanHome, _ := bagman.BagmanHome()
	files, err := bagman.RecursiveFileList(bagmanHome)
	if err != nil {
		t.Errorf("RecursiveFileList() returned error: %v", err)
	}
	// Make a map for quick lookup & check for a handful
	// of files at different levels.
	fileMap := make(map[string]string, 0)
	for _, f := range files {
		fileMap[f] = f
	}
	sampleFiles := []string{
		getPath("README.md"),
		getPath("apps/apt_fixity/apt_fixity.go"),
		getPath("bagman/bucketsummary.go"),
		getPath("config/config.json"),
		getPath("partner-apps/apt_upload/apt_upload.go"),
		getPath("testdata/intel_obj.json"),
		getPath("workers/fixitychecker.go"),
		getPath("testdata/example.edu.sample_good/data/datastream-DC"),
	}
	for _, filePath := range sampleFiles {
		_, present := fileMap[filePath]
		if present == false {
			t.Errorf("File '%s' is missing from recursive file list", filePath)
		}
	}
}

func TestCalculateDigests(t *testing.T) {
	bagmanHome, _ := bagman.BagmanHome()
	absPath := filepath.Join(bagmanHome, "testdata", "result_good.json")
	fileDigest, err := bagman.CalculateDigests(absPath)
	if err != nil {
		t.Errorf("CalculateDigests returned unexpected error: %v", err)
	}
	expectedMd5 := "9cd263b67bad7ae264fda8987fd221e7"
	if fileDigest.Md5Digest != expectedMd5 {
		t.Errorf("Expected digest '%s', got '%s'", expectedMd5, fileDigest.Md5Digest)
	}
	expectedSha := "3c04086d429b4dcba91891dad54759a465869d381f180908203a73b9e3120a87"
	if fileDigest.Sha256Digest != expectedSha {
		t.Errorf("Expected digest '%s', got '%s'", expectedSha, fileDigest.Sha256Digest)
	}
	if fileDigest.Size != 7718 {
		t.Errorf("Expected file size 7718, got %d", fileDigest.Size)
	}
}

func TestGetInstitutionFromBagName(t *testing.T) {
	inst, err := bagman.GetInstitutionFromBagName("chc0390_metadata")
	if err == nil {
		t.Error("GetInstitutionFromBagName accepted invalid bag name 'chc0390_metadata'")
	}
	inst, err = bagman.GetInstitutionFromBagName("chc0390_metadata.tar")
	if err == nil {
		t.Error("GetInstitutionFromBagName accepted invalid bag name 'chc0390_metadata.tar'")
	}
	inst, err = bagman.GetInstitutionFromBagName("miami.chc0390_metadata.tar")
	if err != nil {
		t.Error(err)
	}
	if inst != "miami" {
		t.Error("GetInstitutionFromBagName return institution name '%s', expected 'miami'", inst)
	}
	inst, err = bagman.GetInstitutionFromBagName("miami.edu.chc0390_metadata.tar")
	if err != nil {
		t.Error("GetInstitutionFromBagName should have accepted bag name 'miami.edu.chc0390_metadata.tar'")
	}
}

func TestGetInstitutionFromBagIdentifier(t *testing.T) {
	inst, err := bagman.GetInstitutionFromBagIdentifier("miami.chc0390_metadata.tar")
	if err == nil {
		t.Error("GetInstitutionFromBagIdentifier accepted invalid bag name 'miami.edu.chc0390_metadata.tar'")
	}
	inst, err = bagman.GetInstitutionFromBagIdentifier("miami.edu/miami.edu.chc0390_metadata.tar")
	if err != nil {
		t.Error("GetInstitutionFromBagIdentifier should have accepted bag name 'miama.edu/miami.edu.chc0390_metadata.tar'")
	}
	if inst != "miami.edu" {
		t.Error("GetInstitutionFromBagIdentifier returned '%s', expected 'miami.edu'",
			inst)
	}
}

func TestSavableName(t *testing.T) {
	if bagman.HasSavableName(".") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("..") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("._junk.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("data/subdir/._junk.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("bagit.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("manifest-md5.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("manifest-sha256.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("tagmanifest-md5.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}
	if bagman.HasSavableName("tagmanifest-sha256.txt") == true {
		t.Errorf("HasSavableName() should have returned false")
	}

	if bagman.HasSavableName("data/stuff/bagit.txt") == false {
		t.Errorf("HasSavableName() should have returned true")
	}
	if bagman.HasSavableName("custom_tags/manifest-md5.txt") == false {
		t.Errorf("HasSavableName() should have returned true")
	}
	if bagman.HasSavableName("custom_tags/manifest-sha256.txt") == false {
		t.Errorf("HasSavableName() should have returned true")
	}
	if bagman.HasSavableName("useless_tags/tagmanifest-md5.txt") == false {
		t.Errorf("HasSavableName() should have returned true")
	}
	if bagman.HasSavableName("my_tags/tagmanifest-sha256.txt") == false {
		t.Errorf("HasSavableName() should have returned true")
	}
}

func TestIsValidFileName(t *testing.T) {
	if !bagman.IsValidFileName("data/this/is/just/great.txt") {
		t.Errorf("Name should be valid")
	}
	if bagman.IsValidFileName("data/this/is/-just/great.txt") {
		t.Errorf("Name should NOT be valid")
	}
	if bagman.IsValidFileName("data/this/is/just/great.txt!") {
		t.Errorf("Name should NOT be valid")
	}
	if bagman.IsValidFileName("data/th@s/is/just/great.txt") {
		t.Errorf("Name should NOT be valid")
	}
}

func TestNamePartIsValid(t *testing.T) {
	if !bagman.NamePartIsValid("great.txt") {
		t.Errorf("Name should be valid")
	}
	if !bagman.NamePartIsValid("great-one.txt") {
		t.Errorf("Name should be valid")
	}
	if !bagman.NamePartIsValid("great_one.txt") {
		t.Errorf("Name should be valid")
	}
	if bagman.NamePartIsValid("gre*t.txt") {
		t.Errorf("Name should NOT be valid")
	}
	if bagman.NamePartIsValid("gre&t.txt") {
		t.Errorf("Name should NOT be valid")
	}
	if bagman.NamePartIsValid("gre%t.txt") {
		t.Errorf("Name should NOT be valid")
	}
	if bagman.NamePartIsValid("gre:t.txt") {
		t.Errorf("Name should NOT be valid")
	}
	// Leading dash not allowed.
	if bagman.NamePartIsValid("-great.txt") {
		t.Errorf("Name should NOT be valid")
	}
}
