package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
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
