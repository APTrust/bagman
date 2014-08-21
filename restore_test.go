package bagman_test

import (
	"github.com/APTrust/bagman"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestRestore(t *testing.T) {
	// TODO: Don't run this test unless we have S3 credentials.
	// TODO: Fix other test file where we clobber filepath.
	testfile := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return
	}
	outputDir := filepath.Join("testdata", "tmp")
	restorer, err := bagman.NewBagRestorer(obj, outputDir)
	if err != nil {
		t.Errorf("NewBagRestorer() returned an error: %v", err)
		return
	}
	bagPaths, err := restorer.Restore()
	if err != nil {
		t.Errorf("Restore() returned an error: %v", err)
	}

	verifyAPTInfoFile(t, bagPaths[0])
//	expectedBagit = "BagIt-Version:  0.97\nTag-File-Character-Encoding:  UTF-8"

//	expectedManifest = "8d7b0e3a24fc899b1d92a73537401805 data/object.properties\nc6d8080a39a0622f299750e13aa9c200 data/metadata.xml"

}

func verifyAPTInfoFile(t *testing.T, bagPath string) {
	aptInfoPath := filepath.Join(bagPath, "aptrust-info.txt")
	actualAPTInfo, err := ioutil.ReadFile(aptInfoPath)
	if err != nil {
		t.Error("Could not read aptrust-info.txt: %v", err)
	}
	expectedAPTInfo := "Title:  Notes from the Oesper Collections\nAccess:  institution\n"
	if string(actualAPTInfo) != expectedAPTInfo {
		t.Errorf("aptrust-info.txt contains the wrong data. Expected \n%s\nGot \n%s\n",
			expectedAPTInfo, string(actualAPTInfo))
	}
}
