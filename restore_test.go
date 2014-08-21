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

	expectedAPT := "Title:  Notes from the Oesper Collections\nAccess:  institution\n"
	verifyFileContent(t, bagPaths[0], "aptrust-info.txt", expectedAPT)

	expectedBagit := "BagIt-Version:  0.97\nTag-File-Character-Encoding:  UTF-8\n"
	verifyFileContent(t, bagPaths[0], "bagit.txt", expectedBagit)

	expectedManifest := "8d7b0e3a24fc899b1d92a73537401805 data/object.properties\nc6d8080a39a0622f299750e13aa9c200 data/metadata.xml\n"
	verifyFileContent(t, bagPaths[0], "manifest-md5.txt", expectedManifest)
}


func verifyFileContent(t *testing.T, bagPath, fileName, expectedContent string) {
	filePath := filepath.Join(bagPath, fileName)
	actualContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Error("Could not read file %s: %v", fileName, err)
	}
	if string(actualContent) != expectedContent {
		t.Errorf("%s contains the wrong data. Expected \n%s\nGot \n%s\n",
			fileName, expectedContent, string(actualContent))
	}
}
