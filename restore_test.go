package bagman_test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/APTrust/bagman"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestRestore(t *testing.T) {
	// TODO: Don't run this test unless we have S3 credentials.
	// TODO: Fix other test file where we clobber filepath.

	// Make sure we clean up after ourselves
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	testfile := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return
	}

	restorer, err := bagman.NewBagRestorer(obj, outputDir)
	if err != nil {
		t.Errorf("NewBagRestorer() returned an error: %v", err)
		return
	}

	bagPaths, err := restorer.Restore()
	if err != nil {
		t.Errorf("Restore() returned an error: %v", err)
		return
	}

	// Make sure aptrust-info.txt is correct
	expectedAPT := "Title:  Notes from the Oesper Collections\nAccess:  institution\n"
	verifyFileContent(t, bagPaths[0], "aptrust-info.txt", expectedAPT)

	// Make sure bagit.txt is correct
	expectedBagit := "BagIt-Version:  0.97\nTag-File-Character-Encoding:  UTF-8\n"
	verifyFileContent(t, bagPaths[0], "bagit.txt", expectedBagit)

	// Make sure manifest-md5.txt is correct
	expectedManifest := "8d7b0e3a24fc899b1d92a73537401805 data/object.properties\nc6d8080a39a0622f299750e13aa9c200 data/metadata.xml\n"
	verifyFileContent(t, bagPaths[0], "manifest-md5.txt", expectedManifest)

	// Make sure data dir contains the right number of files
	dataDir := filepath.Join(bagPaths[0], "data")
	files, err := ioutil.ReadDir(dataDir)
	if len(files) != 2 {
		t.Errorf("Data dir has %d files, but should have 2", len(files))
	}

	// Make sure first data file was written correctly
	checksum1, err := md5Digest(filepath.Join(dataDir, "metadata.xml"))
	if checksum1 != "c6d8080a39a0622f299750e13aa9c200" {
		t.Error("Checksum for metadata.xml is incorrect")
	}

	// Make sure second data file was written correctly
	checksum2, err := md5Digest(filepath.Join(dataDir, "object.properties"))
	if checksum2 != "8d7b0e3a24fc899b1d92a73537401805" {
		t.Error("Checksum for metadata.xml is incorrect")
	}
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

func md5Digest (filePath string) (string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("Could not read file %s: %v", filePath, err)
	}
    hash := md5.New()
    io.WriteString(hash, string(data))
    return hex.EncodeToString(hash.Sum(nil)), nil
}
