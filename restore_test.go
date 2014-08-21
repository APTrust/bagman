package bagman_test

import (
	"archive/tar"
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
	// TODO: Fix other test file where we clobber filepath.
	// awsEnvAvailable and printSkipMessage are from S3_test.go
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	// Make sure we clean up after ourselves
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	_, bagPaths, err := restoreBag(false)
	if err != nil {
		t.Error(err)
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

func restoreBag(multipart bool) (*bagman.BagRestorer, []string, error){
	testfile := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(testfile)
	if err != nil {
		detailedErr := fmt.Errorf("Error loading test data file '%s': %v", testfile, err)
		return nil, nil, detailedErr
	}

	outputDir := filepath.Join("testdata", "tmp")
	restorer, err := bagman.NewBagRestorer(obj, outputDir)
	if err != nil {
		detailedErr := fmt.Errorf("NewBagRestorer() returned an error: %v", err)
		return nil, nil, detailedErr
	}

	if multipart == true {
		// Set the bag size to something very small,
		// so the restorer will be forced to restore
		// the object as more than one bag.
		restorer.SetBagSizeLimit(50) // 50 bytes
		restorer.SetBagPadding(0)    //  0 bytes
	}

	bagPaths, err := restorer.Restore()
	if err != nil {
		detailedErr := fmt.Errorf("Restore() returned an error: %v", err)
		return nil, nil, detailedErr
	}
	return restorer, bagPaths, nil
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

func TestCleanup(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	// Make sure we clean up after ourselves
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	restorer, bagPaths, err := restoreBag(false)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = os.Stat(bagPaths[0])
	if err != nil && os.IsNotExist(err) {
		t.Errorf("Bag restorer did not created the expected bag at %s", bagPaths[0])
	}

	restorer.Cleanup()
	_, err = os.Stat(bagPaths[0])
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up the bag at %s", bagPaths[0])
	}

}


func TestRestoreMultipart(t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	// Make sure we clean up after ourselves
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	restorer, bagPaths, err := restoreBag(true)
	if err != nil {
		t.Error(err)
		return
	}

	if len(bagPaths) != 2 {
		t.Errorf("Restore() should have produced 2 bags but produced %d", len(bagPaths))
		return
	}

	// Check existence of both bags before calling cleanup.
	// First bag should have just the object.properties file.
	_, err = os.Stat(bagPaths[0])
	if err != nil && os.IsNotExist(err) {
		t.Errorf("Bag restorer did not created the expected bag at %s", bagPaths[0])
	}
	fileName := filepath.Join(bagPaths[0], "data", "object.properties")
	_, err = os.Stat(fileName)
	if err != nil && os.IsNotExist(err) {
		t.Errorf("Bag restorer did not created the expected file at %s", fileName)
	}
	// Make sure manifest-md5.txt is correct
	expectedManifest := "8d7b0e3a24fc899b1d92a73537401805 data/object.properties\n"
	verifyFileContent(t, bagPaths[0], "manifest-md5.txt", expectedManifest)


	// Second bag should have just the metadata.xml file
	_, err = os.Stat(bagPaths[1])
	if err != nil && os.IsNotExist(err) {
		t.Errorf("Bag restorer did not created the expected bag at %s", bagPaths[1])
	}
	fileName = filepath.Join(bagPaths[1], "data", "metadata.xml")
	_, err = os.Stat(fileName)
	if err != nil && os.IsNotExist(err) {
		t.Errorf("Bag restorer did not created the expected file at %s", fileName)
	}
	// Make sure manifest-md5.txt is correct
	expectedManifest = "c6d8080a39a0622f299750e13aa9c200 data/metadata.xml\n"
	verifyFileContent(t, bagPaths[1], "manifest-md5.txt", expectedManifest)


	// Make sure Cleanup() cleans up both bags
	restorer.Cleanup()
	_, err = os.Stat(bagPaths[0])
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up the bag at %s", bagPaths[0])
	}
	_, err = os.Stat(bagPaths[1])
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up the bag at %s", bagPaths[0])
	}

}

func TestTarBag (t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	// Make sure we clean up after ourselves
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	restorer, bagPaths, err := restoreBag(true)
	if err != nil {
		t.Error(err)
		return
	}

	tarFilePaths := make([]string, 2)
	for i := range bagPaths {
		tarFilePath, err := restorer.TarBag(i)
		if err != nil {
			t.Error(err)
			return
		}
		verifyTarFile(t, i, tarFilePath)
		tarFilePaths[i] = tarFilePath
	}

	// Make sure cleanup gets all the tar files
	restorer.Cleanup()
	_, err = os.Stat(tarFilePaths[0])
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up the tar file at %s", bagPaths[0])
	}
	_, err = os.Stat(tarFilePaths[1])
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up the tar file at %s", bagPaths[0])
	}

}

func verifyTarFile(t *testing.T, bagNumber int, tarFilePath string) {
	_, err := os.Stat(tarFilePath)
	if err != nil {
		t.Errorf("Tar file does not exist at %s", tarFilePath)
	}
	file, err := os.Open(tarFilePath)
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	tarReader := tar.NewReader(file)

	files := make([]string, 0)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("Error reading tar archive header: %v", err)
		}
		files = append(files, header.Name)

		// Verify contents
		buffer := make([]byte, 1000)
		tarReader.Read(buffer)

		actualFileLength := bufferCharLength(buffer)
		if int64(actualFileLength) != header.Size {
			t.Errorf("Tar archive file '%s' has %d bytes. It should have %d.",
				header.Name, actualFileLength, header.Size)
			return
		}
	}

	verifyFilePresence(t, "bagit.txt", files)
	verifyFilePresence(t, "aptrust-info.txt", files)

	if bagNumber == 0 {
		verifyFilePresence(t, "data/object.properties", files)
	} else {
		verifyFilePresence(t, "data/metadata.xml", files)
	}
}

// Verifies a file is in the tar header
func verifyFilePresence(t *testing.T, fileName string, fileList []string) {
	if !contains(fileName, fileList) {
		t.Errorf("%s is missing from tar archive", fileName)
	}
}

func bufferCharLength (buffer []byte) (int) {
	for i, val := range(buffer) {
		if val == 0 {
			return i
		}
	}
	return len(buffer)
}

func contains(str string, list []string) bool {
    for _, value := range list {
        if value == str {
            return true
        }
    }
    return false
}
