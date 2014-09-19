package bagman_test

import (
	"archive/tar"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/crowdmob/goamz/aws"
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

func TestRestorationBucketName (t *testing.T) {
	restorer, _, err := restoreBag(false)
	if err != nil {
		t.Error(err)
		return
	}
	if restorer.RestorationBucketName() != "aptrust.restore.uc.edu" {
		t.Errorf("RestorationBucketName() expected " +
			"'aptrust.restore.uc.edu', got '%s'",
			restorer.RestorationBucketName())
	}
	restorer.SetCustomRestoreBucket("bucket-o-worms")
	if restorer.RestorationBucketName() != "bucket-o-worms" {
		t.Errorf("RestorationBucketName() expected " +
			"'bucket-o-worms', got '%s'",
			restorer.RestorationBucketName())
	}
}

func TestCopyToS3 (t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	expectedSizes := []int64 { int64(5120), int64(5632) }

	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Error(err)
		return
	}

	defer cleanupRestorationBucket(s3Client)
	outputDir := filepath.Join("testdata", "tmp")
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	restorer, bagPaths, err := restoreBag(true)
	if err != nil {
		t.Error(err)
		return
	}
	restorer.SetCustomRestoreBucket("aptrust.test.restore")
	for i := range bagPaths {
		_, err = restorer.TarBag(i)
		if err != nil {
			t.Error(err)
			return
		}
		s3Url, err := restorer.CopyToS3(i)
		if err != nil {
			t.Error(err)
			return
		}

		bagName := filepath.Base(bagPaths[i]) + ".tar"
		expectedUrl := fmt.Sprintf("https://s3.amazonaws.com/aptrust.test.restore/%s",
			bagName)
		if s3Url != expectedUrl {
			t.Errorf("CopyToS3() returned incorrect URL: %s", s3Url)
		}

		key, err := s3Client.GetKey("aptrust.test.restore", bagName)
		if key == nil {
			t.Errorf("Bag %s was not uploaded to S3", bagName)
		}
		if key.Size != expectedSizes[i] {
			t.Errorf("Size for bag %s is incorrect. Expected %s, got %s.",
				bagName, expectedSizes[i], key.Size)
		}
	}
}


func TestRestoreAndPublish (t *testing.T) {
	if !awsEnvAvailable() {
		printSkipMessage("restore_test.go")
		return
	}

	// Set up an S3Client, so we can see if our bags made it to S3.
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		t.Error(err)
		return
	}

	defer cleanupRestorationBucket(s3Client)

	// Load the test fixture.
	testfile := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
	}

	// Create a BagRestorer that will send files to aptrust.test.restore
	outputDir := filepath.Join("testdata", "tmp")
	restorer, err := bagman.NewBagRestorer(obj, outputDir)
	defer os.RemoveAll(filepath.Join(outputDir, "uc.edu"))

	// Restore to this bucket
	restorer.SetCustomRestoreBucket("aptrust.test.restore")

	// Set small bag size limit to force creation of two bags
	restorer.SetBagSizeLimit(50)
	restorer.SetBagPadding(0)


	// Tell it restore, tar, copy to S3 and clean up after itself.
	urls, err := restorer.RestoreAndPublish()
	if err != nil {
		t.Error(err)
		return
	}

	// Make sure we got the right URLs
	if urls[0] != "https://s3.amazonaws.com/aptrust.test.restore/cin.675812.b0001.of0002.tar" {
		t.Errorf("RestoreAndPublish() returned incorrect URL: %s", urls[0])
	}
	if urls[1] != "https://s3.amazonaws.com/aptrust.test.restore/cin.675812.b0002.of0002.tar" {
		t.Errorf("RestoreAndPublish() returned incorrect URL: %s", urls[1])
	}

	// Make sure the files are on S3
	key, err := s3Client.GetKey("aptrust.test.restore", "cin.675812.b0001.of0002.tar")
	if key == nil {
		t.Errorf("Bag cin.675812.b0001.of0002.tar was not uploaded to S3")
	}
	key, err = s3Client.GetKey("aptrust.test.restore", "cin.675812.b0002.of0002.tar")
	if key == nil {
		t.Errorf("Bag cin.675812.b0002.of0002.tar was not uploaded to S3")
	}

	// Make sure it cleaned up the local files
	_, err = os.Stat(filepath.Join(outputDir, "uc.edu", "cin.675812.b0001.of0002.tar"))
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up cin.675812.b0001.of0002.tar")
	}
	_, err = os.Stat(filepath.Join(outputDir, "uc.edu", "cin.675812.b0002.of0002.tar"))
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Bag restorer did not clean up cin.675812.b0002.of0002.tar")
	}


}


func cleanupRestorationBucket (s3Client *bagman.S3Client) {
	s3Client.Delete("aptrust.test.restore", "cin.675812.b0001.of0002.tar")
	s3Client.Delete("aptrust.test.restore", "cin.675812.b0002.of0002.tar")
}
