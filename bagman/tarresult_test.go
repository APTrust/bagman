package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"testing"
	"time"
)

func buildFluctusFiles() ([]*bagman.FluctusFile) {
	// Changed file
	md5_1 := &bagman.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "TestMd5Digest",
	}
	sha256_1 := &bagman.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "TestSha256Digest",
	}
	checksums1 := make([]*bagman.ChecksumAttribute, 2)
	checksums1[0] = md5_1
	checksums1[1] = sha256_1
	fluctusFile1 := &bagman.FluctusFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml",
		ChecksumAttributes: checksums1,
	}

	// Existing file, unchanged
	md5_2 := &bagman.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "a340203a24dcd6f6ca2bc95a4956c65d",
	}
	sha256_2 := &bagman.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "54536211e3ad308e8509091a1db393cbcc7fadd4a9b7f434bec8097d149a2039",
	}
	checksums2 := make([]*bagman.ChecksumAttribute, 2)
	checksums2[0] = md5_2
	checksums2[1] = sha256_2
	fluctusFile2 := &bagman.FluctusFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/object.properties",
		ChecksumAttributes: checksums2,
	}

	fluctusFiles := make([]*bagman.FluctusFile, 2)
	fluctusFiles[0] = fluctusFile1
	fluctusFiles[1] = fluctusFile2
	return fluctusFiles
}


func TestAnyFilesNeedSaving(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if result.TarResult.AnyFilesNeedSaving() == false {
		t.Errorf("AnyFilesNeedSaving should have returned true.")
	}
	for i := range result.TarResult.GenericFiles {
		result.TarResult.GenericFiles[i].NeedsSave = false
	}
	if result.TarResult.AnyFilesNeedSaving() == true {
		t.Errorf("AnyFilesNeedSaving should have returned false.")
	}
}

func TestGenericFilePaths(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	filepaths := result.TarResult.GenericFilePaths()
	if len(filepaths) == 0 {
		t.Error("TarResult.GenericFilePaths returned no file paths")
		return
	}
	for i, path := range filepaths {
		if path != expectedPaths[i] {
			t.Errorf("Expected filepath '%s', got '%s'", expectedPaths[i], path)
		}
	}
}

func TestGetFileByPath(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	gf := result.TarResult.GetFileByPath("data/ORIGINAL/1")
	if gf == nil {
		t.Errorf("GetFileByPath() did not return expected file")
	}
	if gf.Path != "data/ORIGINAL/1" {
		t.Errorf("GetFileByPath() returned the wrong file")
	}
	gf2 := result.TarResult.GetFileByPath("file/does/not/exist")
	if gf2 != nil {
		t.Errorf("GetFileByPath() returned a file when it shouldn't have")
	}
}

func TestAnyFilesCopiedToPreservation(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if result.TarResult.AnyFilesCopiedToPreservation() == false {
		t.Error("AnyFilesCopiedToPreservation should have returned true")
	}
	result.TarResult.GenericFiles[0].StorageURL = ""
	if result.TarResult.AnyFilesCopiedToPreservation() == false {
		t.Error("AnyFilesCopiedToPreservation should have returned true")
	}
	for i := range result.TarResult.GenericFiles {
		result.TarResult.GenericFiles[i].StorageURL = ""
	}
	if result.TarResult.AnyFilesCopiedToPreservation() == true {
		t.Error("AnyFilesCopiedToPreservation should have returned false")
	}
}

func TestAllFilesCopiedToPreservation(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if result.TarResult.AllFilesCopiedToPreservation() == false {
		t.Error("AllFilesCopiedToPreservation should have returned true")
	}
	result.TarResult.GenericFiles[0].StorageURL = ""
	if result.TarResult.AllFilesCopiedToPreservation() == true {
		t.Error("AllFilesCopiedToPreservation should have returned false")
	}
}

func TestMergeExistingFiles(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	fluctusFiles := buildFluctusFiles()
	result.TarResult.MergeExistingFiles(fluctusFiles)

	// Existing and changed.
	// File "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml"
	gf := result.TarResult.GenericFiles[0]
	if gf.ExistingFile == false {
		t.Errorf("GenericFile should have been marked as an existing file")
	}
	if gf.NeedsSave == false {
		t.Errorf("GenericFile should have been marked as needing to be saved")
	}

	// Existing but unchanged.
	// File "ncsu.edu/ncsu.1840.16-2928/data/object.properties"
	gf = result.TarResult.GenericFiles[1]
	if gf.ExistingFile == false {
		t.Errorf("GenericFile should have been marked as an existing file")
	}
	if gf.NeedsSave == true {
		t.Errorf("GenericFile should have been marked as NOT needing to be saved")
	}

	// New file "data/ORIGINAL/1"
	gf = result.TarResult.GenericFiles[2]
	if gf.ExistingFile == true {
		t.Errorf("GenericFile NOT should have been marked as an existing file")
	}
	if gf.NeedsSave == false {
		t.Errorf("GenericFile should have been marked as needing to be saved")
	}

	// New file "data/ORIGINAL/1-metadata.xml"
	gf = result.TarResult.GenericFiles[3]
	if gf.ExistingFile == true {
		t.Errorf("GenericFile NOT should have been marked as an existing file")
	}
	if gf.NeedsSave == false {
		t.Errorf("GenericFile should have been marked as needing to be saved")
	}

}
