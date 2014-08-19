package models_test

import (
	"encoding/json"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
	"path/filepath"
	"testing"
)

func assertValue(t *testing.T, data map[string]interface{}, key, expected string) {
	if data[key] != expected {
		t.Errorf("For key '%s', expected '%s' but found '%s'", key, expected, data[key])
	}
}

func TestSerializeForCreate(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}
	jsonBytes, err := obj.SerializeForCreate()
	if err != nil {
		t.Error(err)
		return
	}

	// Translate the JSON back into a go map so we can test it.
	data := make([]map[string]interface{}, 1)
	err = json.Unmarshal(jsonBytes, &data)
	if err != nil {
		t.Error(err)
		return
	}

	// Intellectual object
	assertValue(t, data[0], "access", "consortia")
	assertValue(t, data[0], "description", "Description of intellectual object.")
	assertValue(t, data[0], "identifier", "ncsu.edu/ncsu.1840.16-2928")
	assertValue(t, data[0], "institution_id", "ncsu.edu")

	// Intellectual object events
	objEvents := data[0]["premisEvents"].([]interface{})
	firstEvent := objEvents[0].(map[string]interface{})
	secondEvent := objEvents[1].(map[string]interface{})
	assertValue(t, firstEvent, "type", "identifier_assignment")
	assertValue(t, firstEvent, "outcome", "Success")
	assertValue(t, secondEvent, "type", "ingest")
	assertValue(t, secondEvent, "outcome", "Success")
	assertValue(t, secondEvent, "outcome_detail", "4 files copied")
	if len(objEvents) != 2 {
		t.Errorf("Expected 2 object events but found %d", len(objEvents))
	}

	// Generic files
	files := data[0]["generic_files"].([]interface{})
	file1 := files[0].(map[string]interface{})
	assertValue(t, file1, "created", "2014-04-25T18:05:51Z")
	assertValue(t, file1, "format", "application/xml")
	assertValue(t, file1, "identifier", "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml")
	assertValue(t, file1, "modified", "2014-04-25T18:05:51Z")
	assertValue(t, file1, "uri", "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml")
	if file1["size"] != float64(5105) {
		t.Errorf("Expected file size 5105, got %f", file1["size"])
	}
	if len(files) != 4 {
		t.Errorf("Expected 4 generic files, found %d", len(files))
	}

	// Generic file checksums
	checksums := file1["checksum"].([]interface{})
	checksum2 := checksums[1].(map[string]interface{})
	assertValue(t, checksum2, "algorithm", "sha256")
	assertValue(t, checksum2, "datetime", "2014-06-09T14:12:45.574358959Z")
	assertValue(t, checksum2, "digest", "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784")
	if len(checksums) != 2 {
		t.Errorf("Expected 2 checksums but found %d", len(checksums))
	}

	// Generic file events
	events := file1["premisEvents"].([]interface{})
	event1 := events[0].(map[string]interface{})
	event2 := events[1].(map[string]interface{})
	event3 := events[2].(map[string]interface{})
	event4 := events[3].(map[string]interface{})
	event5 := events[4].(map[string]interface{})

	assertValue(t, event1, "type", "fixity_check")
	assertValue(t, event1, "outcome_detail", "md5:84586caa94ff719e93b802720501fcc7")

	assertValue(t, event2, "type", "ingest")
	assertValue(t, event2, "outcome_detail", "84586caa94ff719e93b802720501fcc7")

	assertValue(t, event3, "type", "fixity_generation")
	assertValue(t, event3, "outcome_detail", "sha256:ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784")

	assertValue(t, event4, "type", "identifier_assignment")
	assertValue(t, event4, "outcome_detail", "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml")

	assertValue(t, event5, "type", "identifier_assignment")
	assertValue(t, event5, "outcome_detail", "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml")

	if len(events) != 5 {
		t.Errorf("Expected 5 file events but found %d", len(events))
	}

}

func TestGFBagName(t *testing.T) {
	gf := models.GenericFile{}
	gf.Identifier = "uc.edu/cin.675812/data/object.properties"
	bagname, err := gf.BagName()
	if err != nil {
		t.Error(err)
	}
	if bagname != "cin.675812" {
		t.Errorf("BagName returned '%s'; expected 'cin.675812'", bagname)
	}
}

func TestGFInstitutionId(t *testing.T) {
	gf := models.GenericFile{}
	gf.Identifier = "uc.edu/cin.675812/data/object.properties"
	instId, err := gf.InstitutionId()
	if err != nil {
		t.Errorf(err.Error())
	}
	if instId != "uc.edu" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu'", instId)
	}
}

func TestGFOriginalPath(t *testing.T) {
	gf := models.GenericFile{}
	gf.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath, err := gf.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "data/object.properties" {
		t.Errorf("OriginalPath returned some kinda shizzle. Expected 'data/object.properties', got '%s'",
			origPath)
	}
}


func TestGFGetChecksum(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := bagman.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	if intelObj == nil {
		return
	}
	gf := intelObj.GenericFiles[1]

	// MD5
	md5Checksum := gf.GetChecksum("md5")
	if md5Checksum == nil {
		t.Errorf("GetChecksum did not return md5 sum")
	}
	if md5Checksum.Digest != "c6d8080a39a0622f299750e13aa9c200" {
		t.Errorf("GetChecksum did not return md5 sum")
	}

	// SHA256
	sha256Checksum := gf.GetChecksum("sha256")
	if sha256Checksum == nil {
		t.Errorf("GetChecksum did not return sha256 sum")
	}
	if sha256Checksum.Digest != "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790" {
		t.Errorf("GetChecksum did not return sha256 sum")
	}

	// bogus checksum
	bogusChecksum := gf.GetChecksum("bogus")
	if bogusChecksum != nil {
		t.Errorf("GetChecksum returned something it shouldn't have")
	}
}
