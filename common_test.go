package bagman_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/crowdmob/goamz/s3"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Empty timestamp
var emptyTime time.Time = time.Time{}

// Our test fixture describes a bag that includes the following file paths
var expectedPaths [4]string = [4]string{
	"data/metadata.xml",
	"data/object.properties",
	"data/ORIGINAL/1",
	"data/ORIGINAL/1-metadata.xml",
}

func TestOwnerOf(t *testing.T) {
	if bagman.OwnerOf("aptrust.receiving.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if bagman.OwnerOf("aptrust.restore.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified restoration bucket owner")
	}
}

func TestReceivingBucketFor(t *testing.T) {
	if bagman.ReceivingBucketFor("unc.edu") != "aptrust.receiving.unc.edu" {
		t.Error("ReceivingBucketFor returned incorrect receiving bucket name")
	}
}

func TestRestorationBucketFor(t *testing.T) {
	if bagman.RestorationBucketFor("unc.edu") != "aptrust.restore.unc.edu" {
		t.Error("RestorationBucketFor returned incorrect restoration bucket name")
	}
}

func TestTagValue(t *testing.T) {
	result := &bagman.BagReadResult{}
	result.Tags = make([]bagman.Tag, 2)
	result.Tags[0] = bagman.Tag{Label: "Label One", Value: "Value One"}
	result.Tags[1] = bagman.Tag{Label: "Label Two", Value: "Value Two"}

	if result.TagValue("LABEL ONE") != "Value One" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("Label Two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("label two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("Non-existent label") != "" {
		t.Error("TagValue returned wrong result.")
	}
}

// Returns a basic ProcessResult that can be altered for
// specific tests.
func baseResult() (result *bagman.ProcessResult) {
	result = &bagman.ProcessResult{}
	result.S3File = &bagman.S3File{}
	result.S3File.BucketName = "aptrust.receiving.unc.edu"
	result.S3File.Key = s3.Key{}
	result.S3File.Key.Key = "sample.tar"
	result.S3File.Key.ETag = "\"0123456789ABCDEF\""
	result.S3File.Key.LastModified = "2014-05-28T16:22:24.016Z"
	return result
}

// Returns a result with Stage set to stage. If successful is false,
// the result will include an error message.
func getResult(stage bagman.StageType, successful bool) (result *bagman.ProcessResult) {
	result = baseResult()
	if successful == false {
		result.ErrorMessage = fmt.Sprintf("Sample error message. Sumpin went rawng!")
	}
	result.Stage = stage
	return result
}

// Returns a result that shows processing failed in unpack stage.
func resultFailedUnpack() (result *bagman.ProcessResult) {
	return result
}

// Returns a result that shows processing failed in store stage.
func resultFailedStore() (result *bagman.ProcessResult) {
	return result
}

// Returns a result that shows processing failed in record-to-fedora stage.
func resultFailedRecord() (result *bagman.ProcessResult) {
	return result
}

// Make sure ProcessStatus.IngestStatus() returns the correct
// ProcessStatus data.
func TestIngestStatus(t *testing.T) {
	passedFetch := getResult("Fetch", true)
	assertCorrectSummary(t, passedFetch, bagman.StatusPending)
	failedFetch := getResult("Fetch", false)
	assertCorrectSummary(t, failedFetch, bagman.StatusFailed)

	passedUnpack := getResult("Unpack", true)
	assertCorrectSummary(t, passedUnpack, bagman.StatusPending)
	failedUnpack := getResult("Unpack", false)
	assertCorrectSummary(t, failedUnpack, bagman.StatusFailed)

	passedStore := getResult("Store", true)
	assertCorrectSummary(t, passedStore, bagman.StatusPending)
	failedStore := getResult("Store", false)
	assertCorrectSummary(t, failedStore, bagman.StatusFailed)

	passedRecord := getResult("Record", true)
	assertCorrectSummary(t, passedRecord, bagman.StatusSuccess)
	failedRecord := getResult("Record", false)
	assertCorrectSummary(t, failedRecord, bagman.StatusFailed)
}

func assertCorrectSummary(t *testing.T, result *bagman.ProcessResult, expectedStatus bagman.StatusType) {
	status := result.IngestStatus()
	expectedBagDate := "2014-05-28 16:22:24.016 +0000 UTC"
	if status.Date == emptyTime {
		t.Error("ProcessStatus.Date was not set")
	}
	if status.Action != "Ingest" {
		t.Error("ProcessStatus.Type is incorrect. Should be Ingest.")
	}
	if status.Name != result.S3File.Key.Key {
		t.Errorf("ProcessStatus.Name: Expected %s, got %s",
			result.S3File.Key.Key,
			status.Name)
	}
	if status.Bucket != result.S3File.BucketName {
		t.Errorf("ProcessStatus.Bucket: Expected %s, got %s",
			result.S3File.BucketName,
			status.Bucket)
	}
	if status.BagDate.String() != expectedBagDate {
		t.Errorf("ProcessStatus.BagDate: Expected %s, got %s",
			expectedBagDate,
			status.BagDate)
	}
	if status.ETag != strings.Replace(result.S3File.Key.ETag, "\"", "", 2) {
		t.Errorf("ProcessStatus.ETag: Expected %s, got %s",
			result.S3File.Key.ETag,
			status.ETag)
	}
	if status.Stage != result.Stage {
		t.Errorf("ProcessStatus.Stage: Expected %s, got %s",
			result.Stage,
			status.Stage)
	}
	if status.Institution != bagman.OwnerOf(result.S3File.BucketName) {
		t.Errorf("ProcessStatus.Institution: Expected %s, got %s",
			bagman.OwnerOf(result.S3File.BucketName),
			status.Institution)
	}
	if result.ErrorMessage == "" && status.Note != "No problems" {
		t.Errorf("ProcessStatus.Note should be '%s', but it's '%s'.",
			"No problems", status.Note)
	}
	if result.ErrorMessage != "" && status.Note == "" {
		t.Error("ProcessStatus.Note should have a value, but it's empty.")
	}
	if result.ErrorMessage != "" && status.Note != result.ErrorMessage {
		t.Errorf("ProcessStatus.Note: Expected %s, got %s",
			result.ErrorMessage,
			status.Note)
	}
	if status.Status != expectedStatus {
		t.Errorf("ProcessStatus.Status: Expected %s, got %s",
			expectedStatus,
			status.Status)
		t.Errorf("This failure may be due to a temporary demo setting that considers Validation the final step.")
	}
}

func TestIntellectualObject(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}
	if obj.Title != "Title of an Intellectual Object" {
		t.Errorf("IntellectualObject.Title is '%s', expected '%s'.",
			obj.Title,
			"Title of an Intellectual Object")
	}
	if obj.Description != "Description of intellectual object." {
		t.Errorf("IntellectualObject.Description is '%s', expected '%s'.",
			obj.Description,
			"Description of intellectual object.")
	}
	if obj.Identifier != "ncsu.edu/ncsu.1840.16-2928" {
		t.Errorf("IntellectualObject.Identifier is '%s', expected '%s'.",
			obj.Identifier,
			"ncsu.edu.ncsu.1840.16-2928")
	}
	if obj.Access != "consortia" {
		t.Errorf("IntellectualObject.Access is '%s', expected '%s'.",
			obj.Access,
			"consortia")
	}

	// Special test for Identifier
	result.S3File.Key.Key = "ncsu.1840.16-2928-blah.b12.of79.tar"
	obj, err = result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}
	if obj.Identifier != "ncsu.edu/ncsu.1840.16-2928-blah" {
		t.Errorf("IntellectualObject.Identifier is '%s', expected '%s'.",
			obj.Identifier,
			"ncsu.edu.ncsu.1840.16-2928-blah")
	}
}

func TestGenericFiles(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	emptyTime := time.Time{}
	genericFiles, err := result.GenericFiles()
	if err != nil {
		t.Errorf("Error creating generic files from result: %v", err)
	}
	for _, gf := range genericFiles {
		if gf.URI == "" {
			t.Error("GenericFile.URI should not be nil")
		}
		if gf.Size <= 0 {
			t.Error("GenericFile.Size should be greater than zero")
		}
		if gf.Created == emptyTime {
			t.Error("GenericFile.Created should not be nil")
		}
		if gf.Modified == emptyTime {
			t.Error("GenericFile.Modified should not be nil")
		}
		if gf.Id != "" {
			t.Errorf("GenericFile.Id should be empty, but it's '%s'", gf.Id)
		}
		if strings.Index(gf.Identifier, "/") < 0 {
			t.Errorf("GenericFile.Identifier should contain slashes")
		}
		if strings.Index(gf.Identifier, "ncsu.edu") < 0 {
			t.Errorf("GenericFile.Identifier should contain the owner's domain name")
		}
		if strings.Index(gf.Identifier, strings.Replace(gf.Identifier, "/", "", -1)) > -1 {
			t.Errorf("GenericFile.Identifier should contain the file name")
		}
		for _, cs := range gf.ChecksumAttributes {
			if cs.Algorithm != "md5" && cs.Algorithm != "sha256" {
				t.Error("ChecksumAttribute.Algorithm should be either 'md5' or 'sha256'")
			}
			if cs.DateTime == emptyTime {
				t.Error("ChecksumAttribute.DateTime should not be nil")
			}
			if len(cs.Digest) == 0 {
				t.Error("ChecksumAttribute.Digest is empty")
			}

		}
	}

	// Look more closely at one GenericFile
	// A normal generic file URI would end with a UUID, but this
	// is an actual file in a fixture bucket used for testing.
	gf1 := genericFiles[0]
	if gf1.URI != "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml" {
		t.Errorf("GenericFile.URI is '%s', expected '%s'",
			gf1.URI,
			"https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml")
	}
	if gf1.Size != 5105 {
		t.Errorf("GenericFile.Size is %d, expected %d", gf1.Size, 5105)
	}
	// We can't get created time, so we're using modifed timstamp
	// for both created and modified
	modified, _ := time.Parse("2006-01-02T15:04:05Z", "2014-04-25T18:05:51Z")
	if gf1.Created != modified {
		t.Errorf("GenericFile.Created is %s, expected %d",
			gf1.Created,
			"0001-01-01T00:00:00Z")
	}
	if gf1.Modified != modified {
		t.Errorf("GenericFile.Modified is %s, expected %s",
			gf1.Modified,
			"2014-04-25T18:05:51Z")
	}

	// Test the checksums
	if gf1.ChecksumAttributes[0].Algorithm != "md5" {
		t.Errorf("ChecksumAttribute.Algorithm should be either 'md5'")
	}
	if gf1.ChecksumAttributes[0].Digest != "84586caa94ff719e93b802720501fcc7" {
		t.Errorf("ChecksumAttribute.Digest is %s, expected %s",
			gf1.ChecksumAttributes[0].Digest,
			"84586caa94ff719e93b802720501fcc7")
	}
	// MD5 checksum date is the modified date, since S3 calculates it
	// when the tar file is uploaded to the receiving bucket
	if gf1.ChecksumAttributes[0].DateTime != modified {
		t.Errorf("ChecksumAttributes.Date is %s, expected %s",
			gf1.ChecksumAttributes[0].DateTime,
			"2014-04-25T19:01:20.000Z")
	}

	if gf1.ChecksumAttributes[1].Algorithm != "sha256" {
		t.Errorf("ChecksumAttribute.Algorithm should be either 'md5'")
	}
	if gf1.ChecksumAttributes[1].Digest != "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784" {
		t.Errorf("ChecksumAttribute.Digest is %s, expected %s",
			gf1.ChecksumAttributes[1].Digest,
			"84586caa94ff719e93b802720501fcc7")
	}
	shaTime, _ := time.Parse("2006-01-02T15:04:05Z", "2014-06-09T14:12:45.574358959Z")
	if gf1.ChecksumAttributes[1].DateTime != shaTime {
		t.Errorf("ChecksumAttributes.Date is %s, expected %s",
			gf1.ChecksumAttributes[1].DateTime,
			"2014-06-09T14:12:45.574358959Z")
	}
}

func TestPremisEvents(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	emptyTime := time.Time{}
	genericFiles, err := result.GenericFiles()
	if err != nil {
		t.Errorf("Error creating generic files from result: %v", err)
	}
	for i, file := range genericFiles {
		if file.Events[0].EventType != "fixity_check" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[0].EventType,
				"fixity_check")
		}
		if file.Events[0].DateTime == emptyTime {
			t.Errorf("Event.DateTime is missing")
		}
		if file.Events[0].Identifier == "" {
			t.Errorf("Fixity check event identifier is missing")
		}

		if file.Events[1].EventType != "ingest" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[1].EventType,
				"ingest")
		}
		if file.Events[1].DateTime != result.TarResult.GenericFiles[i].StoredAt {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[1].DateTime,
				result.TarResult.GenericFiles[i].StoredAt)
		}
		if file.Events[1].OutcomeDetail != result.TarResult.GenericFiles[i].StorageMd5 {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[1].OutcomeDetail,
				result.TarResult.GenericFiles[i].StorageMd5)
		}
		if file.Events[1].Identifier == "" {
			t.Errorf("Ingest event identifier is missing")
		}

		if file.Events[2].EventType != "fixity_generation" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[2].EventType,
				"fixity_generation")
		}
		if file.Events[2].DateTime != result.TarResult.GenericFiles[i].Sha256Generated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[2].DateTime,
				result.TarResult.GenericFiles[i].Sha256Generated)
		}
		expected256 := fmt.Sprintf("sha256:%s", result.TarResult.GenericFiles[i].Sha256)
		if file.Events[2].OutcomeDetail != expected256 {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[2].OutcomeDetail,
				expected256)
		}
		if file.Events[2].Identifier == "" {
			t.Errorf("Fixity generation event id is missing")
		}

		if file.Events[3].EventType != "identifier_assignment" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[3].EventType,
				"identifier_assignment")
		}
		if file.Events[3].DateTime != result.TarResult.GenericFiles[i].UuidGenerated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[3].DateTime,
				result.TarResult.GenericFiles[i].UuidGenerated)
		}
		if file.Events[3].OutcomeDetail != result.TarResult.GenericFiles[i].Identifier {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[3].OutcomeDetail,
				result.TarResult.GenericFiles[i].Identifier)
		}
		if file.Events[3].Identifier == "" {
			t.Errorf("Identifier assignement event id is missing")
		}

		if file.Events[4].EventType != "identifier_assignment" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[4].EventType,
				"identifier_assignment")
		}
		if file.Events[4].DateTime != result.TarResult.GenericFiles[i].UuidGenerated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[4].DateTime,
				result.TarResult.GenericFiles[i].UuidGenerated)
		}
		if file.Events[4].OutcomeDetail != result.TarResult.GenericFiles[i].StorageURL {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[4].OutcomeDetail,
				result.TarResult.GenericFiles[i].StorageURL)
		}
		if file.Events[4].Identifier == "" {
			t.Errorf("Identifier assignement event id is missing")
		}
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

func TestMetadataRecordSucceeded(t *testing.T) {
	record := &bagman.MetadataRecord{
		Type:         "PremisEvent",
		Action:       "fixity_generation",
		EventObject:  "data/ORIGINAL/1",
		ErrorMessage: "",
	}
	if record.Succeeded() == false {
		t.Error("MetadataRecord.Succeeded() returned false when it should return true")
	}
	record.ErrorMessage = "Server returned status code 403: forbidden"
	if record.Succeeded() == true {
		t.Error("MetadataRecord.Succeeded() returned true when it should return false")
	}
}

func getFedoraResult(t *testing.T) *bagman.FedoraResult {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	intellectualObject, err := result.IntellectualObject()
	if err != nil {
		t.Error(err)
	}
	genericFilePaths := result.TarResult.GenericFilePaths()
	return bagman.NewFedoraResult(intellectualObject.Identifier, genericFilePaths)
}

func TestFedoraResultAddRecord(t *testing.T) {

	fedoraResult := getFedoraResult(t)

	// Add some invalid MetadataRecords, and make sure we get errors
	// Bad type
	err := fedoraResult.AddRecord("BadType", "some action", "some object", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with bad type")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with bad type to its collection")
	}

	// Good type, bad action
	err = fedoraResult.AddRecord("PremisEvent", "some action", "some object", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with bad action")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with bad action to its collection")
	}

	// Good type, good action, missing eventObject
	err = fedoraResult.AddRecord("PremisEvent", "some action", "", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with missing event object")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with missing event object to its collection")
	}

	// Good records
	err = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid IntellectualObject record: %v", err)
	}
	err = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid GenericFile record: %v", err)
	}
	err = fedoraResult.AddRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid PremisEvent record for fixity_generation: %v", err)
	}
	err = fedoraResult.AddRecord("PremisEvent", "identifier_assignment", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid PremisEvent record for identifier_assignment: %v", err)
	}
	if len(fedoraResult.MetadataRecords) != 4 {
		t.Errorf("FedoraResult should have 4 MetadataRecords, but it has %d", len(fedoraResult.MetadataRecords))
	}
}

func TestFedoraResultFindRecord(t *testing.T) {

	fedoraResult := getFedoraResult(t)

	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "")
	_ = fedoraResult.AddRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1", "")

	record := fedoraResult.FindRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier)
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("GenericFile", "file_registered", "data/ORIGINAL/1")
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1")
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("No such record", "", "")
	if record != nil {
		t.Error("FedoraResult.FindRecord returned a record when it shouldn't have")
	}

}

func TestFedoraResultRecordSucceeded(t *testing.T) {

	fedoraResult := getFedoraResult(t)

	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "Internet blew up")

	succeeded := fedoraResult.RecordSucceeded("IntellectualObject", "object_registered",
		fedoraResult.ObjectIdentifier)
	if false == succeeded {
		t.Error("FedoraResult.RecordSucceeded returned false when it should have returned true")
	}
	succeeded = fedoraResult.RecordSucceeded("GenericFile", "file_registered", "data/ORIGINAL/1")
	if true == succeeded {
		t.Error("FedoraResult.RecordSucceeded returned true when it should have returned false")
	}
}

func TestAllRecordsSucceeded(t *testing.T) {

	fedoraResult := getFedoraResult(t)

	// Add successful events for the intellectual object
	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("PremisEvent", "ingest", fedoraResult.ObjectIdentifier, "")
	// Add successful events for each generic file
	for _, path := range expectedPaths {
		_ = fedoraResult.AddRecord("GenericFile", "file_registered", path, "")
		_ = fedoraResult.AddRecord("PremisEvent", "identifier_assignment", path, "")
		_ = fedoraResult.AddRecord("PremisEvent", "fixity_generation", path, "")
	}

	if fedoraResult.AllRecordsSucceeded() == false {
		t.Error("FedoraResult.AllRecordsSucceeded() returned false when it should have returned true")
	}

	// Alter one record so it fails...
	record := fedoraResult.FindRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1")
	record.ErrorMessage = "Fluctus got drunk and dropped all punch cards in the toilet"

	if fedoraResult.AllRecordsSucceeded() == true {
		t.Error("FedoraResult.AllRecordsSucceeded() returned true when it should have returned false")
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

func TestDeleteAttemptedAndSucceeded(t *testing.T) {
	filepath := filepath.Join("testdata", "cleanup_result.json")
	var result bagman.CleanupResult
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}

	if result.Succeeded() == false {
		t.Error("result.Succeeded() should have returned true")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = "Spongebob"
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = ""
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == true {
			t.Error("file.DeleteAttempted() should have returned false")
		}
	}

}

func buildGfModels() ([]*models.GenericFile) {
	// Changed file
	md5_1 := &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "TestMd5Digest",
	}
	sha256_1 := &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "TestSha256Digest",
	}
	checksums1 := make([]*models.ChecksumAttribute, 2)
	checksums1[0] = md5_1
	checksums1[1] = sha256_1
	gfModel1 := &models.GenericFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml",
		ChecksumAttributes: checksums1,
	}

	// Existing file, unchanged
	md5_2 := &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "a340203a24dcd6f6ca2bc95a4956c65d",
	}
	sha256_2 := &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "54536211e3ad308e8509091a1db393cbcc7fadd4a9b7f434bec8097d149a2039",
	}
	checksums2 := make([]*models.ChecksumAttribute, 2)
	checksums2[0] = md5_2
	checksums2[1] = sha256_2
	gfModel2 := &models.GenericFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/object.properties",
		ChecksumAttributes: checksums2,
	}

	gfModels := make([]*models.GenericFile, 2)
	gfModels[0] = gfModel1
	gfModels[1] = gfModel2
	return gfModels
}

func TestMergeExistingFiles(t *testing.T) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	gfModels := buildGfModels()
	result.TarResult.MergeExistingFiles(gfModels)

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

func TestS3ObjectName(t *testing.T) {
	s3File := bagman.S3File {
		BucketName: "aptrust.receiving.virginia.edu",
		Key: s3.Key {
			Key: "some.file.b001.of200.tar",
		},
	}
	expected := "virginia.edu/some.file"
	actual, err := s3File.ObjectName()
	if err != nil {
		t.Errorf("S3File.ObjectName() returned error %v", err)
	}
	if actual != expected {
		t.Errorf("S3File.ObjectName() should have returned '%s', but returned '%s'",
			expected, actual)
	}
	s3File.Key.Key = "some.file.b1.of2.tar"
	actual, _ = s3File.ObjectName()
	if actual != expected {
		t.Errorf("S3File.ObjectName() should have returned '%s', but returned '%s'",
			expected, actual)
	}
}

func TestS3BagName(t *testing.T) {
	s3File := bagman.S3File {
		BucketName: "aptrust.receiving.virginia.edu",
		Key: s3.Key {
			Key: "some.file.b001.of200.tar",
		},
	}
	expected := "virginia.edu/some.file.b001.of200.tar"
	actual := s3File.BagName()
	if actual != expected {
		t.Errorf("S3File.BagName() should have returned '%s', but returned '%s'",
			expected, actual)
	}

	s3File.Key.Key = "some.file.b1.of2.tar"
	expected = "virginia.edu/some.file.b1.of2.tar"
	actual = s3File.BagName()
	if actual != expected {
		t.Errorf("S3File.BagName() should have returned '%s', but returned '%s'",
			expected, actual)
	}
}

func TestProcessStatusSerializeForFluctus(t *testing.T) {
	bagDate, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-07-02T12:00:00.000Z")
	ingestDate, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-09-10T12:00:00.000Z")
	ps := bagman.ProcessStatus{
		Id: 9000,
		ObjectIdentifier: "ncsu.edu/some_object",
		GenericFileIdentifier: "ncsu.edu/some_object/data/doc.pdf",
		Name: "Sample Document",
		Bucket: "aptrust.receiving.ncsu.edu",
		ETag: "12345",
		BagDate: bagDate,
		Institution: "ncsu.edu",
		Date: ingestDate,
		Note: "so many!",
		Action: "Ingest",
		Stage: "Store",
		Status: "Success",
		Outcome: "happy day!",
		Retry: true,
		Reviewed: false,
	}
	bytes, err := ps.SerializeForFluctus()
	if err != nil {
		t.Error(err)
	}
	expected := "{\"action\":\"Ingest\",\"bag_date\":\"2014-07-02T12:00:00Z\",\"bucket\":\"aptrust.receiving.ncsu.edu\",\"date\":\"2014-09-10T12:00:00Z\",\"etag\":\"12345\",\"generic_file_identifier\":\"ncsu.edu/some_object/data/doc.pdf\",\"institution\":\"ncsu.edu\",\"name\":\"Sample Document\",\"note\":\"so many!\",\"object_identifier\":\"ncsu.edu/some_object\",\"outcome\":\"happy day!\",\"retry\":true,\"reviewed\":false,\"stage\":\"Store\",\"status\":\"Success\"}"
	actual := string(bytes)
	if actual != expected {
		t.Errorf("ProcessStatus.SerializeForFluctus expected:\n'%s'\nbut got:\n'%s'", expected, actual)
	}
}

func TestProcessStatusHasBeenStored(t *testing.T) {
	ps := bagman.ProcessStatus{
		Action: "Ingest",
		Stage: "Record",
		Status: "Success",
	}
	if ps.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = bagman.StageCleanup
	if ps.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = bagman.StageStore
	ps.Status = bagman.StatusPending
	if ps.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = bagman.StageStore
	ps.Status = bagman.StatusStarted
	if ps.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	ps.Stage = bagman.StageFetch
	if ps.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	ps.Stage = bagman.StageUnpack
	if ps.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	ps.Stage = bagman.StageValidate
	if ps.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
}

func TestProcessStatusShouldTryIngest(t *testing.T) {
	ps := bagman.ProcessStatus{
		Action: "Ingest",
		Stage: "Receive",
		Status: "Pending",
		Retry: true,
	}

	// Test stages
	if ps.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = "Fetch"
	if ps.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = "Unpack"
	if ps.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = "Validate"
	if ps.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	ps.Stage = "Record"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	// Test Store/Pending and Store/Started
	ps.Stage = "Store"
	ps.Status = "Started"
	if ps.ShouldTryIngest() == true {
		t.Error("ShouldTryIngest() should have returned false")
	}

	ps.Stage = "Store"
	ps.Status = "Pending"
	if ps.ShouldTryIngest() == true {
		t.Error("ShouldTryIngest() should have returned false")
	}

	// Test Retry = false
	ps.Status = "Started"
	ps.Retry = false

	ps.Stage = "Receive"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	ps.Stage = "Fetch"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	ps.Stage = "Unpack"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	ps.Stage = "Validate"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	ps.Stage = "Record"
	if ps.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}


}
