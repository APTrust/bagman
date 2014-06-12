package bagman_test

import (
    "testing"
    "time"
    "fmt"
    "strings"
    "io/ioutil"
    "encoding/json"
    "path/filepath"
    "launchpad.net/goamz/s3"
    "github.com/APTrust/bagman"
)

var emptyTime time.Time = time.Time{}

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
    result.Tags[0] = bagman.Tag{ Label: "Label One", Value: "Value One" }
    result.Tags[1] = bagman.Tag{ Label: "Label Two", Value: "Value Two" }

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
func getResult(stage string, successful bool) (result *bagman.ProcessResult) {
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
    assertCorrectSummary(t, passedFetch, "Processing")
    failedFetch := getResult("Fetch", false)
    assertCorrectSummary(t, failedFetch, "Failed")

    passedUnpack := getResult("Unpack", true)
    assertCorrectSummary(t, passedUnpack, "Processing")
    failedUnpack := getResult("Unpack", false)
    assertCorrectSummary(t, failedUnpack, "Failed")

    passedStore := getResult("Store", true)
    assertCorrectSummary(t, passedStore, "Processing")
    failedStore := getResult("Store", false)
    assertCorrectSummary(t, failedStore, "Failed")

    passedRecord := getResult("Record", true)
    // TODO: Change Processing to Succeeded when Record step is working.
    assertCorrectSummary(t, passedRecord, "Processing")
    failedRecord := getResult("Record", false)
    assertCorrectSummary(t, failedRecord, "Failed")
}

func assertCorrectSummary(t *testing.T, result *bagman.ProcessResult, expectedStatus string) {
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
        t.Error("ProcessStatus.Note should be '%s', but it's '%s'.",
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

// Loads a result from the test data directory.
func loadResult(filename string) (result *bagman.ProcessResult, err error) {
    file, err := ioutil.ReadFile(filename)
    if err != nil {
        return nil, err
    }
    err = json.Unmarshal(file, &result)
    if err != nil{
        return nil, err
    }
    return result, nil
}

func TestIntellectualObject(t *testing.T) {
    filepath := filepath.Join("testdata", "result_good.json")
    result, err := loadResult(filepath)
    if err != nil {
        t.Errorf("Error loading test data file '%s': %v", filepath, err)
    }
    obj := result.IntellectualObject()
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
    if obj.Identifier != "ncsu.edu.ncsu.1840.16-2928" {
        t.Errorf("IntellectualObject.Identifier is '%s', expected '%s'.",
            obj.Identifier,
            "ncsu.edu.ncsu.1840.16-2928")
    }
    if obj.Access != "Consortial" {
        t.Errorf("IntellectualObject.Access is '%s', expected '%s'.",
            obj.Access,
            "Consortial")
    }
}

func TestGenericFiles(t *testing.T) {
    filepath := filepath.Join("testdata", "result_good.json")
    result, err := loadResult(filepath)
    if err != nil {
        t.Errorf("Error loading test data file '%s': %v", filepath, err)
    }
    emptyTime := time.Time{}
    genericFiles := result.GenericFiles()

    for _, gf := range(genericFiles) {
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
    gf1 := genericFiles[0]
    if gf1.URI != "https://s3.amazonaws.com/aptrust.storage/b21fdb34-1f79-4101-62c5-56918f4782fc" {
        t.Errorf("GenericFile.URI is '%s', expected '%s'",
            gf1.URI,
            "https://s3.amazonaws.com/aptrust.storage/b21fdb34-1f79-4101-62c5-56918f4782fc")
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
    result, err := loadResult(filepath)
    if err != nil {
        t.Errorf("Error loading test data file '%s': %v", filepath, err)
    }
    emptyTime := time.Time{}
    genericFiles := result.GenericFiles()
    for i, file := range(genericFiles) {
        if file.Events[0].EventType != "Ingest" {
            t.Errorf("EventType is '%s', expected '%s'",
                file.Events[0].EventType,
                "Ingest")
        }
        if file.Events[0].DateTime != emptyTime {
            t.Errorf("DateTime is %v, expected %v",
                file.Events[0].DateTime,
                emptyTime)
        }
        if file.Events[1].EventType != "Fixity Generation" {
            t.Errorf("EventType is '%s', expected '%s'",
                file.Events[1].EventType,
                "Ingest")
        }
        if file.Events[1].DateTime != result.TarResult.GenericFiles[i].Sha256Generated {
            t.Errorf("DateTime is %v, expected %v",
                file.Events[1].DateTime,
                result.TarResult.GenericFiles[i].Sha256Generated)
        }
        if file.Events[1].OutcomeDetail != result.TarResult.GenericFiles[i].Sha256 {
            t.Errorf("OutcomeDetail is '%s', expected '%s'",
                file.Events[1].OutcomeDetail,
                result.TarResult.GenericFiles[i].Sha256)
        }
        if file.Events[2].EventType != "Identifier Assignment" {
            t.Errorf("EventType is '%s', expected '%s'",
                file.Events[2].EventType,
                "Identifier Assignment")
        }
        if file.Events[2].DateTime != result.TarResult.GenericFiles[i].UuidGenerated {
            t.Errorf("DateTime is %v, expected %v",
                file.Events[2].DateTime,
                result.TarResult.GenericFiles[i].UuidGenerated)
        }
        if file.Events[2].OutcomeDetail != result.TarResult.GenericFiles[i].Uuid {
            t.Errorf("OutcomeDetail is '%s', expected '%s'",
                file.Events[2].OutcomeDetail,
                result.TarResult.GenericFiles[i].Uuid)
        }
    }
}
