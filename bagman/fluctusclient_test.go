// Integration tests for Fluctus client.
// Requires a running Fluctus server.
package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/satori/go.uuid"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var fluctusAPIVersion string = "v1"
var fluctusSkipMessagePrinted bool = false

// objId and gfId come from our test fixture in testdata/result_good.json
var objId string = "ncsu.edu/ncsu.1840.16-2928"
var gfId string = "ncsu.edu/ncsu.1840.16-2928/data/object.properties"

var testInstitutionId = "fe908327-3635-43c2-9ca6-849485febcf3"

func runFluctusTests() bool {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		if fluctusSkipMessagePrinted == false {
			fluctusSkipMessagePrinted = true
			fmt.Printf("Skipping fluctus integration tests: "+
				"Fluctus server is not running at %s\n", fluctusUrl)
		}
		return false
	}
	return true
}

func getClient(t *testing.T) *bagman.FluctusClient {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	logger := bagman.DiscardLogger("client_test")
	fluctusClient, err := bagman.NewFluctusClient(
		fluctusUrl,
		fluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		logger)
	if err != nil {
		t.Errorf("Error constructing fluctus client: %v", err)
	}
	return fluctusClient
}

// Loads an intellectual object with events and generic files
// from a test fixture into our test Fedora/Fluctus instance.
func loadTestResult(t *testing.T) error {

	fluctusClient := getClient(t)

	// Load processing result fixture
	testfile := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return err
	}
	// Get the intellectual object from the processing result
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}

	// Try to get the object with this identifier from Fluctus
	fluctusObj, err := fluctusClient.IntellectualObjectGet(obj.Identifier, false)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
		return err
	}

	// Add this object to fluctus if it doesn't already exist.
	if fluctusObj == nil {
		_, err := fluctusClient.IntellectualObjectCreate(obj, bagman.MAX_FILES_FOR_CREATE)
		if err != nil {
			t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
			return err
		}
	}

	return nil
}

func TestIntellectualObjectGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	// Get the lightweight version of an existing object
	obj, err := fluctusClient.IntellectualObjectGet(objId, false)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
	}
	if obj == nil {
		t.Error("IntellectualObjectGet did not return the expected object")
	}
	if obj != nil && len(obj.GenericFiles) > 0 {
		t.Error("IntellectualObject has GenericFiles. It shouldn't.")
	}

	// Get the heavyweight version of an existing object,
	// and make sure the related fields are actually there.
	obj, err = fluctusClient.IntellectualObjectGet(objId, true)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
	}
	if obj == nil {
		t.Error("IntellectualObjectGet did not return the expected object")
	}
	if obj != nil {
		if len(obj.GenericFiles) == 0 {
			t.Error("IntellectualObject has no GenericFiles, but it should.")
		}
		gf := findFile(obj.GenericFiles, gfId)
		if len(gf.Events) == 0 {
			t.Error("GenericFile from Fluctus is missing events.")
		}
		if len(gf.ChecksumAttributes) == 0 {
			t.Error("GenericFile from Fluctus is missing checksums.")
		}
	}

	// Make sure we don't blow up when fetching an object that does not exist.
	obj, err = fluctusClient.IntellectualObjectGet("changeme:99999", false)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
	}
	if obj != nil {
		t.Errorf("IntellectualObjectGet returned something that shouldn't be there: %v", obj)
	}

}

func TestIntellectualObjectGetForRestore(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	// Get the lightweight version of an existing object
	obj, err := fluctusClient.IntellectualObjectGetForRestore(objId)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObjectGetForRestore: %v", err)
	}
	if obj == nil {
		t.Error("IntellectualObjectGetForRestore did not return the expected object")
	}

	// IntellectualObjectGetForRestore returns only a bare minimum of
	// info about each generic file. Just enough to restore: identifier,
	// size and uri.
	if obj != nil {
		if len(obj.GenericFiles) == 0 {
			t.Error("IntellectualObject has no GenericFiles, but it should.")
		}
		gf := findFile(obj.GenericFiles, gfId)
		if gf.Size != 73 {
			t.Error("GenericFile from Fluctus has incorrect size attribute.")
		}
		if gf.URI != "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/object.properties" {
			t.Error("GenericFile from Fluctus has incorrect URI.")
		}
		if gf.Identifier != "ncsu.edu/ncsu.1840.16-2928/data/object.properties" {
			t.Error("GenericFile from Fluctus has incorrect identifier.")
		}
	}

	// Make sure we get an error on a bad call
	obj, err = fluctusClient.IntellectualObjectGetForRestore("changeme:99999")
	if err == nil {
		t.Errorf("IntellectualObjectGetForRestore should have returned an error.")
	}

}

// Returns the file with the specified id. We use this in testing
// because we want to look at a file that we know has both events
// and checksums.
func findFile(files []*bagman.GenericFile, id string) *bagman.GenericFile {
	for _, f := range files {
		if f.Identifier == id || f.Id == id {
			return f
		}
	}
	return nil
}

func TestIntellectualObjectUpdate(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	obj, err := fluctusClient.IntellectualObjectGet(objId, false)
	if err != nil {
		t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
	}
	if obj == nil {
		t.Error("IntellectualObjectGet did not return the expected object")
		return // Can't finish remaining tests
	}

	// Update an existing object
	newObj, err := fluctusClient.IntellectualObjectUpdate(obj)
	if err != nil {
		t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
	}
	if newObj == nil {
		t.Error("New object should be an object, but it's nil.")
	} else if newObj.Id != obj.Id || newObj.Title != obj.Title ||
		newObj.Description != obj.Description {
		t.Error("New object attributes don't match what was submitted.")
	}
}

func TestIntellectualObjectCreate(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	// Load processing result fixture
	testfile := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return
	}
	// Get the intellectual object from the processing result
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
		return
	}

	// Save a new object... just change the id & identifier,
	// so Fluctus thinks it's new
	obj.Id = ""
	oldIdentifier := obj.Identifier
	obj.Identifier = fmt.Sprintf("test.edu/%d", time.Now().Unix())
	// Update the identifier on all of the generic files...
	for i := range obj.GenericFiles {
		obj.GenericFiles[i].Identifier = strings.Replace(
			obj.GenericFiles[i].Identifier, oldIdentifier, obj.Identifier, 1)
	}
	newObj, err := fluctusClient.IntellectualObjectCreate(obj, bagman.MAX_FILES_FOR_CREATE)
	if err != nil {
		t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
		return
	}
	if newObj.Identifier != obj.Identifier || newObj.Title != obj.Title ||
		newObj.Description != obj.Description {
		t.Error("New object attributes don't match what was submitted.")
	}

	// Make sure alt_identifer was saved
	if len(newObj.AltIdentifier) != 1 {
		t.Errorf("Expected new object to have 1 alt identifier. It has %s", len(newObj.AltIdentifier))
	} else if newObj.AltIdentifier[0] != "ncsu-internal-id-0001" {
		t.Errorf("Expected alt_identifier 'ncsu-internal-id-0001', got '%s'", newObj.AltIdentifier[0])
	}
}

func TestGenericFileGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	// Get the lightweight version of an existing object
	gf, err := fluctusClient.GenericFileGet(gfId, false)
	if err != nil {
		t.Errorf("Error asking fluctus for GenericFile: %v", err)
	}
	if gf == nil {
		t.Error("GenericFileGet did not return the expected object")
	}
	if gf != nil && len(gf.Events) > 0 {
		t.Error("GenericFile has Events. It shouldn't.")
	}
	if gf != nil && len(gf.ChecksumAttributes) > 0 {
		t.Error("GenericFile has ChecksumAttributes. It shouldn't.")
	}

	// Get the heavyweight version of an existing generic file,
	// and make sure the related fields are actually there.
	gf, err = fluctusClient.GenericFileGet(gfId, true)
	if err != nil {
		t.Errorf("Error asking fluctus for GenericFile: %v", err)
	}
	if gf == nil {
		t.Error("GenericFile did not return the expected object")
	}
	if gf != nil {
		if len(gf.Events) == 0 {
			t.Error("GenericFile from Fluctus is missing events.")
		}
		if len(gf.ChecksumAttributes) == 0 {
			t.Error("GenericFile from Fluctus is missing checksums.")
		}
	}

	// Make sure we don't blow up when fetching an object that does not exist.
	gf, err = fluctusClient.GenericFileGet("changeme:99999", false)
	if err != nil {
		t.Errorf("Error asking fluctus for GenericFile: %v", err)
	}
	if gf != nil {
		t.Errorf("GenericFile returned something that shouldn't be there: %v", gf)
	}

}

func TestGenericFileSave(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	gf, err := fluctusClient.GenericFileGet(gfId, true)
	if err != nil {
		t.Errorf("Error asking fluctus for GenericFile: %v", err)
	}
	if gf == nil {
		t.Error("GenericFileGet did not return the expected file")
		return // Can't finish remaining tests
	}

	// Fluctus pukes when there's no identifier.
	if gf.Identifier == "" {
		gf.Identifier = "/data/blah/blah/blah.xml"
	}

	// Update an existing file
	newGf, err := fluctusClient.GenericFileSave(objId, gf)
	if err != nil {
		t.Errorf("Error updating existing GenericFile in fluctus: %v", err)
		return // Can't proceed with other tests if this didn't work
	}
	if newGf.Identifier != gf.Identifier || newGf.URI != gf.URI ||
		newGf.Size != gf.Size {
		t.Error("New file attributes don't match what was submitted.")
	}

	// Save a new file... just change the id, so Fluctus thinks it's new
	gf.Id = fmt.Sprintf("test:%d", time.Now().Unix())
	newGf, err = fluctusClient.GenericFileSave(objId, gf)
	if err != nil {
		t.Errorf("Error saving new GenericFile to fluctus: %v", err)
		return // Can't proceed with next test
	}
	if newGf.Identifier != gf.Identifier || newGf.URI != gf.URI ||
		newGf.Size != gf.Size {
		t.Error("New file attributes don't match what was submitted.")
	}
}

func TestEventSave(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	eventId := uuid.NewV4()
	ingestEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "Ingest",
		DateTime:           time.Now(),
		Detail:             "Completed copy to perservation bucket",
		Outcome:            string(bagman.StatusSuccess),
		OutcomeDetail:      "md5: 000000001234567890",
		Object:             "goamz S3 client",
		Agent:              "https://github.com/crowdmob/goamz/s3",
		OutcomeInformation: "Multipart put using md5 checksum",
	}

	// Make sure we can save an IntellectualObject event
	obj, err := fluctusClient.PremisEventSave(objId, "IntellectualObject", ingestEvent)
	if err != nil {
		t.Errorf("Error saving IntellectualObject ingest event to Fluctus: %v", err)
	}
	if obj == nil {
		t.Error("PremisEventSave did not return the expected event object")
		return // Can't finish remaining tests
	}
	if obj.Identifier != ingestEvent.Identifier {
		t.Error("PremisEventSave returned object with wrong id")
	}

	eventId = uuid.NewV4()
	identifierEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "identifier_assignment",
		DateTime:           time.Now(),
		Detail:             "S3 key generated for file",
		Outcome:            string(bagman.StatusSuccess),
		OutcomeDetail:      "00000000-0000-0000-0000-000000000000",
		Object:             "GoUUID",
		Agent:              "https://github.com/satori/go.uuid",
		OutcomeInformation: "Generated with uuid.NewV4()",
	}

	// Make sure we can save an IntellectualObject event
	obj, err = fluctusClient.PremisEventSave(gfId, "GenericFile", identifierEvent)
	if err != nil {
		t.Errorf("Error saving GenericFile identifier assignment event to Fluctus: %v", err)
	}
	if obj == nil {
		t.Error("PremisEventSave did not return the expected event object")
		return // Can't finish remaining tests
	}
	if obj.Identifier != identifierEvent.Identifier {
		t.Error("PremisEventSave returned object with wrong id")
	}
}

func TestCacheInstitutions(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)
	err := fluctusClient.CacheInstitutions()
	if err != nil {
		t.Errorf("Error caching institutions: %v", err)
	}
}

func TestInstitutionGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)
	inst, err := fluctusClient.InstitutionGet("test.edu")
	if err != nil {
		t.Errorf("Error getting institution 'test.edu': %v", err)
		return
	}
	if inst.Name != "Test University" {
		t.Errorf("Expected Name 'Test University', got '%s'", inst.Name)
	}
	if inst.Identifier != "test.edu" {
		t.Errorf("Expected Identifier 'test.edu', got '%s'", inst.Identifier)
	}
	if inst.BriefName != "test" {
		t.Errorf("Expected BriefName 'test', got '%s'", inst.BriefName)
	}
	if inst.DpnUuid != testInstitutionId {
		t.Errorf("Expected name '%s', got '%s'", testInstitutionId, inst.DpnUuid)
	}
}

func TestBulkStatusGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	err := loadTestResult(t)
	if err != nil {
		return
	}

	sinceWhen, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-01-01T12:00:00.000Z")
	records, err := fluctusClient.BulkStatusGet(sinceWhen)
	if err != nil {
		t.Errorf("Error getting bulk status: %v", err)
	}
	if len(records) == 0 {
		t.Error("BulkStatusGet returned no records when it should have returned something.")
	}

	records, err = fluctusClient.BulkStatusGet(time.Now())
	if err != nil {
		t.Errorf("Error getting bulk status: %v", err)
	}
	if len(records) != 0 {
		t.Error("BulkStatusGet records when it shouldn't have.")
	}
}

func TestSendProcessedItem(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)
	itemName := uuid.NewV4()
	status := &bagman.ProcessStatus{
		Id:          0,
		Name:        itemName.String(),
		ObjectIdentifier:  fmt.Sprintf("test.edu/%s", itemName.String()),
		Bucket:      "aptrust.receiving.test.test.edu",
		ETag:        "0000000000",
		BagDate:     time.Now().UTC(),
		Institution: "test.edu",
		Date:        time.Now().UTC(),
		Note:        "Test item",
		Action:      "Ingest",
		Stage:       "Receive",
		Status:      "Pending",
		Outcome:     "O-diddly Kay!",
		Retry:       true,
		Reviewed:    false,
		State:       "{ This should be a blob of JSON }",
		Node:        "10.11.12.13",
		Pid:         31337,
		NeedsAdminReview: true,
	}

	// Create new records
	err := fluctusClient.SendProcessedItem(status)
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}
	if status.Id != 0 {
		t.Error("status.Id was reassigned when it should not have been")
	}

	// Update existing record
	err = fluctusClient.SendProcessedItem(status)
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}
	if status.Id == 0 {
		t.Error("status.Id should have been reassigned but was not")
	}
}

func TestGetReviewedItems(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	// Make sure we have a couple of reviewed items...
	sinceWhen, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-01-01T12:00:00.000Z")
	records, err := fluctusClient.BulkStatusGet(sinceWhen)

	if err != nil {
		t.Errorf("Error getting bulk status: %v", err)
	}
	if len(records) < 2 {
		t.Errorf("Expected at least 2 status records. Abandoning TestGetReviewedItems")
		return
	}
	records[0].Reviewed = true
	records[1].Reviewed = true
	err = fluctusClient.SendProcessedItem(records[0])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	reviewed, err := fluctusClient.GetReviewedItems()
	if err != nil {
		t.Errorf("Error getting reviewed items: %v", err)
	}
	if len(reviewed) < 2 {
		t.Errorf("GetReviewedItems returned %d items; expected at least two", len(reviewed))
	}
}


func TestRestorationItemsGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	// Make sure we have a couple of items to be restored...
	sinceWhen, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-01-01T12:00:00.000Z")
	records, err := fluctusClient.BulkStatusGet(sinceWhen)

	if err != nil {
		t.Errorf("Error getting bulk status: %v", err)
	}
	if len(records) < 2 {
		t.Errorf("Not enough records in Fluctus to test RestorationItemsGet")
		return
	}
	// TODO: This causes a problem because it REPLACES the original
	// ingest record in Fluctus. It should ADD a NEW restore record.
	records[0].Action = bagman.ActionRestore
	records[0].Stage = bagman.StageRequested
	records[0].Status = bagman.StatusPending
	records[0].Retry = true
	err = fluctusClient.SendProcessedItem(records[0])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}
	records[1].Action = bagman.ActionRestore
	records[1].Stage = bagman.StageRequested
	records[1].Status = bagman.StatusPending
	records[1].Retry = true
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	// Get items to be restored. There should be at least
	// the two we just saved.
	itemsToRestore, err := fluctusClient.RestorationItemsGet("")
	if err != nil {
		t.Errorf("Error getting restoration items: %v", err)
	}
	if len(itemsToRestore) < 2 {
		t.Error("RestorationItemsGet returned no records when it should have returned something.")
	}

	// Ask for records with a specific object identifier.
	// We should get at least the one we set up here.
	lastRecord := records[len(records)-1]
	lastRecord.Action = bagman.ActionRestore
	lastRecord.Stage = bagman.StageResolve
	lastRecord.Status = bagman.StatusFailed
	lastRecord.Retry = true
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	itemsToRestore, err = fluctusClient.RestorationItemsGet(lastRecord.ObjectIdentifier)
	if err != nil {
		t.Errorf("Error getting restoration items: %v", err)
	}
	if len(itemsToRestore) < 1 {
		t.Error("RestorationItemsGet returned no records when it should have returned something.")
	}

	// Make sure we get empty list and not error when there are no items
	lastRecord.Retry = false
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	itemsToRestore, err = fluctusClient.RestorationItemsGet(lastRecord.ObjectIdentifier)
	if err != nil {
		t.Errorf("Error getting restoration items: %v", err)
	}
	if len(itemsToRestore) == 0 {
		t.Error("RestorationItemsGet returned no records when it should have returned something.")
	}
}

func TestDeletionItemsGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	// Make sure we have a couple of items to be restored...
	sinceWhen, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-01-01T12:00:00.000Z")
	records, err := fluctusClient.BulkStatusGet(sinceWhen)

	if err != nil {
		t.Errorf("Error getting bulk status: %v", err)
	}
	if len(records) < 2 {
		t.Errorf("Not enough records in Fluctus to test DeletionItemsGet")
		return
	}
	// TODO: This causes a problem because it REPLACES the original
	// ingest record in Fluctus. It should ADD a NEW delete record.
	records[0].Action = bagman.ActionDelete
	records[0].Stage = bagman.StageRequested
	records[0].Status = bagman.StatusPending
	records[0].Retry = true
	err = fluctusClient.SendProcessedItem(records[0])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}
	records[1].Action = bagman.ActionDelete
	records[1].Stage = bagman.StageRequested
	records[1].Status = bagman.StatusPending
	records[1].Retry = true
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	// Get items to be restored. There should be at least
	// the two we just saved.
	itemsToRestore, err := fluctusClient.DeletionItemsGet("")
	if err != nil {
		t.Errorf("Error getting deletion items: %v", err)
	}
	if len(itemsToRestore) < 2 {
		t.Error("DeletionItemsGet returned no records when it should have returned something.")
	}

	// Ask for records with a specific object identifier.
	// We should get at least the one we set up here.
	lastRecord := records[len(records)-1]
	lastRecord.Action = bagman.ActionDelete
	lastRecord.Stage = bagman.StageResolve
	lastRecord.Status = bagman.StatusFailed
	lastRecord.Retry = true
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	itemsToRestore, err = fluctusClient.DeletionItemsGet(lastRecord.GenericFileIdentifier)
	if err != nil {
		t.Errorf("Error getting deletion items: %v", err)
	}
	if len(itemsToRestore) < 1 {
		t.Error("DeletionItemsGet returned no records when it should have returned something.")
	}

	// Make sure we get empty list and not error when there are no items
	lastRecord.Retry = false
	err = fluctusClient.SendProcessedItem(records[1])
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
	}

	itemsToRestore, err = fluctusClient.DeletionItemsGet(lastRecord.GenericFileIdentifier)
	if err != nil {
		t.Errorf("Error getting restoration items: %v", err)
	}
	if len(itemsToRestore) == 0 {
		t.Error("DeletionItemsGet returned no records when it should have returned something.")
	}
}


func TestRestorationStatusSet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)

	// Create a test record
	itemName := uuid.NewV4()
	record := &bagman.ProcessStatus{
		Id:          0,
		Name:        itemName.String(),
		ObjectIdentifier: fmt.Sprintf("test.edu/%s", itemName.String()),
		Bucket:      "aptrust.receiving.test.test.edu",
		ETag:        "0000000000",
		BagDate:     time.Now(),
		Institution: "test.edu",
		Date:        time.Now(),
		Note:        "Test item",
		Action:      "Restore",
		Stage:       "Requested",
		Status:      "Pending",
		Outcome:     "la de da",
		Retry:       true,
		Reviewed:    false,
	}

	err := fluctusClient.SendProcessedItem(record)
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
		return
	}
	if record.Id != 0 {
		t.Error("record.Id was reassigned when it should not have been")
	}

	// Let's see if these properties stick, while we're at it.
	record.State = "{ This should be a blob of JSON }"
	record.Node = "10.11.12.13"
	record.Pid = 31337
	record.NeedsAdminReview = true
	record.Retry = false

	// Now update the status on that record
	err = fluctusClient.RestorationStatusSet(record)
	if err != nil {
		t.Errorf("Error setting restoration status: %v", err)
		return
	}

	updatedRecords, err := fluctusClient.RestorationItemsGet(record.ObjectIdentifier)
	if err != nil {
		t.Errorf("Error getting restoration items: %v", err)
	}
	if len(updatedRecords) == 0 {
		t.Error("RestorationItemsGet should have returned an updated record.")
		return
	}

	if updatedRecords[0].Stage != bagman.StageRequested {
		t.Errorf("Stage should be '%s', but is '%s'", bagman.StageRequested, updatedRecords[0].Stage)
	}
	if updatedRecords[0].Status != bagman.StatusPending {
		t.Errorf("Status should be '%s', but is '%s'", bagman.StatusPending, updatedRecords[0].Status)
	}
	if updatedRecords[0].Retry != false {
		t.Error("Retry should be false, but is true")
	}
	if updatedRecords[0].Note != "Test item" {
		t.Errorf("Note should be 'Test item', but is '%s'", updatedRecords[0].Note)
	}

	if updatedRecords[0].State != "{ This should be a blob of JSON }" {
		t.Errorf("State should be '%s', but is '%s'", "{ This should be a blob of JSON }",
			updatedRecords[0].Status)
	}
	if updatedRecords[0].Node != "10.11.12.13" {
		t.Errorf("Node should be '10.11.12.13', but is '%s'", updatedRecords[0].Node)
	}
	if updatedRecords[0].Pid != 31337 {
		t.Errorf("Pid should be 31337, but is '%d'", updatedRecords[0].Pid)
	}
	if updatedRecords[0].NeedsAdminReview != true {
		t.Errorf("NeedsAdminReview should be true, but is %t", updatedRecords[0].NeedsAdminReview)
	}

}

func TestNewJsonRequest(t *testing.T) {
	fluctusClient := getClient(t)
	request, err := fluctusClient.NewJsonRequest("GET",
		"http://localhost/miami.edu%2Fmiami.asm_0530%2Fdata%2FMichael Carlbach Numbers(xlsx).mtf",
		nil)
	if err != nil {
		t.Error(err)
		return
	}
	// We're testing for two things here:
	// 1. That %2F remains %2F instead of being converted to slashes.
	// 2. That spaces are converted to %20
	// These are both necessary for the proper functioning of the
	// generic_files endpoint in Fluctus.
	expected := "http://localhost/miami.edu%2Fmiami.asm_0530%2Fdata%2FMichael%20Carlbach%20Numbers(xlsx).mtf"
	if request.URL.RequestURI() != expected {
		t.Errorf("Request URL expected '%s' but got '%s'", expected, request.URL)
	}
}

func TestProcessStatusSearch(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)
	psEmpty := &bagman.ProcessStatus {}
	results, err := fluctusClient.ProcessStatusSearch(psEmpty, false, false)
	if err != nil {
		t.Error(err)
		return
	}
	if len(results) == 0 {
		t.Error("ProcessStatusSearch returned no results (without filters)")
		return
	}
	processStatus := results[0]
	results, err = fluctusClient.ProcessStatusSearch(processStatus, true, true)
	if err != nil {
		t.Error(err)
		return
	}
	if len(results) == 0 {
		t.Error("ProcessStatusSearch returned no results (filtering on known item)")
		return
	}
}

func TestGenericFileSaveBatch(t *testing.T) {
	if runFluctusTests() == false {
		return
	}

	// Make sure our test object is in Fluctus
	err := loadTestResult(t)
	if err != nil {
		return
	}

	// Load processing result fixture
	testfile := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return
	}
	// Get the intellectual object from the processing result
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}

    fluctusClient := getClient(t)

	// Add some new GenericFiles
	timestamp := fmt.Sprintf("%d",time.Now().Unix())
	genericFiles := make([]*bagman.GenericFile, 2)
	genericFiles[0] = &bagman.GenericFile{
		Identifier: obj.GenericFiles[0].Identifier + timestamp,
		Format: obj.GenericFiles[0].Format,
		URI: obj.GenericFiles[0].URI + timestamp,
		Size: int64(1000),
		Created: time.Now(),
		Modified: time.Now(),
		ChecksumAttributes: obj.GenericFiles[0].ChecksumAttributes,
		Events: obj.GenericFiles[0].Events,
	}
	genericFiles[1] = &bagman.GenericFile{
		Identifier: obj.GenericFiles[1].Identifier + timestamp,
		Format: obj.GenericFiles[1].Format,
		URI: obj.GenericFiles[1].URI + timestamp,
		Size: int64(1000),
		Created: time.Now(),
		Modified: time.Now(),
		ChecksumAttributes: obj.GenericFiles[1].ChecksumAttributes,
		Events: obj.GenericFiles[1].Events,
	}

    // And throw in the old ones, so we're doing some creates
    // and some updates.
    genericFiles = append(genericFiles, obj.GenericFiles[0], obj.GenericFiles[1])
	err = fluctusClient.GenericFileSaveBatch(objId, genericFiles)
	if err != nil {
		t.Error(err)
	}
}

func TestGetFilesNotCheckedSince(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	fluctusClient := getClient(t)
	sinceWhen := time.Date(2028,1,1,12,0,0,0,time.UTC)
	files, err := fluctusClient.GetFilesNotCheckedSince(sinceWhen, 0, 10)
	if err != nil {
		t.Error(err)
	}
	if len(files) < 1 {
		t.Errorf("GetFilesNotCheckedSince should have returned at least one file")
		return
	}
	if files[0].ChecksumAttributes == nil || len(files[0].ChecksumAttributes) < 2 {
		t.Errorf("GenericFile records are missing checksums")
	}
}
