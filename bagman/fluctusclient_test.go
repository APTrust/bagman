// Integration tests for Fluctus client.
// Requires a running Fluctus server.
package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nu7hatch/gouuid"
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

func runFluctusTests() bool {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		if fluctusSkipMessagePrinted == false {
			fluctusSkipMessagePrinted = true
			fmt.Printf("Skipping fluctus integration tests: "+
				"fluctus server is not running at %s\n", fluctusUrl)
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

	eventId, err := uuid.NewV4()
	if err != nil {
		t.Errorf("Error generating UUID: %v", err)
	}
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

	eventId, err = uuid.NewV4()
	if err != nil {
		t.Errorf("Error generating UUID: %v", err)
	}
	identifierEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "identifier_assignment",
		DateTime:           time.Now(),
		Detail:             "S3 key generated for file",
		Outcome:            string(bagman.StatusSuccess),
		OutcomeDetail:      "00000000-0000-0000-0000-000000000000",
		Object:             "GoUUID",
		Agent:              "https://github.com/nu7hatch/gouuid",
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
	itemName, err := uuid.NewV4()
	if err != nil {
		t.Errorf("Error generating UUID: %v", err)
	}
	status := &bagman.ProcessStatus{
		Id:          0,
		Name:        itemName.String(),
		ObjectIdentifier:  fmt.Sprintf("test.edu/%s", itemName.String()),
		Bucket:      "aptrust.receiving.test.edu",
		ETag:        "0000000000",
		BagDate:     time.Now(),
		Institution: "test.edu",
		Date:        time.Now(),
		Note:        "Test item",
		Action:      "Ingest",
		Stage:       "Receive",
		Status:      "Pending",
		Outcome:     "O-diddly Kay!",
		Retry:       true,
		Reviewed:    false,
	}

	// Create new records
	err = fluctusClient.SendProcessedItem(status)
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
	itemName, err := uuid.NewV4()
	if err != nil {
		t.Errorf("Error generating UUID: %v", err)
	}
	record := &bagman.ProcessStatus{
		Id:          0,
		Name:        itemName.String(),
		ObjectIdentifier: fmt.Sprintf("test.edu/%s", itemName.String()),
		Bucket:      "aptrust.receiving.test.edu",
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

	err = fluctusClient.SendProcessedItem(record)
	if err != nil {
		t.Errorf("Error sending processed item: %v", err)
		return
	}
	if record.Id != 0 {
		t.Error("record.Id was reassigned when it should not have been")
	}

	// Now update the status on that record
	err = fluctusClient.RestorationStatusSet(record.ObjectIdentifier, bagman.StageFetch,
		bagman.StatusStarted, "Updated note", false)
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

	if updatedRecords[0].Stage != bagman.StageFetch {
		t.Errorf("Stage should be '%s', but is '%s'", bagman.StageFetch, updatedRecords[0].Stage)
	}
	if updatedRecords[0].Status != bagman.StatusStarted {
		t.Errorf("Status should be '%s', but is '%s'", bagman.StatusStarted, updatedRecords[0].Status)
	}
	if updatedRecords[0].Retry != false {
		t.Error("Retry should be false, but is true")
	}
	if updatedRecords[0].Note != "Updated note" {
		t.Errorf("Note should be 'Updated note', but is '%s'", updatedRecords[0].Note)
	}
}