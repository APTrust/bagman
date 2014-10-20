package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
	"time"
)

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

func TestIsStoring(t *testing.T) {
	ps := bagman.ProcessStatus{
		Action: "Ingest",
		Stage: "Store",
		Status: "Started",
	}
	if ps.IsStoring() == false {
		t.Error("IsStoring() should have returned true")
	}
	ps.Status = "Pending"
	if ps.IsStoring() == true {
		t.Error("IsStoring() should have returned false")
	}
	ps.Status = "Started"
	ps.Stage = "Record"
	if ps.IsStoring() == true {
		t.Error("IsStoring() should have returned false")
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

func getSomeStatus(action bagman.ActionType) ([]*bagman.ProcessStatus) {
	statusRecords := make([]*bagman.ProcessStatus, 3)
	statusRecords[0] = &bagman.ProcessStatus{
		Action: action,
		Stage: "Resolve",
		Status: bagman.StatusSuccess,
	}
	statusRecords[1] = &bagman.ProcessStatus{
		Action: action,
		Stage: "Resolve",
		Status: bagman.StatusFailed,
	}
	statusRecords[2] = &bagman.ProcessStatus{
		Action: action,
		Stage: "Requested",
		Status: bagman.StatusPending,
	}
	return statusRecords
}

func TestHasPendingDeleteRequest(t *testing.T) {
	statusRecords := getSomeStatus(bagman.ActionDelete)
	if bagman.HasPendingDeleteRequest(statusRecords) == false {
		t.Error("HasPendingDeleteRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusStarted
	if bagman.HasPendingDeleteRequest(statusRecords) == false {
		t.Error("HasPendingDeleteRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusCancelled
	if bagman.HasPendingDeleteRequest(statusRecords) == true {
		t.Error("HasPendingDeleteRequest() should have returned false")
	}
}

func TestHasPendingRestoreRequest(t *testing.T) {
	statusRecords := getSomeStatus(bagman.ActionRestore)
	if bagman.HasPendingRestoreRequest(statusRecords) == false {
		t.Error("HasPendingRestoreRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusStarted
	if bagman.HasPendingRestoreRequest(statusRecords) == false {
		t.Error("HasPendingRestoreRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusCancelled
	if bagman.HasPendingRestoreRequest(statusRecords) == true {
		t.Error("HasPendingRestoreRequest() should have returned false")
	}
}

func TestHasPendingIngestRequest(t *testing.T) {
	statusRecords := getSomeStatus(bagman.ActionIngest)
	if bagman.HasPendingIngestRequest(statusRecords) == false {
		t.Error("HasPendingIngestRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusStarted
	if bagman.HasPendingIngestRequest(statusRecords) == false {
		t.Error("HasPendingIngestRequest() should have returned true")
	}
	statusRecords[2].Status = bagman.StatusCancelled
	if bagman.HasPendingIngestRequest(statusRecords) == true {
		t.Error("HasPendingIngestRequest() should have returned false")
	}
}
