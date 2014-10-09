package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
)

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
