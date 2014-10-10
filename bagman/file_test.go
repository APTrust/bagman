package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"testing"
	"time"
)

func loadGenericFile() (*bagman.File, error) {
	filepath := filepath.Join("testdata", "result_good.json")
	result, err := bagman.LoadResult(filepath)
	if err != nil {
		return nil, fmt.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	return result.TarResult.Files[0], nil
}


func TestToGenericFile(t *testing.T) {
	file, err := loadGenericFile()
	if err != nil {
		t.Error(err)
		return
	}
	genericFile, err := file.ToGenericFile()
	expectedIdentifier := "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml"
	if genericFile.Identifier != expectedIdentifier {
		t.Errorf("Identifier expected '%s', got '%s'", expectedIdentifier, genericFile.Identifier)
	}
	expectedFormat := "application/xml"
	if genericFile.Format != expectedFormat {
		t.Errorf("Format expected '%s', got '%s'", expectedFormat, genericFile.Format)
	}
	expectedURI := "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml"
	if genericFile.URI != expectedURI {
		t.Errorf("URI expected '%s', got '%s'", expectedURI, genericFile.URI)
	}
	expectedSize := int64(5105)
	if genericFile.Size != expectedSize {
		t.Errorf("Size expected %d, got %d", expectedSize, genericFile.Size)
	}
	expectedTime := time.Date(2014, 4, 25, 18, 05, 51, 0, time.UTC)
	if genericFile.Created != expectedTime {
		t.Errorf("Created expected '%v', got '%v'", expectedTime, genericFile.Created)
	}
	if genericFile.Modified != expectedTime {
		t.Errorf("Modified expected '%v', got '%v'", expectedTime, genericFile.Modified)
	}
	if len(genericFile.ChecksumAttributes) != 2 {
		t.Errorf("GenericFile should have 2 checksum attributes")
	}
	for i := range genericFile.ChecksumAttributes {
		cs := genericFile.ChecksumAttributes[i]
		if i == 0 {
			if cs.Algorithm != "md5" {
				t.Errorf("First algorithm should be md5")
			}
			if cs.DateTime != expectedTime {
				t.Errorf("Checksum DateTime should be %v", expectedTime)
			}
			if cs.Digest != "84586caa94ff719e93b802720501fcc7" {
				t.Errorf("Checksum Digest should be '84586caa94ff719e93b802720501fcc7'")
			}
		} else {
			if cs.Algorithm != "sha256" {
				t.Errorf("First algorithm should be sha256")
			}
			expectedShaTime := time.Date(2014, 6, 9, 14, 12, 45, 574358959, time.UTC)
			if cs.DateTime != expectedShaTime {
				t.Errorf("Checksum DateTime should be %v", expectedShaTime)
			}
			if cs.Digest != "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784" {
				t.Errorf("Checksum Digest should be 'ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784'")
			}
		}
	}
	// We'll test individual events below
	if len(genericFile.Events) != 5 {
		t.Errorf("PremisEvents should contain 5 events")
	}
}

func TestPremisEvents(t *testing.T) {
	file, err := loadGenericFile()
	if err != nil {
		t.Error(err)
		return
	}
	events, err := file.PremisEvents()
	if len(events) != 5 {
		t.Errorf("PremisEvents() should have returned 5 events")
		return
	}

	// Fixity check event
	event := events[0]
	if event.Identifier == "" {
		t.Errorf("Event.Identifier should not be empty")
	}
	if event.EventType != "fixity_check" {
		t.Errorf("Event.EventType expected 'fixity_check', got '%s'", event.EventType)
	}
	if event.DateTime != file.Md5Verified {
		t.Errorf("Event.DateTime expected '%v', got '%v'", file.Md5Verified, event.DateTime)
	}
	expectedDetail := "Fixity check against registered hash"
	if event.Detail != expectedDetail {
		t.Errorf("Event.Detail expected '%s', got '%s'", expectedDetail, event.Detail)
	}
	expectedOutcome := string(bagman.StatusSuccess)
	if event.Outcome != expectedOutcome {
		t.Errorf("Event.Outcome expected '%s', got '%s'", expectedOutcome, event.Outcome)
	}
	expectedOutcomeDetail := fmt.Sprintf("md5:%s", file.Md5)
	if event.OutcomeDetail != expectedOutcomeDetail {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", expectedOutcomeDetail, event.OutcomeDetail)
	}
	if event.Object != "Go crypto/md5" {
		t.Errorf("Event.Object expected 'Go crypto/md5', got '%s'", event.Object)
	}
	expectedAgent := "http://golang.org/pkg/crypto/md5/"
	if event.Agent != expectedAgent {
		t.Errorf("Event.Agent expected '%s', got '%s'", expectedAgent, event.Agent)
	}
	if event.OutcomeInformation != "Fixity matches" {
		t.Errorf("event.OutcomeInformation expected 'Fixity matches', got '%s'", event.OutcomeInformation)
	}

	// Copy to S3 event
	event = events[1]
	if event.EventType != "ingest" {
		t.Errorf("Event.EventType expected 'ingest', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.StorageMd5 {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.StorageMd5, event.OutcomeDetail)
	}

	// Sha256 fixity generation
	event = events[2]
	if event.EventType != "fixity_generation" {
		t.Errorf("Event.EventType expected 'fixity_generation', got '%s'", event.EventType)
	}
	expectedOutcomeDetail = fmt.Sprintf("sha256:%s", file.Sha256)
	if event.OutcomeDetail != expectedOutcomeDetail {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", expectedOutcomeDetail, event.OutcomeDetail)
	}

	// Identifier assignment (friendly identifier)
	event = events[3]
	if event.EventType != "identifier_assignment" {
		t.Errorf("Event.EventType expected 'identifier_assignment', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.Identifier {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.Identifier, event.OutcomeDetail)
	}

	// Identifier assignment (storage URL)
	event = events[4]
	if event.EventType != "identifier_assignment" {
		t.Errorf("Event.EventType expected 'identifier_assignment', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.StorageURL {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.StorageURL, event.OutcomeDetail)
	}

}
