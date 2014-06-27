package client_test

import (
    "testing"
	"fmt"
    "log"
	"os"
	"io/ioutil"
	"net/http"
//	"encoding/json"
	"time"
	"github.com/nu7hatch/gouuid"
	"github.com/APTrust/bagman/fluctus/client"
    "github.com/APTrust/bagman/fluctus/models"
)

// TODO: Fix tests so they don't depend on these hard-coded ids!
var fluctusUrl string = "http://localhost:3000"
var objId string = "changeme:29323"
var gfId string = "changeme:29324"
var skipMessagePrinted bool = false

func runFluctusTests() (bool) {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		if skipMessagePrinted == false {
			skipMessagePrinted = true
			fmt.Printf("Skipping fluctus integration tests: " +
				"fluctus server is not running at %s\n", fluctusUrl)
		}
		return false
	}
	return true
}

func getClient(t *testing.T) (*client.Client) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	logger := log.New(ioutil.Discard, "", 0)
	client, err := client.New(fluctusUrl,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		logger)
	if err != nil {
        t.Errorf("Error constructing fluctus client: %v", err)
    }
	return client
}


func TestIntellectualObjectGet(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	client := getClient(t)

	// Get the lightweight version of an existing object
	obj, err := client.IntellectualObjectGet(objId, false)
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
	obj, err = client.IntellectualObjectGet(objId, true)
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
	obj, err = client.IntellectualObjectGet("changeme:99999", false)
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
func findFile(files []*models.GenericFile, id string) (*models.GenericFile) {
	for _, f := range(files) {
		if f.Id == id {
			return f
		}
	}
	return nil
}

func TestIntellectualObjectSave(t *testing.T) {
	if runFluctusTests() == false {
		return
	}
	client := getClient(t)
	obj, err := client.IntellectualObjectGet(objId, false)
	if err != nil {
        t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
    }
	if obj == nil {
        t.Error("IntellectualObjectGet did not return the expected object")
		return // Can't finish remaining tests
	}

	// Update an existing object
	newObj, err := client.IntellectualObjectSave(obj)
	if err != nil {
        t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
    }
	if newObj == nil {
		t.Error("New object should be an object, but it's nil.")
	} else if newObj.Id != obj.Id || newObj.Title != obj.Title ||
		newObj.Description != obj.Description {
		t.Error("New object attributes don't match what was submitted.")
	}

	// Save a new object... just change the id, so Fluctus thinks it's new
	obj.Id = fmt.Sprintf("test:%d", time.Now().Unix())
	newObj, err = client.IntellectualObjectSave(obj)
	if err != nil {
        t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
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
	client := getClient(t)

	// Get the lightweight version of an existing object
	gf, err := client.GenericFileGet(gfId, false)
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
	gf, err = client.GenericFileGet(gfId, true)
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
	gf, err = client.GenericFileGet("changeme:99999", false)
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
	client := getClient(t)
	gf, err := client.GenericFileGet(gfId, true)
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
	newGf, err := client.GenericFileSave(objId, gf)
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
	newGf, err = client.GenericFileSave(objId, gf)
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
	client := getClient(t)

	eventId, err := uuid.NewV4()
	if err != nil {
		t.Errorf("Error generating UUID: %v", err)
	}
	ingestEvent := &models.PremisEvent{
		Identifier: eventId.String(),
		EventType: "Ingest",
		DateTime: time.Now(),
		Detail: "Completed copy to perservation bucket",
		Outcome: "Success",
		OutcomeDetail: "md5: 000000001234567890",
		Object: "goamz S3 client",
		Agent: "https://launchpad.net/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}

	// Make sure we can save an IntellectualObject event
	obj, err := client.PremisEventSave(objId, "IntellectualObject", ingestEvent)
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
	identifierEvent := &models.PremisEvent{
		Identifier: eventId.String(),
		EventType: "identifier_assignment",
		DateTime: time.Now(),
		Detail: "S3 key generated for file",
		Outcome: "Success",
		OutcomeDetail: "00000000-0000-0000-0000-000000000000",
		Object: "GoUUID",
		Agent: "https://github.com/nu7hatch/gouuid",
		OutcomeInformation: "Generated with uuid.NewV4()",
	}

	// Make sure we can save an IntellectualObject event
	obj, err = client.PremisEventSave(gfId, "GenericFile", identifierEvent)
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
	client := getClient(t)
	err := client.CacheInstitutions()
	if err != nil {
		t.Errorf("Error caching institutions: %v", err)
	}
}
