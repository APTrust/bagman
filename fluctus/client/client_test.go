package client_test

import (
    "testing"
	"fmt"
    "log"
	"os"
	"io/ioutil"
	"net/http"
	"time"
    "github.com/APTrust/bagman/fluctus/client"
)

var fluctusUrl string = "http://localhost:3000"
var objId string = "changeme:28082"
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
		for _, gf := range obj.GenericFiles {
			if len(gf.Events) == 0 {
				t.Error("GenericFile from Fluctus is missing events.")
			}
			if len(gf.ChecksumAttributes) == 0 {
				t.Error("GenericFile from Fluctus is missing checksums.")
			}
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
	}

	// Update an existing object
	newObj, err := client.IntellectualObjectSave(obj)
	if err != nil {
        t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
    }
	if newObj.Id != obj.Id || newObj.Title != obj.Title ||
		newObj.Description != obj.Description {
		t.Error("New object attributes don't match what was submitted.")
	}

	// Save a new object... just change the id, so Fluctus thinks it's new
	obj.Id = fmt.Sprintf("test:%d", time.Now().Unix())
	newObj, err = client.IntellectualObjectSave(obj)
	if err != nil {
        t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
    }
	if newObj.Id != obj.Id || newObj.Title != obj.Title || newObj.Description != obj.Description {
		t.Error("New object attributes don't match what was submitted.")
	}
}
