package client_test

import (
    "testing"
	"fmt"
    "log"
	"os"
	"net/http"
//    "github.com/APTrust/bagman"
    "github.com/APTrust/bagman/fluctus/client"
)

var fluctusUrl string = "http://localhost:3000"
var objId string = "changeme:28082"

func runFluctusTests() (bool) {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		fmt.Printf("Skipping fluctus integration tests: " +
			"fluctus server is not running at %s", fluctusUrl)
		return false
	}
	return true
}

func getClient(t *testing.T) (*client.Client) {
	logger := log.New(os.Stdout, "", 0)
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
	obj, err := client.IntellectualObjectGet(objId)
	if err != nil {
        t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
    }
	if obj == nil {
        t.Error("IntellectualObjectGet did not return the expected object")
	}

	obj, err = client.IntellectualObjectGet("changeme:99999")
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
	obj, err := client.IntellectualObjectGet(objId)
	if err != nil {
        t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
    }
	if obj == nil {
        t.Error("IntellectualObjectGet did not return the expected object")
	}

	newObj, err := client.IntellectualObjectSave(obj)
	if err != nil {
        t.Errorf("Error saving IntellectualObject to fluctus: %v", err)
    }
	fmt.Println(obj)
	fmt.Println(newObj)
}
