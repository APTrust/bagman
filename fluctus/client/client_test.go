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

func runFluctusTests() (bool) {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

func TestIntellectualObjectExists(t *testing.T) {
	if runFluctusTests() == false {
		fmt.Println("Skipping fluctus integration tests: local fluctus server not found.")
		return
	}

	logger := log.New(os.Stdout, "", 0)
	client, err := client.New(fluctusUrl,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		logger)
	if err != nil {
        t.Errorf("Error constructing fluctus client: %v", err)
    }

	exists, err := client.IntellectualObjectExists("changeme:28082")
	if err != nil {
        t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
    }
	if exists == false {
        t.Error("Object should exist, but IntellectualObjectExists returned false.")
	}

	exists, err = client.IntellectualObjectExists("changeme:99999")
	if err != nil {
        t.Errorf("Error asking fluctus for IntellectualObject: %v", err)
    }
	if exists == true {
        t.Error("Object should not exist, but IntellectualObjectExists returned true.")
	}

}
