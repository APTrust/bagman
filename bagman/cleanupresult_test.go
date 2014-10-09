package bagman_test

import (
	"encoding/json"
	"github.com/APTrust/bagman/bagman"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func TestDeleteAttemptedAndSucceeded(t *testing.T) {
	filepath := filepath.Join("..", "testdata", "cleanup_result.json")
	var result bagman.CleanupResult
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}

	if result.Succeeded() == false {
		t.Error("result.Succeeded() should have returned true")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = "Spongebob"
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = ""
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == true {
			t.Error("file.DeleteAttempted() should have returned false")
		}
	}
}