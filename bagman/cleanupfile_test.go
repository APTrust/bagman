package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
	"time"
)

func TestDeleteAttempted(t *testing.T) {
	cf := bagman.CleanupFile {
		BucketName: "charley",
		Key: "horse",
		ErrorMessage: "",
	}
	if cf.DeleteAttempted() == true {
		t.Errorf("DeleteAttempted() should have returned false")
	}
	cf.ErrorMessage = "Oopsie!"
	if cf.DeleteAttempted() == false {
		t.Errorf("DeleteAttempted() should have returned true")
	}
	cf.ErrorMessage = ""
	cf.DeletedAt = time.Now()
	if cf.DeleteAttempted() == false {
		t.Errorf("DeleteAttempted() should have returned true")
	}
}
