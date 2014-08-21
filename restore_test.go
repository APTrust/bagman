package bagman_test

import (
	"github.com/APTrust/bagman"
	"path/filepath"
	"testing"
)

func TestRestore(t *testing.T) {
	// TODO: Don't run this test unless we have S3 credentials.
	// TODO: Fix other test file where we clobber filepath.
	testfile := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(testfile)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", testfile, err)
		return
	}
	outputDir := filepath.Join("testdata", "tmp")
	restorer, err := bagman.NewBagRestorer(obj, outputDir)
	if err != nil {
		t.Errorf("NewBagRestorer() returned an error: %v", err)
		return
	}
	err = restorer.Restore()
	if err != nil {
		t.Errorf("Restore() returned an error: %v", err)
	}
}
