package dpn_test

import (
	"github.com/APTrust/bagman/dpn"
	"testing"
)

func TestOriginalBagName(t *testing.T) {
	result := dpn.NewDPNResult("test.edu/ncsu.1840.16-1004")
	bagName, err := result.OriginalBagName()
	if err != nil {
		t.Errorf("OriginalBagName returned an unexpected error: %v", err)
	}
	if bagName != "ncsu.1840.16-1004" {
		t.Errorf("OriginalBagName returned '%s', expected 'ncsu.1840.16-1004'", bagName)
	}

	result = dpn.NewDPNResult("invalid-bag-name-is-missing-slash")
	bagName, err = result.OriginalBagName()
	if err == nil {
		t.Errorf("OriginalBagName did not return expected error for invalid bag name")
	}
}

func TestTarFilePath(t *testing.T) {
	result := dpn.NewDPNResult("test.edu/ncsu.1840.16-1004")
	result.PackageResult = &dpn.PackageResult{
		TarFilePath: "/path/to/packaged_file.tar",
	}
	if result.TarFilePath() != "/path/to/packaged_file.tar" {
		t.Errorf("TarFilePath() returned %s, expected '/path/to/packaged_file.tar'",
			result.TarFilePath())
	}
	result.PackageResult = nil
	result.CopyResult = &dpn.CopyResult{
		LocalPath: "/path/to/copied_file.tar",
	}
	if result.TarFilePath() != "/path/to/copied_file.tar" {
		t.Errorf("TarFilePath() returned %s, expected '/path/to/copied_file.tar'",
			result.TarFilePath())
	}
	result.CopyResult = nil
	if result.TarFilePath() != "" {
		t.Errorf("TarFilePath() returned %s, expected empty string",
			result.TarFilePath())
	}
}
