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

func TestTokenFormatStringFor(t *testing.T) {
	config := &dpn.DPNConfig{}
	format := config.TokenFormatStringFor("mickey")
	if format != dpn.DEFAULT_TOKEN_FORMAT_STRING {
		t.Errorf("TokenFormatStringFor(): Expected %s, got %s",
			dpn.DEFAULT_TOKEN_FORMAT_STRING, format)
	}

	customFormat := "Token smokin=%s"
	formatMap := make(map[string]string, 0)
	formatMap["minnie"] = customFormat
	config.AuthTokenHeaderFormats = formatMap
	format = config.TokenFormatStringFor("minnie")
	if format != customFormat {
		t.Errorf("TokenFormatStringFor(): Expected %s, got %s",
			customFormat, format)
	}
}
