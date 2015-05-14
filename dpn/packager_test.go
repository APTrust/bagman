package dpn_test

import (
	"github.com/APTrust/bagman/dpn"
	"testing"
)

func TestPathWithinArchive(t *testing.T) {
	result := dpn.NewDPNResult("test.edu/ncsu.1840.16-1004")
	path, err := dpn.PathWithinArchive(
		result,
		"/mnt/apt/staging/test.edu/ncsu.1840.16-1004/data/subdir/file1.pdf",
		"/mnt/apt/staging/test.edu/ncsu.1840.16-1004")
	if err != nil {
		t.Errorf("PathWithinArchive returned an unexpected error: %v", err)
	}
	if path != "ncsu.1840.16-1004/data/subdir/file1.pdf" {
		t.Errorf("OriginalBagName returned '%s', " +
			"expected 'ncsu.1840.16-1004/data/subdir/file1.pdf'", path)
	}

	result = dpn.NewDPNResult("invalid-bag-name")
	path, err = dpn.PathWithinArchive(
		result,
		"/mnt/apt/staging/invalid-bag-name/data/subdir/file1.pdf",
		"/mnt/apt/staging/invalid-bag-name")

	if err == nil {
		t.Errorf("PathWithinBag did not return expected error for invalid bag name")
	}
}
