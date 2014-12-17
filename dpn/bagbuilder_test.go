package dpn_test

import (
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testBagPath() (string) {
	filePath, _ := filepath.Abs("test_bag")
	// We have to do this for the bagins bag,
	// even if we're not going to write to disk!
	os.MkdirAll(filePath, 0755)
	return filePath
}

func intelObj(t *testing.T) (*bagman.IntellectualObject) {
	filename := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	return obj
}

func createBagBuilder(t *testing.T, withGenericFiles bool) (builder *dpn.BagBuilder) {
	obj := intelObj(t)
	if obj != nil {
		if withGenericFiles {
			builder = dpn.NewBagBuilder(testBagPath(), obj, obj.GenericFiles)
		} else {
			builder = dpn.NewBagBuilder(testBagPath(), obj, nil)
		}
	} else {
		t.Errorf("Could not create bag builder.")
	}
	return builder
}

// Delete the test bag directory.
func tearDown() {
	testPath := testBagPath()
	// Be sure not to delete cwd!
	if strings.HasSuffix(testPath, "test_bag") {
		os.RemoveAll(testBagPath())
	}
}

func TestNewBagBuilder(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
}

func TestDPNBagit(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	tagfile := builder.DPNBagIt()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from DPNBagIt()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "bagit.txt") {
		t.Errorf("Wrong DPN bagit.txt file path: %s", tagfile.Name())
	}
	verifyTagField(t, tagfile, "BagIt-Version", "")
	verifyTagField(t, tagfile, "Tag-File-Character-Encoding", "")
}

func TestDPNBagInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestDPNInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestDPNManifestSha256(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestDPNTagManifest(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestAPTrustBagit(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestAPTrustBagInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestAPTrustInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestAPTrustManifestMd5(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestDataFiles(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestDataPath(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestAPTrustMetadataPath(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func TestBuildBag(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}

}

func verifyTagField(t *testing.T, tagfile *bagins.TagFile, label, value string) {
	for _, tagfield := range tagfile.Data.Fields() {
		if tagfield.Label() == label && tagfield.Value() != value {
			t.Errorf("In tag file '%s', for label '%s', expected '%s', but got '%s'",
				tagfile.Name(), label, value, tagfield.Value())
		}
	}
}
