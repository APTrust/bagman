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

const CONFIG_FILE = "dpn/bagbuilder_config.json"
var defaultMetadata *dpn.DefaultMetadata

func testBagPath() (string) {
	filePath, _ := filepath.Abs("test_bag")
	return filePath
}

func loadConfig(t *testing.T, configPath string) (*dpn.DefaultMetadata) {
	if defaultMetadata != nil {
		return defaultMetadata
	}
	var err error
	defaultMetadata, err = dpn.LoadConfig(configPath)
	if err != nil {
		t.Errorf("Error loading %s: %v\n", configPath, err)
		return nil
	}
	return defaultMetadata
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
	config := loadConfig(t, CONFIG_FILE)
	if obj != nil && config != nil {
		if withGenericFiles {
			builder = dpn.NewBagBuilder(testBagPath(), obj, obj.GenericFiles, config)
		} else {
			builder = dpn.NewBagBuilder(testBagPath(), obj, nil, config)
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
	verifyTagField(t, tagfile, "BagIt-Version", "0.97")
	verifyTagField(t, tagfile, "Tag-File-Character-Encoding", "UTF-8")
}

func TestDPNBagInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	tagfile := builder.DPNBagInfo()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from DPNBagInfo()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "bag-info.txt") {
		t.Errorf("Wrong DPN bag-info.txt file path: %s", tagfile.Name())
	}

	verifyTagField(t, tagfile, "Source-Organization", "uc.edu")
	verifyTagField(t, tagfile, "Organization-Address", "")
	verifyTagField(t, tagfile, "Contact-Name", "")
	verifyTagField(t, tagfile, "Contact-Phone", "")
	verifyTagField(t, tagfile, "Contact-Email", "")
	verifyTagField(t, tagfile, "Bagging-Date", builder.BagTime())
	verifyTagField(t, tagfile, "Bag-Size", "686")
	verifyTagField(t, tagfile, "Bag-Group-Identifier", "")
	verifyTagField(t, tagfile, "Bag-Count", "1")
}

func TestDPNInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	tagfile := builder.DPNInfo()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from DPNInfo()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "dpn-tags","dpn-info.txt") {
		t.Errorf("Wrong DPN dpn-info.txt file path: %s", tagfile.Name())
	}

	verifyTagField(t, tagfile, "DPN-Object-ID", builder.UUID)
	verifyTagField(t, tagfile, "Local-ID", "uc.edu/cin.675812")
	verifyTagField(t, tagfile, "First-Node-Name", "APTrust")
	verifyTagField(t, tagfile, "First-Node-Address", "160 McCormick Rd., Charlottesville, VA 22904")
	verifyTagField(t, tagfile, "First-Node-Contact-Name", "APTrust Administrator")
	verifyTagField(t, tagfile, "First-Node-Contact-Email", "help@aptrust.org")
	verifyTagField(t, tagfile, "Version-Number", "1")
	verifyTagField(t, tagfile, "Previous-Version-Object-ID", "")
	verifyTagField(t, tagfile, "Brightening-Object-ID", "")
	verifyTagField(t, tagfile, "Rights-Object-ID", "")
	verifyTagField(t, tagfile, "Object-Type", dpn.BAG_TYPE_DATA)
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
