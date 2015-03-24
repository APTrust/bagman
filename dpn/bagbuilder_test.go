package dpn_test

import (
//	"fmt"
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
	manifest := builder.DPNManifestSha256()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if manifest == nil {
		t.Errorf("Got unexpected nil from DPNManifestSha256()")
		return
	}
	if manifest.Name() != filepath.Join(builder.LocalPath, "manifest-sha256.txt") {
		t.Errorf("Wrong DPN manifest-sha256.txt file path: %s", manifest.Name())
	}
	if len(manifest.Data) != 2 {
		t.Errorf("Manifest should contain exactly 2 items, but it contains %s",
			len(manifest.Data))
	}
	if manifest.Data["data/uc.edu/cin.675812/data/object.properties"] !=
		"8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/uc.edu/cin.675812/data/object.properties"],
			"data/uc.edu/cin.675812/data/object.properties",
			"8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7")
	}
	if manifest.Data["data/uc.edu/cin.675812/data/metadata.xml"] !=
		"a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/uc.edu/cin.675812/data/metadata.xml"],
			"data/uc.edu/cin.675812/data/metadata.xml",
			"a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790")
	}
}

func TestDPNTagManifest(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	manifest := builder.DPNTagManifest()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if manifest == nil {
		t.Errorf("Got unexpected nil from DPNTagManifest()")
		return
	}
	if manifest.Name() != filepath.Join(builder.LocalPath, "tagmanifest-sha256.txt") {
		t.Errorf("Wrong DPN tagmanifest-sha256.txt file path: %s", manifest.Name())
	}
	if len(manifest.Data) != 3 {
		t.Errorf("Tag manifest should contain exactly 3 items, but it contains %s",
			len(manifest.Data))
	}
	if manifest.Data["bagit.txt"] !=
		"49b477e8662d591f49fce44ca5fc7bfe76c5a71f69c85c8d91952a538393e5f4" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["bagit.txt"],
			"bagit.txt",
			"49b477e8662d591f49fce44ca5fc7bfe76c5a71f69c85c8d91952a538393e5f4")
	}
	if manifest.Data["bag-info.txt"] !=
		"9c64f25c14313d4c6c9608dc0aa0457a610539e51c76856234783864030c6529" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["bag-info.txt"],
			"bag-info.txt",
			"9c64f25c14313d4c6c9608dc0aa0457a610539e51c76856234783864030c6529")
	}
	// This checksum changes every time we run the tests because the
	// dpn-info.txt file includes the randomly-generated DPN bag UUID.
	if len(manifest.Data["dpn-tags/dpn-info.txt"]) != 64 {
		t.Errorf("Tag manifest is missing checksum for file dpn-tags/dpn-info.txt")
	}
}

func TestAPTrustBagit(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	tagfile := builder.APTrustBagIt()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from APTrustBagIt()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "aptrust-tags", "bagit.txt") {
		t.Errorf("Wrong aptrust-tags/bagit.txt file path: %s", tagfile.Name())
	}
	verifyTagField(t, tagfile, "BagIt-Version", "0.97")
	verifyTagField(t, tagfile, "Tag-File-Character-Encoding", "UTF-8")
}

func TestAPTrustBagInfo(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	tagfile := builder.APTrustBagInfo()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from APTrustBagInfo()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "aptrust-tags", "bag-info.txt") {
		t.Errorf("Wrong aptrust-tags/bagit.txt file path: %s", tagfile.Name())
	}
	verifyTagField(t, tagfile, "Source-Organization", builder.IntellectualObject.InstitutionId)
	verifyTagField(t, tagfile, "Bagging-Date", builder.BagTime())
	verifyTagField(t, tagfile, "Bag-Count", "1")
	verifyTagField(t, tagfile, "Internal-Sender-Description", builder.IntellectualObject.Description)
	verifyTagField(t, tagfile, "Internal-Sender-Identifier", builder.IntellectualObject.Identifier)
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
