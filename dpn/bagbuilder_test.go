package dpn_test

import (
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

const CONFIG_FILE = "dpn/dpn_config.json"
var dpnConfig *dpn.DPNConfig
var _testPath string

func testBagPath() (string) {
	if _testPath == "" {
		_testPath, _ = ioutil.TempDir("", "dpn")
	}
	return _testPath
}

func intelObj(t *testing.T) (*bagman.IntellectualObject) {
	filename := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	return obj
}

func createBagBuilder(t *testing.T) (builder *dpn.BagBuilder) {
	obj := intelObj(t)
	config := loadConfig(t, CONFIG_FILE)
	builder, err := dpn.NewBagBuilder(testBagPath(), obj, config.DefaultMetadata)
	if err != nil {
		tearDown()
		t.Errorf("Could not create bag builder: %s", err.Error())
		return nil
	}
	builder.Bag.Save()
	return builder
}

// Delete the test bag directory.
func tearDown() {
	os.RemoveAll(testBagPath())
}

func TestNewBagBuilder(t *testing.T) {
	builder := createBagBuilder(t)
	defer tearDown()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
}

func TestDPNBagit(t *testing.T) {
	builder := createBagBuilder(t)
	defer tearDown()
	if builder == nil {
		return
	}
	tagfile, err := builder.Bag.TagFile("bagit.txt")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
		return
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from DPNBagIt()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "bagit.txt") {
		t.Errorf("Wrong DPN bagit.txt file path: Expected %s, got %s",
			filepath.Join(builder.LocalPath, "bagit.txt"), tagfile.Name())
	}
	verifyTagField(t, tagfile, "BagIt-Version", "0.97")
	verifyTagField(t, tagfile, "Tag-File-Character-Encoding", "UTF-8")
}

func TestDPNBagInfo(t *testing.T) {
	builder := createBagBuilder(t)
	defer tearDown()
	if builder == nil {
		return
	}
	tagfile, err := builder.Bag.TagFile("bag-info.txt")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
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
	builder := createBagBuilder(t)
	defer tearDown()
	if builder == nil {
		return
	}
	tagfile, err := builder.Bag.TagFile("dpn-tags/dpn-info.txt")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
		return
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
	verifyTagField(t, tagfile, "Interpretive-Object-ID", "")
	verifyTagField(t, tagfile, "Rights-Object-ID", "")
	verifyTagField(t, tagfile, "Object-Type", dpn.BAG_TYPE_DATA)
}


func TestAPTrustBagit(t *testing.T) {
	builder := createBagBuilder(t)
	defer tearDown()
	if builder == nil {
		return
	}
	tagfile, err := builder.Bag.TagFile("aptrust-tags/bagit.txt")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
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

func verifyFile(t *testing.T, filePath string) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Errorf("Can't stat %s: %v", filePath, err)
	}
	if fileInfo.Size() == 0 {
		t.Errorf("File %s exists but is empty", filePath)
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
