package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
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

func loadConfig(t *testing.T, configPath string) (*dpn.DPNConfig) {
	if dpnConfig != nil {
		return dpnConfig
	}
	var err error
	dpnConfig, err = dpn.LoadConfig(configPath)
	if err != nil {
		t.Errorf("Error loading %s: %v\n", configPath, err)
		return nil
	}
	return dpnConfig
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
			builder = dpn.NewBagBuilder(testBagPath(), obj, config.DefaultMetadata)
		} else {
			builder = dpn.NewBagBuilder(testBagPath(), obj, config.DefaultMetadata)
		}
	} else {
		t.Errorf("Could not create bag builder.")
	}
	return builder
}

// Delete the test bag directory.
func tearDown() {
	os.RemoveAll(testBagPath())
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
	if manifest.Data["data/object.properties"] !=
		"8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/object.properties"],
			"data/object.properties",
			"8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7")
	}
	if manifest.Data["data/metadata.xml"] !=
		"a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/metadata.xml"],
			"data/metadata.xml",
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
		t.Errorf("Wrong aptrust-tags/bag-info.txt file path: %s", tagfile.Name())
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
	tagfile := builder.APTrustInfo()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if tagfile == nil {
		t.Errorf("Got unexpected nil from APTrustInfo()")
		return
	}
	if tagfile.Name() != filepath.Join(builder.LocalPath, "aptrust-tags", "aptrust-info.txt") {
		t.Errorf("Wrong aptrust-tags/aptrust-info.txt file path: %s", tagfile.Name())
	}
	verifyTagField(t, tagfile, "Title", builder.IntellectualObject.Title)
	verifyTagField(t, tagfile, "Description", builder.IntellectualObject.Description)
	verifyTagField(t, tagfile, "Access", builder.IntellectualObject.Access)
}

func TestAPTrustManifestMd5(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	manifest := builder.APTrustManifestMd5()
	if builder.ErrorMessage != "" {
		t.Errorf(builder.ErrorMessage)
	}
	if manifest == nil {
		t.Errorf("Got unexpected nil from APTrustManifestMd5()")
		return
	}
	if manifest.Name() != builder.APTrustMetadataPath("manifest-md5.txt") {
		t.Errorf("Wrong DPN aptrust-info/manifest-md5.txt file path: %s", manifest.Name())
	}
	if len(manifest.Data) != 2 {
		t.Errorf("Tag manifest should contain exactly 2 items, but it contains %s",
			len(manifest.Data))
	}
	if manifest.Data["data/object.properties"] !=
		"8d7b0e3a24fc899b1d92a73537401805" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/object.properties"],
			"data/object.properties",
			"8d7b0e3a24fc899b1d92a73537401805")
 	}
	if manifest.Data["data/metadata.xml"] !=
		"c6d8080a39a0622f299750e13aa9c200" {
		t.Errorf("Got checksum %s for file %s. Expected checksum %s.",
			manifest.Data["data/metadata.xml"],
			"data/metadata.xml",
			"c6d8080a39a0622f299750e13aa9c200")
	}
}

func TestDataFiles(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	dataFiles := builder.DataFiles()
	if len(dataFiles) != 2 {
		t.Errorf("DataFiles() should have returned 2 files; got %d", len(dataFiles))
	}

	urlPrefix := "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/"
	filePath0 := "data/object.properties"
	if dataFiles[0].ExternalPathType != "S3 Bucket" {
		t.Errorf("ExternalPathType '%s' is incorrect", dataFiles[0].ExternalPathType)
	}
	if dataFiles[0].ExternalPath != urlPrefix + filePath0 {
		t.Errorf("ExternalPath '%s' is incorrect", dataFiles[0].ExternalPath)
	}
	if dataFiles[0].PathInBag != filePath0 {
		t.Errorf("PathInBag '%s' is incorrect", dataFiles[0].PathInBag)
	}

	filePath1 := "data/metadata.xml"
	if dataFiles[1].ExternalPathType != "S3 Bucket" {
		t.Errorf("ExternalPathType '%s' is incorrect", dataFiles[1].ExternalPathType)
	}
	if dataFiles[1].ExternalPath != urlPrefix + filePath1 {
		t.Errorf("ExternalPath '%s' is incorrect", dataFiles[1].ExternalPath)
	}
	if dataFiles[1].PathInBag != filePath1 {
		t.Errorf("PathInBag '%s' is incorrect", dataFiles[1].PathInBag)
	}

}

func TestDataPath(t *testing.T) {
	origPath := "virginia.edu/my_bag/data/my_file.txt"
	expected := "data/my_file.txt"
	if dpn.DataPath(origPath) != expected {
		t.Errorf("Got data path %s, expected %s",
			dpn.DataPath(origPath), expected)
	}
}

func TestAPTrustMetadataPath(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	origPath := "special-tag-file.txt"
	expected := filepath.Join(testBagPath(), "aptrust-tags", origPath)
	if builder.APTrustMetadataPath(origPath) != expected {
		t.Errorf("APTrustMetadataPath returned %s, expected %s",
			builder.APTrustMetadataPath(origPath), expected)
	}
}

func TestBuildBag(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	bag, err := builder.BuildBag()
	if err != nil {
		t.Errorf("BuildBag() returned error: %v", err)
	}
	if bag == nil {
		t.Error("BuildBag() returned nil")
	}
	if len(bag.DataFiles) != 2 {
		t.Errorf("Bag should have 2 data files, but it has %d", len(bag.DataFiles))
	}
	if bag.APTrustManifestMd5 == nil {
		t.Error("APTrustManifestMd5 is missing")
	}
	if bag.APTrustBagIt == nil {
		t.Error("APTrustBagIt is missing")
	}
	if bag.APTrustBagInfo == nil {
		t.Error("APTrustBagInfo is missing")
	}
	if bag.APTrustInfo == nil {
		t.Error("APTrustInfo is missing")
	}
	if bag.DPNBagIt == nil {
		t.Error("DPNBagIt is missing")
	}
	if bag.DPNBagInfo == nil {
		t.Error("DPNBagInfo is missing")
	}
	if bag.DPNInfo == nil {
		t.Error("DPNInfo is missing")
	}
	if bag.DPNManifestSha256 == nil {
		t.Error("DPNManifestSha256 is missing")
	}
	if bag.DPNTagManifest == nil {
		t.Error("DPNTagManifest is missing")
	}
}

func TestBagWrite(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
	defer os.RemoveAll(builder.LocalPath)
	bag, err := builder.BuildBag()
	if err != nil {
		t.Errorf("BuildBag() returned error: %v", err)
	}
	errors := bag.Write()
	if errors != nil && len(errors) > 0 {
		t.Errorf("Write() returned errors: %s", strings.Join(errors, "\n"))
		return
	}

	//dumpBagFiles(bag)

	verifyFile(t, bag.DPNManifestSha256.Name())
	verifyFile(t, bag.DPNTagManifest.Name())
	verifyFile(t, bag.APTrustManifestMd5.Name())
	verifyFile(t, bag.DPNBagIt.Name())
	verifyFile(t, bag.DPNBagInfo.Name())
	verifyFile(t, bag.DPNInfo.Name())
	verifyFile(t, bag.APTrustBagIt.Name())
	verifyFile(t, bag.APTrustBagInfo.Name())
	verifyFile(t, bag.APTrustInfo.Name())

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

// Dumps the path and contents of the bag's manifest and tag files
// to STDOUT.
func dumpBagFiles(bag *dpn.Bag) {
	str := bag.DPNManifestSha256.ToString()
	fmt.Println(bag.DPNManifestSha256.Name(), str)

	str = bag.DPNTagManifest.ToString()
	fmt.Println(bag.DPNTagManifest.Name(), str)

	str = bag.APTrustManifestMd5.ToString()
	fmt.Println(bag.APTrustManifestMd5.Name(), str)

	str, _ = bag.DPNBagIt.ToString()
	fmt.Println(bag.DPNBagIt.Name(), str)

	str, _ = bag.DPNBagInfo.ToString()
	fmt.Println(bag.DPNBagInfo.Name(), str)

	str, _ = bag.DPNInfo.ToString()
	fmt.Println(bag.DPNInfo.Name(), str)

	str, _ =  bag.APTrustBagIt.ToString()
	fmt.Println(bag.APTrustBagIt.Name(), str)

	str, _ = bag.APTrustBagInfo.ToString()
	fmt.Println(bag.APTrustBagInfo.Name(), str)

	str, _ = bag.APTrustInfo.ToString()
	fmt.Println(bag.APTrustInfo.Name(), str)
}
