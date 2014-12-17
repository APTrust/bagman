package dpn_test

import (
	//"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"os"
	"path/filepath"
	"testing"
)

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
			builder = dpn.NewBagBuilder("test_bag", obj, obj.GenericFiles)
		} else {
			builder = dpn.NewBagBuilder("test_bag", obj, nil)
		}
	} else {
		t.Errorf("Could not create bag builder.")
	}
	return builder
}

func tearDown() {
	os.RemoveAll("test_bag")
}

func TestNewBagBuilder(t *testing.T) {
	_ = createBagBuilder(t, true)
}

func TestDPNBagit(t *testing.T) {
	builder := createBagBuilder(t, true)
	if builder == nil {
		return
	}
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
