package dpn

import (
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/nu7hatch/gouuid"
	"os"
	"path/filepath"
	"strings"
	"time"
)


// NewBagBuilder returns a new BagBuilder.
// Param localPath is the path to which the bag builder should write the
// DPN bag. Param obj is an IntellectualObject containing metadata
// about the APTrust bag that we'll be repackaging. Param defaultMetadata
// contains default metadata, such as the BagIt version, ingest node name,
// etc.
func NewBagBuilder(localPath string, obj *bagman.IntellectualObject, defaultMetadata *DefaultMetadata) (*BagBuilder) {
	uuid, uuidErr := uuid.NewV4()
	filePath, err := filepath.Abs(localPath)
	bag := &Bag{
		LocalPath: filePath,
		Type: BAG_TYPE_DATA,
	}
	builder :=  &BagBuilder{
		LocalPath: filePath,
		IntellectualObject: obj,
		DefaultMetadata: defaultMetadata,
		UUID: uuid.String(),
		bag: bag,
	}
	if err != nil {
		builder.ErrorMessage = err.Error()
	}
	if uuidErr != nil {
		builder.ErrorMessage += uuidErr.Error()
	}

	err = os.MkdirAll(filepath.Join(filePath, "dpn-tags"), 0755)
	if err != nil {
		builder.ErrorMessage += err.Error()
	}
	err = os.MkdirAll(filepath.Join(filePath, "data"), 0755)
	if err != nil {
		builder.ErrorMessage += err.Error()
	}
	err = os.MkdirAll(filepath.Join(filePath, "aptrust-tags"), 0755)
	if err != nil {
		builder.ErrorMessage += err.Error()
	}
	return builder
}

// BagTime returns the datetime the bag was created,
// in RFC3339 format (e.g. "2015-03-05T10:10:00Z")
func (builder *BagBuilder) BagTime() (string) {
	return builder.bagtime.Format(time.RFC3339)
}

func (builder *BagBuilder) BuildBag() (*Bag, error) {
	if builder.bag.Type == BAG_TYPE_DATA {
		builder.bag.DataFiles = builder.DataFiles()
		builder.bag.APTrustManifestMd5 = builder.APTrustManifestMd5()
	}

	builder.bag.APTrustBagIt = builder.APTrustBagIt()
	builder.bag.APTrustBagInfo = builder.APTrustBagInfo()
	builder.bag.APTrustInfo = builder.APTrustInfo()

	builder.bag.DPNBagIt = builder.DPNBagIt()
	builder.bag.DPNBagInfo = builder.DPNBagInfo()
	builder.bag.DPNInfo = builder.DPNInfo()
	builder.bag.DPNManifestSha256 = builder.DPNManifestSha256()
	builder.bag.DPNTagManifest = builder.DPNTagManifest()
	if builder.ErrorMessage != "" {
		return nil, fmt.Errorf(builder.ErrorMessage)
	}
	return builder.bag, nil
}

func (builder *BagBuilder) DPNBagIt() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "bagit.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("BagIt-Version",
		builder.DefaultMetadata.BagItVersion))
	tagFile.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding",
		builder.DefaultMetadata.BagItEncoding))
	return tagFile
}

func (builder *BagBuilder) DPNBagInfo() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "bag-info.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("Source-Organization",
		builder.IntellectualObject.InstitutionId))
	tagFile.Data.AddField(*bagins.NewTagField("Organization-Address", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Name", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Phone", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Email", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bagging-Date", builder.BagTime()))

	// TODO: How can we put the bag size in a file that's inside the bag?
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Size",
		fmt.Sprintf("%d", builder.IntellectualObject.TotalFileSize())))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Group-Identifier", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Count", "1"))
	return tagFile
}

func (builder *BagBuilder) DPNInfo() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "dpn-tags", "dpn-info.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("DPN-Object-ID",
		builder.UUID))
	tagFile.Data.AddField(*bagins.NewTagField("Local-ID",
		builder.IntellectualObject.Identifier))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Name",
		builder.DefaultMetadata.IngestNodeName))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Address",
		builder.DefaultMetadata.IngestNodeAddress))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Contact-Name",
		builder.DefaultMetadata.IngestNodeContactName))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Contact-Email",
		builder.DefaultMetadata.IngestNodeContactEmail))

	// TODO: Not sure how to fill in the next three items.
	// We have to wait until DPN versioning spec is written, then we
	// need to know how to let depositors specify whether to overwrite
	// bags or save new versions in DPN, then we need a way of knowing
	// which DPN object this is a new version of, and which version
	// it should be.
	tagFile.Data.AddField(*bagins.NewTagField("Version-Number", "1"))
	// Are we also using First-Version-Object-ID?
	// Check https://wiki.duraspace.org/display/DPN/BagIt+Specification
	// for updates.
	tagFile.Data.AddField(*bagins.NewTagField("First-Version-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Brightening-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Rights-Object-ID", ""))

	// Bag Type
	tagFile.Data.AddField(*bagins.NewTagField("Object-Type",
		builder.bag.Type))

	return tagFile
}

func (builder *BagBuilder) DPNManifestSha256() (*bagins.Manifest) {
	manifestPath := filepath.Join(builder.LocalPath, "manifest-sha256.txt")
	manifest, err := bagins.NewManifest(manifestPath, "sha256")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	for _, gf := range builder.IntellectualObject.GenericFiles {
		pathInBag := DataPath(gf.Identifier)
		sha256 := gf.GetChecksum("sha256")
		if sha256 == nil {
			builder.ErrorMessage += fmt.Sprintf("[GenericFile %s is missing sha256 checksum] ", gf.Identifier)
			return nil
		}
		manifest.Data[pathInBag] = sha256.Digest
	}
	return manifest
}

func (builder *BagBuilder) DPNTagManifest() (*bagins.Manifest) {
	manifestPath := filepath.Join(builder.LocalPath, "tagmanifest-sha256.txt")
	manifest, err := bagins.NewManifest(manifestPath, "sha256")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}

	bagIt := builder.DPNBagIt()
	if bagIt == nil {
		builder.ErrorMessage += "[Cannot run checksum on DPN bagit.txt: failed to produce tagfile.] "
	} else {
		bagItStr, err := bagIt.ToString()
		if err != nil {
			builder.ErrorMessage += "[Cannot get contents of DPN bagit.txt.] "
		}
		manifest.Data["bagit.txt"] = sha256Digest(bagItStr)
	}

	bagInfo := builder.DPNBagInfo()
	if bagInfo == nil {
		builder.ErrorMessage += "[Cannot run checksum on DPN bag-info.txt: failed to produce tagfile.] "
	} else {
		bagInfoStr, err := bagInfo.ToString()
		if err != nil {
			builder.ErrorMessage += "[Cannot get contents of DPN bag-info.txt.] "
		}
		manifest.Data["bag-info.txt"] = sha256Digest(bagInfoStr)
	}

	// Note that dpn-info.txt contains a UUID unique to this bag.
	dpnInfo := builder.DPNInfo()
	if dpnInfo == nil {
		builder.ErrorMessage += "[Cannot run checksum on DPN bag-info.txt: failed to produce tagfile.] "
	} else {
		dpnInfoStr, err := dpnInfo.ToString()
		if err != nil {
			builder.ErrorMessage += "[Cannot get contents of DPN dpn-info.txt.] "
		}
		manifest.Data["dpn-tags/dpn-info.txt"] = sha256Digest(dpnInfoStr)
	}

	return manifest
}

func sha256Digest(str string) (string) {
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}


func (builder *BagBuilder) APTrustBagIt() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bagit.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("BagIt-Version",
		APTRUST_BAGIT_VERSION))
	tagFile.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding",
		APTRUST_BAGIT_ENCODING))
	return tagFile
}


func (builder *BagBuilder) APTrustBagInfo() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bag-info.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("Source-Organization",
		builder.IntellectualObject.InstitutionId))
	tagFile.Data.AddField(*bagins.NewTagField("Bagging-Date", builder.BagTime()))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Count", "1"))
	tagFile.Data.AddField(*bagins.NewTagField("Internal-Sender-Description",
		builder.IntellectualObject.Description))
	tagFile.Data.AddField(*bagins.NewTagField("Internal-Sender-Identifier",
		builder.IntellectualObject.Identifier))
	return tagFile
}


func (builder *BagBuilder) APTrustInfo() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("aptrust-info.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("Title", builder.IntellectualObject.Title))
	tagFile.Data.AddField(*bagins.NewTagField("Description", builder.IntellectualObject.Description))
	tagFile.Data.AddField(*bagins.NewTagField("Access", builder.IntellectualObject.Access))
	return tagFile
}


func (builder *BagBuilder) APTrustManifestMd5() (*bagins.Manifest) {
	manifestPath := builder.APTrustMetadataPath("manifest-md5.txt")
	manifest, err := bagins.NewManifest(manifestPath, "md5")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	for _, gf := range builder.IntellectualObject.GenericFiles {
		pathInBag := DataPath(gf.Identifier)
		md5 := gf.GetChecksum("md5")
		if md5 == nil {
			builder.ErrorMessage += fmt.Sprintf("[GenericFile %s is missing sha256 checksum] ", gf.Identifier)
			return nil
		}
		manifest.Data[pathInBag] = md5.Digest
	}
	return manifest
}

// Returns a list of files that should be packed into the data
// directory of the DPN bag.
func (builder *BagBuilder) DataFiles() ([]DataFile) {
	dataFiles := make([]DataFile, len(builder.IntellectualObject.GenericFiles))
	for i, gf := range builder.IntellectualObject.GenericFiles {
		dataFiles[i] = DataFile{
			ExternalPathType: PATH_TYPE_S3,
			ExternalPath: gf.URI,
			PathInBag: DataPath(gf.Identifier),
		}
	}
	return dataFiles
}

// Given a GenericFile identifier, returns the path inside
// the bag where that file should reside.
func DataPath(identifier string) (string) {
	index := strings.Index(identifier, "data/")
	return identifier[index:]
	//return fmt.Sprintf("data/%s", identifier)
}

// Returns the path inside the bag for a APTrust metadata file.
func (builder *BagBuilder) APTrustMetadataPath(filename string) (string) {
	return filepath.Join(builder.LocalPath, "aptrust-tags", filename)
}
