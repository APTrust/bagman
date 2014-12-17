package dpn

import (
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
)

type BagBuilder struct {
	LocalPath          string
	IntellectualObject *bagman.IntellectualObject
	GenericFiles       []*bagman.GenericFile
	ErrorMessage       string
}


func NewBagBuilder(localPath string, obj *bagman.IntellectualObject, gf []*bagman.GenericFile) (*BagBuilder) {
	// gf may be nil if bag is for IntelObj
	if gf == nil {
		gf = make([]*bagman.GenericFile, 0)
	}
	filePath, err := filepath.Abs(localPath)
	builder :=  &BagBuilder{
		LocalPath: filePath,
		IntellectualObject: obj,
		GenericFiles: gf,
	}
	if err != nil {
		builder.ErrorMessage = err.Error()
	}
	return builder
}

func (builder *BagBuilder) BuildBag() (error) {
	bag := &Bag{
		LocalPath: builder.LocalPath,
	}
	if len(builder.GenericFiles) > 0 {
		bag.Type = BAG_TYPE_FILE
		bag.DataFiles = builder.DataFiles()
	} else {
		bag.Type = BAG_TYPE_OBJECT
		bag.APTrustBagIt = builder.APTrustBagIt()
		bag.APTrustBagInfo = builder.APTrustBagInfo()
		bag.APTrustInfo = builder.APTrustInfo()
		bag.APTrustManifestMd5 = builder.APTrustManifestMd5()
	}
	bag.DPNBagIt = builder.DPNBagIt()
	bag.DPNBagInfo = builder.DPNBagInfo()
	bag.DPNInfo = builder.DPNInfo()
	bag.DPNManifestSha256 = builder.DPNManifestSha256()
	bag.DPNTagManifest = builder.DPNTagManifest()
	if builder.ErrorMessage != "" {
		return fmt.Errorf(builder.ErrorMessage)
	}
	return nil
}

func (builder *BagBuilder) DPNBagIt() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "bagit.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("BagIt-Version", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding", ""))
	return tagFile
}

func (builder *BagBuilder) DPNBagInfo() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "bag-info.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("Source-Organization", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Organization-Address", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Name", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Phone", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Contact-Email", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bagging-Date", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Size", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Group-Identifier", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Count", ""))
	return tagFile
}

func (builder *BagBuilder) DPNInfo() (*bagins.TagFile) {
	tagFilePath := filepath.Join(builder.LocalPath, "dpn-tags","dpn-info.txt")
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("DPN-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Local-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Name", ""))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Address", ""))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Contact-Name", ""))
	tagFile.Data.AddField(*bagins.NewTagField("First-Node-Contact-Email", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Version-Number", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Previous-Version-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Brightening-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Rights-Object-ID", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Object-Type", ""))
	return tagFile
}

func (builder *BagBuilder) DPNManifestSha256() (*bagins.Manifest) {
	manifest, err := bagins.NewManifest("manifest", "sha256")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	for _, gf := range builder.GenericFiles {
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
	// Can't use bagins.Manifest here because manifest expects
	manifest, err := bagins.NewManifest("tagmanifest-sha256", "sha256")
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

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustBagIt() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bagit.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("BagIt-Version", "0.97"))
	tagFile.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding", "UTF-8"))
	return tagFile
}

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustBagInfo() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bag-info.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(*bagins.NewTagField("Source-Organization", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bagging-Date", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Bag-Count", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Internal-Sender-Description", ""))
	tagFile.Data.AddField(*bagins.NewTagField("Internal-Sender-Identifier", ""))
	return tagFile
}

// For IntellectualObject bags only
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

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustManifestMd5() (*bagins.Manifest) {
	manifestPath := builder.APTrustMetadataPath("manifest-md5.txt")
	manifest, err := bagins.NewManifest(manifestPath, "md5")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	for _, gf := range builder.GenericFiles {
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
// directory of the DPN bag. For now, we're doing one file per
// bag, but this code can handle multiple files.
func (builder *BagBuilder) DataFiles() ([]DataFile) {
	dataFiles := make([]DataFile, len(builder.GenericFiles))
	for i, gf := range builder.GenericFiles {
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
	return fmt.Sprintf("data/%s", identifier)
}

// Returns the path inside the bag for a APTrust metadata file.
// Only IntellectualObject bags will contain these files.
func (builder *BagBuilder) APTrustMetadataPath(filename string) (string) {
	return fmt.Sprintf("data/%s/%s", builder.IntellectualObject.Identifier, filename)
}
