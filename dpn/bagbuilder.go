package dpn

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
)

type BagBuilder struct {
	LocalPath          string
	IntellectualObject *bagman.IntellectualObject
	GenericFiles       []bagman.GenericFile
	ErrorMessage       string
}


func NewBagBuilder(localPath string, obj *bagman.IntellectualObject, gf []bagman.GenericFile) (*Bag) {
	if gf == nil {
		gf = make([]bagman.GenericFile, 0)
	}
	return &BagBuilder{
		IntellectualObject: obj,
		GenericFile: gf,
	}
}

func (builder *BagBuilder) BuildBag() (error) {
	bag := &Bag{
		LocalPath: builder.LocalPath,
	}
	if len(builder.GenericFiles) > 0 {
		bag.Type = BAG_TYPE_FILE
		bag.AddDataFile(builder.DataFiles())
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
	tagFilePath := fmt.Sprintf("%s/bagit.txt", builder.LocalPath)
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("BagIt-Version", ""))
	tagFile.Data.AddField(bagins.NewTagField("Tag-File-Character-Encoding", ""))
	return tagFile
}

func (builder *BagBuilder) DPNBagInfo() (*bagins.TagFile) {
	tagFilePath := fmt.Sprintf("%s/bag-info.txt", builder.LocalPath)
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("Source-Organization", ""))
	tagFile.Data.AddField(bagins.NewTagField("Organization-Address", ""))
	tagFile.Data.AddField(bagins.NewTagField("Contact-Name", ""))
	tagFile.Data.AddField(bagins.NewTagField("Contact-Phone", ""))
	tagFile.Data.AddField(bagins.NewTagField("Contact-Email", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bagging-Date", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bag-Size", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bag-Group-Identifier", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bag-Count", ""))
	return tagFile
}

func (builder *BagBuilder) DPNInfo() (*bagins.TagFile) {
	tagFilePath := fmt.Sprintf("%s/dpn-tags/dpn-info.txt", builder.LocalPath)
	tagFile, err := bagins.NewTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("DPN-Object-ID", ""))
	tagFile.Data.AddField(bagins.NewTagField("Local-ID", ""))
	tagFile.Data.AddField(bagins.NewTagField("First-Node-Name", ""))
	tagFile.Data.AddField(bagins.NewTagField("First-Node-Address", ""))
	tagFile.Data.AddField(bagins.NewTagField("First-Node-Contact-Name", ""))
	tagFile.Data.AddField(bagins.NewTagField("First-Node-Contact-Email", ""))
	tagFile.Data.AddField(bagins.NewTagField("Version-Number", ""))
	tagFile.Data.AddField(bagins.NewTagField("Previous-Version-Object-ID", ""))
	tagFile.Data.AddField(bagins.NewTagField("Brightening-Object-ID", ""))
	tagFile.Data.AddField(bagins.NewTagField("Rights-Object-ID", ""))
	tagFile.Data.AddField(bagins.NewTagField("Object-Type", ""))
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

// What goes in here?
func (builder *BagBuilder) DPNTagManifest() (*bagins.Manifest) {
	return nil
}

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustBagIt() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bagit.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("BagIt-Version", "0.97"))
	tagFile.Data.AddField(bagins.NewTagField("Tag-File-Character-Encoding", "UTF-8"))
	return tagFile
}

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustBagInfo() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("bag-info.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("Source-Organization", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bagging-Date", ""))
	tagFile.Data.AddField(bagins.NewTagField("Bag-Count", ""))
	tagFile.Data.AddField(bagins.NewTagField("Internal-Sender-Description", ""))
	tagFile.Data.AddField(bagins.NewTagField("Internal-Sender-Identifier", ""))
	return tagFile
}

// For IntellectualObject bags only
func (builder *BagBuilder) APTrustInfo() (*bagins.TagFile) {
	tagFile, err := bagins.NewTagFile(builder.APTrustMetadataPath("aptrust-info.txt"))
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return nil
	}
	tagFile.Data.AddField(bagins.NewTagField("Title", builder.IntellectualObject.Title))
	tagFile.Data.AddField(bagins.NewTagField("Description", builder.IntellectualObject.Description))
	tagFile.Data.AddField(bagins.NewTagField("Access", builder.IntellectualObject.Access))
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

func (builder *BagBuilder) DataFiles() ([]DataFile) {
	dataFiles := make([]DataFile, len(builder.GenericFiles))
	for i, gf := builder.GenericFiles {
		dataFiles[i] = DataFile{
			ExternalPathType: PATH_TYPE_S3,
			ExternalPath: gf.URI,
			PathInBag: DataPath(gf.Identifier),
		}
	}
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
