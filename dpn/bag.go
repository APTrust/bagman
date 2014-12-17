package dpn

import (
	"github.com/APTrust/bagins"
)

var BAG_TYPE_OBJECT = "IntellectualObject"
var BAG_TYPE_FILE   = "GenericFile"

var PATH_TYPE_LOCAL = "Local Filesystem"
var PATH_TYPE_S3    = "S3 Bucket"

type Bag struct {
	// The type of bag: IntellectualObject or GenericFile
	Type                string

	// Where does this bag reside on disk, or where
	// should it reside when we write it? Use an
	// absolute path that ends with the bag name.
	// For example:
	// /mnt/aptrust/dpn/DPN-91e09518-e910-464c-8b6c-8e39685e9acc
	LocalPath           string

	// The name/id of the bag
	DPNObjectId         string

	// DPN bag data.
	DPNBagIt            *bagins.TagFile
	DPNBagInfo          *bagins.TagFile
	DPNInfo             *bagins.TagFile
	DPNManifestSha256   *bagins.Manifest
	DPNTagManifest      *bagins.Manifest

	// APTrust metadata for all objects - type will change from string!
	DescriptiveMetaData string
	PremisEvents        string
	APTrustDPNManifest  string

	// APTrust bag data.
	// Only for bags representing IntellectualObjects.
	APTrustBagIt        *bagins.TagFile
	APTrustBagInfo      *bagins.TagFile
	APTrustInfo         *bagins.TagFile
	APTrustManifestMd5  *bagins.Manifest

	// Files inside the data directory
	DataFiles           []DataFile
}

type DataFile struct {
	ExternalPathType  string
	ExternalPath      string
	PathInBag         string
}


func (bag *Bag) Write(outputPath string) (error) {
	// Write me!
	return nil
}
