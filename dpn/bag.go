package dpn

import (
	"github.com/APTrust/bagins"
)


type Bag struct {
	// The type of bag: IntellectualObject or GenericFile
	Type                string

	// Where does this bag reside on disk, or where
	// should it reside when we write it? Use an
	// absolute path that ends with the bag name.
	// For example:
	// /mnt/aptrust/dpn/test.edu/my_bag
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

	// // Files inside the bag. This includes items in the payload
	// // directory and items in custom tag directories.
	// DataFiles           []DataFile

	// Bag Errors
	errors              []string
}

// type DataFile struct {
// 	ExternalPathType  string
// 	ExternalPath      string
// 	PathInBag         string
// }

// Tests for the Write functions are in bagbuilder_test.go

func (bag *Bag) Write() ([]string) {
	bag.WriteManifest(bag.DPNManifestSha256)
	bag.WriteManifest(bag.DPNTagManifest)
	bag.WriteManifest(bag.APTrustManifestMd5)
	bag.WriteTagFile(bag.DPNBagIt)
	bag.WriteTagFile(bag.DPNBagInfo)
	bag.WriteTagFile(bag.DPNInfo)
	bag.WriteTagFile(bag.APTrustBagIt)
	bag.WriteTagFile(bag.APTrustBagInfo)
	bag.WriteTagFile(bag.APTrustInfo)
	return bag.errors
}

func (bag *Bag) WriteTagFile(tagFile *bagins.TagFile) {
	if err := tagFile.Create(); err != nil {
		bag.errors = append(bag.errors, err.Error())
	}
}

func (bag *Bag) WriteManifest(manifest *bagins.Manifest) {
	if err := manifest.Create(); err != nil {
		bag.errors = append(bag.errors, err.Error())
	}
}
