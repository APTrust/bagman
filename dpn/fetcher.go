package dpn

import (
	"github.com/APTrust/bagman/bagman"
	"strings"
)


type DPNFetchResult struct {
	FetchResult *bagman.FetchResult
	GenericFile *bagman.GenericFile
}

func (result *DPNFetchResult) Succeeded() (bool) {
	if result.FetchResult == nil || result.FetchResult.ErrorMessage != "" {
		return false
	}
	ourMd5FromIngest := result.GenericFile.GetChecksum("md5")
	return result.FetchResult.LocalMd5 == ourMd5FromIngest.Digest
}

type DPNFetchResults struct {
	Items []*DPNFetchResult
	idMap map[string]*DPNFetchResult
}

// Finds a DPNFetchResult by GenericFile.Identifier
func (results *DPNFetchResults) FindByIdentifier(identifier string) (*DPNFetchResult) {
	if len(results.idMap) != len(results.Items) {
		results.buildMap()
	}
	dpnFetchResult, _ := results.idMap[identifier]
	return dpnFetchResult
}

func (results *DPNFetchResults) buildMap() {
	results.idMap = make(map[string]*DPNFetchResult, len(results.Items))
	for i := range results.Items {
		item := results.Items[i]
		gf := item.GenericFile
		results.idMap[gf.Identifier] = item
	}
}

// FetchFiles fetches remote S3 files that make up the specified
// IntellectualObject into the specified directory.
func FetchObjectFiles(s3Client *bagman.S3Client, genericFiles []*bagman.GenericFile, dir string) ([]*DPNFetchResult, error) {
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	results := make([]*DPNFetchResult, len(genericFiles))
	for i, gf := range genericFiles {
		origPath, err := gf.OriginalPath()
		if err != nil {
			return nil, err
		}
		localPath := dir + origPath
		fetchResult := s3Client.FetchURLToFile(gf.URI, localPath)
		results[i] = &DPNFetchResult{
			FetchResult: fetchResult,
			GenericFile: gf,
		}
	}
	return results, nil
}
