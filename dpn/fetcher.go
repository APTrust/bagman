package dpn

import (
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"strings"
)


type DPNFetchResult struct {
	FetchResult *bagman.FetchResult
	GenericFile *bagman.GenericFile
}

func (result *DPNFetchResult) Succeeded() (bool) {
	if result.FetchResult == nil || result.GenericFile == nil || result.FetchResult.ErrorMessage != "" {
		return false
	}
	ourMd5FromIngest := result.GenericFile.GetChecksum("md5")
	if ourMd5FromIngest == nil {
		return false  // This should never happen.
	}
	return result.FetchResult.LocalMd5 == ourMd5FromIngest.Digest
}

type FetchResultCollection struct {
	Items []*DPNFetchResult
	idMap map[string]*DPNFetchResult
}

func NewFetchResultCollection() (*FetchResultCollection) {
	return &FetchResultCollection {
		Items: make([]*DPNFetchResult, 0),
		idMap: make(map[string]*DPNFetchResult, 0),
	}
}

func (results *FetchResultCollection) SuccessCount() (int) {
	count := 0
	for _, result := range results.Items {
		if result.Succeeded() {
			count += 1
		}
	}
	return count
}

func (results *FetchResultCollection) Errors() ([]string) {
	errors := make([]string, 0)
	for _, result := range results.Items {
		if !result.Succeeded() {
			errors = append(errors, result.FetchResult.ErrorMessage)
		}
	}
	return errors
}

func (results *FetchResultCollection) Add(result *DPNFetchResult) {
	results.Items = append(results.Items, result)
}

// Finds a DPNFetchResult by GenericFile.Identifier
func (results *FetchResultCollection) FindByIdentifier(identifier string) (*DPNFetchResult) {
	if len(results.idMap) != len(results.Items) {
		results.buildMap()
	}
	dpnFetchResult, _ := results.idMap[identifier]
	return dpnFetchResult
}

func (results *FetchResultCollection) buildMap() {
	results.idMap = make(map[string]*DPNFetchResult, len(results.Items))
	for i := range results.Items {
		item := results.Items[i]
		gf := item.GenericFile
		results.idMap[gf.Identifier] = item
	}
}

// FetchFiles fetches remote S3 files that make up the specified
// IntellectualObject into the specified directory.
func FetchObjectFiles(s3Client *bagman.S3Client, genericFiles []*bagman.GenericFile, dir string) (*FetchResultCollection, error) {
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	results := NewFetchResultCollection()
	for _, gf := range genericFiles {
		origPath, err := gf.OriginalPath()
		if err != nil {
			return nil, err
		}
		localPath := filepath.Join(dir, origPath)
		fetchResult := s3Client.FetchURLToFile(gf.URI, localPath)
		result := &DPNFetchResult{
			FetchResult: fetchResult,
			GenericFile: gf,
		}
		results.Add(result)
	}
	return results, nil
}
