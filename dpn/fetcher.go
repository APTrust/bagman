package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"strings"
)

// fetcher.go fetches APTrust files from S3 so we can put them into
// a DPN bag. This is used for DPN bags that we create at APTrust.
// For copying bags from remote DPN nodes, see copier.go, which copies
// files to our local staging area so we can replicate them.

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
		result.FetchResult.ErrorMessage = fmt.Sprintf("Cannot verify md5 checksum on file '%s', because the system cannot find the GenericFile's original md5 digest.", result.GenericFile.Identifier)
		return false  // This should never happen.
	}
	checksumMatches := result.FetchResult.LocalMd5 == ourMd5FromIngest.Digest
	if !checksumMatches {
		result.FetchResult.ErrorMessage = fmt.Sprintf("Checksum mismatch for GenericFile '%s' (URI '%s'): md5 checksum of file fetched from S3 is '%s'; md5 checksum of GenericFile is '%s'", result.GenericFile.Identifier, result.GenericFile.URI, result.FetchResult.LocalMd5, ourMd5FromIngest.Digest)
	}
	return checksumMatches
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
	// Map prevents the same file being counted twice
	// if it was added twice with Add().
	succeeded := make(map[string]bool)
	for _, result := range results.Items {
		_, alreadyCounted := succeeded[result.GenericFile.Identifier]
		if result.Succeeded() && !alreadyCounted {
			succeeded[result.GenericFile.Identifier] = true
		}
	}
	return len(succeeded)
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
		// APTrust bags allow misc tag files in top directory,
		// but DPN BagIt spec doesn't explicitly allow that,
		// so we'll put those files in custom tag dir, which
		// DPN does allow.
		newPath := origPath
		if !strings.Contains(origPath, "/") {
			newPath = fmt.Sprintf("aptrust-tags/%s", origPath)
		}
		localPath := filepath.Join(dir, newPath)
		fetchResult := s3Client.FetchURLToFile(gf.URI, localPath)
		result := &DPNFetchResult{
			FetchResult: fetchResult,
			GenericFile: gf,
		}
		results.Add(result)
	}
	return results, nil
}
