package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"strings"
)


type DPNFetchResult struct {
	FetchResult *bagman.FetchResult
	GenericFile *bagman.GenericFile
}

// FetchFiles fetches remote S3 files that make up the specified
// IntellectualObject into the specified directory.
func FetchObjectFiles(s3Client *bagman.S3Client, obj *bagman.IntellectualObject, dir string) ([]*DPNFetchResult, error) {
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	results := make([]*DPNFetchResult, len(obj.GenericFiles))
	for i, gf := range obj.GenericFiles {
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
		if fetchResult.ErrorMessage != "" {
			err := fmt.Errorf("Error retrieving %s from %s: %s",
				gf.Identifier, gf.URI, fetchResult)
			results = nil
			return nil, err
		}
	}
	return results, nil
}
