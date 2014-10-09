package bagman

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
	InputFile     string
	OutputDir     string
	ErrorMessage  string
	Warnings      []string
	FilesUnpacked []string
	GenericFiles  []*GenericFile
}

// Returns true if any of the untarred files are new or updated.
func (result *TarResult) AnyFilesNeedSaving() (bool) {
	for _, gf := range result.GenericFiles {
		if gf.NeedsSave == true {
			return true
		}
	}
	return false
}

// GenericFilePaths returns a list of all the GenericFile paths
// that were untarred from the bag. The list will look something
// like "data/file1.gif", "data/file2.pdf", etc.
func (result *TarResult) GenericFilePaths() []string {
	paths := make([]string, len(result.GenericFiles))
	for index, gf := range result.GenericFiles {
		paths[index] = gf.Path
	}
	return paths
}

// Returns the GenericFile with the specified path, if it exists.
func (result *TarResult) GetFileByPath(filePath string) (*GenericFile) {
	for index, gf := range result.GenericFiles {
		if gf.Path == filePath {
			// Be sure to return to original, and not a copy!
			return result.GenericFiles[index]
		}
	}
	return nil
}

// MergeExistingFiles merges data from generic files that
// already exist in Fedora. This is necessary when an existing
// bag is reprocessed or re-uploaded.
func (result *TarResult) MergeExistingFiles(fluctusFiles []*FluctusFile) {
	for _, fluctusFile := range fluctusFiles {
		origPath, _ := fluctusFile.OriginalPath()
		gf := result.GetFileByPath(origPath)
		if gf != nil {
			gf.ExistingFile = true
			// Files have the same path and name. If the checksum
			// has not changed, there is no reason to re-upload
			// this file to the preservation bucket, nor is there
			// any reason to create new ingest events in Fedora.
			existingMd5 := fluctusFile.GetChecksum("md5")
			if gf.Md5 == existingMd5.Digest {
				gf.NeedsSave = false
			}
		}
	}
}

// Returns true if any generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AnyFilesCopiedToPreservation() bool {
	for _, gf := range result.GenericFiles {
		if gf.StorageURL != "" {
			return true
		}
	}
	return false
}

// Returns true if all generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AllFilesCopiedToPreservation() bool {
	for _, gf := range result.GenericFiles {
		if gf.NeedsSave && gf.StorageURL == "" {
			return false
		}
	}
	return true
}
