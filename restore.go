package bagman

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/diamondap/goamz/aws"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
	"strings"
)

const (
	// DefaultBagSizeLimit is 250GB.
	DefaultBagSizeLimit = int64(268435456000)

	// Allox approx 1MB padding for tag files,
	// manifest, and tar file headers.
	DefaultBagPadding   = int64(1000000)

	S3UriPrefix = "https://s3.amazonaws.com/"
)

// (nsq worker) Make sure we have enough disk space.
// Create the bag (baggins).
// Create tag file with title and description.
// Create subdirectories.
// Download files from S3 to tmp area.
// Copy files into subdirectories.
// Save bag.
// (nsq worker) Tar the bag.
// (nsq worker) Copy the tar file to S3 restoration bucket.
// (nsq worker) Log results.
// (nsq worker) Clean up all temp files.

type FileSet struct {
	Files []*models.GenericFile
}

type BagRestorer struct {
	// The intellectual object we'll be restoring.
	IntellectualObject *models.IntellectualObject
	s3Client           *S3Client
	workingDir         string
	errorMessage       string
	tarFiles           []string
	fileSets           []*FileSet
	bags               []*bagins.Bag
	logger             *logging.Logger
	bagSizeLimit       int64
	bagPadding         int64
}

// Creates a new bag restorer from the intellectual object.
// Param working dir is the path to the directory into which
// files should be downloaded and the bag should be built.
func NewBagRestorer(intelObj *models.IntellectualObject, workingDir string) (*BagRestorer, error) {
	if intelObj == nil {
		return nil, fmt.Errorf("IntellectualObject cannot be nil")
	}
	absWorkingDir, err := filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}
	s3Client, err := NewS3Client(aws.USEast)
	if err != nil {
		return nil, err
	}
	// Specify the location & create a new empty bag.
	restorer := BagRestorer {
		IntellectualObject: intelObj,
		s3Client: s3Client,
		workingDir: absWorkingDir,
		bagSizeLimit: DefaultBagSizeLimit,
		bagPadding: DefaultBagPadding,
	}
	return &restorer, nil
}

// Sets a logger to which the BagRestorer will print debug messages.
// This is optional.
func (restorer *BagRestorer) SetLogger(logger *logging.Logger) {
	restorer.logger = logger
}

// Prints debug messages to the log
func (restorer *BagRestorer) debug (message string) {
	if restorer.logger != nil {
		restorer.logger.Debug(message)
	}
}

// Sets the size limit for a bag. Default is 250GB. This is used
// primarily for testing.
func (restorer *BagRestorer) SetBagSizeLimit(limit int64) {
	restorer.bagSizeLimit = limit
}

func (restorer *BagRestorer) GetBagSizeLimit() (int64) {
	return restorer.bagSizeLimit
}

// Set the padding for the bag. This is the amount of space you
// think tag files, manifests and tar file headers may occupy.
func (restorer *BagRestorer) SetBagPadding(limit int64) {
	restorer.bagPadding = limit
}

func (restorer *BagRestorer) GetBagPadding() (int64) {
	return restorer.bagPadding
}

// Returns the total number of bytes the files in the data directory
// may occupy for this bag, which is calculated as bagSizeLimit - bagPadding.
func (restorer *BagRestorer) GetFileSetSizeLimit() (int64) {
	return restorer.bagSizeLimit - restorer.bagPadding
}


// Fills restorer.fileSets with lists of files that can be packaged
// into individual bags.
func (restorer *BagRestorer) buildFileSets() {
	bytesInSet := int64(0)
	fileSet := &FileSet{}
	for _, gf := range restorer.IntellectualObject.GenericFiles {
		if len(fileSet.Files) > 0 && bytesInSet + gf.Size > restorer.GetFileSetSizeLimit() {
			restorer.fileSets = append(restorer.fileSets, fileSet)
			fileSet = &FileSet{}
			bytesInSet = 0
		}
		fileSet.Files = append(fileSet.Files, gf)
		bytesInSet += gf.Size
		restorer.debug(fmt.Sprintf("Added %s to fileset %d", gf.Identifier, len(restorer.fileSets) + 1))
	}
	if bytesInSet > 0 {
		restorer.fileSets = append(restorer.fileSets, fileSet)
	}
}

// Restores an IntellectualObject by downloading all of its files
// and assembling them into one or more bags. Returns a slice of
// strings, each of which is the path to a bag.
func (restorer *BagRestorer) Restore() ([]string, error) {
	restorer.buildFileSets()
	paths := make([]string, len(restorer.fileSets))
	for i := range(restorer.fileSets) {
		bag, err := restorer.buildBag(i)
		if err != nil {
			return nil, err
		}
		paths[i] = bag.Path()
		restorer.debug(fmt.Sprintf("Finished bag %s", bag.Path()))
	}
	return paths, nil
}

// Creates a single bag and returns a reference to the bag object.
func (restorer *BagRestorer) buildBag(setNumber int) (*bagins.Bag, error) {
	bagName := restorer.bagName(setNumber)
	err := restorer.makeDirectory(bagName)
	if err != nil {
		return nil, err
	}
	restorer.debug(fmt.Sprintf("Creating bag %s", bagName))
	bag, err := bagins.NewBag(restorer.workingDir, bagName, "md5")
	if err != nil {
	 	return nil, err
	}
	// Add tag files. See https://github.com/APTrust/bagins/blob/develop/bag.go#L88
	err = restorer.writeAPTrustTagFile(bag)
	if err != nil {
	 	return nil, err
	}

	// Fetch the generic files
	filesFetched, err := restorer.fetchAllFiles(setNumber)
	if err != nil {
	 	return nil, err
	}

	// Add the fetched files to the bag
	for _, fileName := range filesFetched {
		err = bag.AddFile(fileName, filepath.Base(fileName))
		if err != nil {
			return nil, err
		}
	}

	// Call save to make sure the manifest and tag files
	// are all written to disk.
	errs := bag.Save()
	if errs != nil {
		errMsg := ""
		for i := range(errs) {
			errMsg += fmt.Sprintf("%v | ", errs[i])
		}
		return nil, fmt.Errorf(errMsg)
	}

	return bag, nil
}

// Writes the aptrust-info.txt tag file.
func (restorer *BagRestorer) writeAPTrustTagFile(bag *bagins.Bag) (error) {
	restorer.debug(fmt.Sprintf("Creating aptrust-info.txt"))
	if err := bag.AddTagfile("aptrust-info.txt"); err != nil {
		return err
	}
	tagFile, err := bag.TagFile("aptrust-info.txt")
	if err != nil {
		return err
	}
	tagFile.Data.AddField(*bagins.NewTagField("Title", restorer.IntellectualObject.Title))
	tagFile.Data.AddField(*bagins.NewTagField("Access", restorer.IntellectualObject.Access))
	if restorer.IntellectualObject.Description != "" {
		tagFile.Data.AddField(*bagins.NewTagField("Access", restorer.IntellectualObject.Access))
	}
	return nil
}

// Fetches all of the data files for a bag.
func (restorer *BagRestorer) fetchAllFiles(setNumber int) ([]string, error) {
	fileSet := restorer.fileSets[setNumber]
	localFilePaths := make([]string, len(fileSet.Files))
	for i, gf := range fileSet.Files {
		// TODO: Use go-routines to fetch multiple files at once?
		// If we are restoring many bags simultaneously, we could
		// wind up with too many open connections/file handles.
		fetchResult := restorer.fetchFile(gf, setNumber)
		if fetchResult.ErrorMessage != "" {
			restorer.cleanup(setNumber)
			err := fmt.Errorf("Error fetching file %s from %s: %s",
				gf.Identifier, gf.URI, fetchResult.ErrorMessage)
			return nil, err
		}
		localFilePaths[i] = fetchResult.LocalTarFile
	}
	return localFilePaths, nil
}

// Creates the directories necessary to restore a bag.
func (restorer *BagRestorer) makeDirectory(bagName string) (error){
	localPath := filepath.Join(restorer.workingDir, bagName)
	localDir := filepath.Dir(localPath)
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		restorer.debug(fmt.Sprintf("Creating directory %s", localDir))
		err = os.MkdirAll(localDir, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

// Fetches the requested file from S3 and returns a FetchResult.
func (restorer *BagRestorer) fetchFile(gf *models.GenericFile, setNumber int) (*FetchResult) {
	prefix := strings.SplitN(gf.Identifier, "/data/", 2)
	subdir := strings.Replace(gf.Identifier, prefix[0], restorer.bagName(setNumber), 1)
	localPath := filepath.Join(restorer.workingDir, subdir)
	bucketName, key := bucketNameAndKey(gf.URI)
	restorer.debug(fmt.Sprintf("Fetching key %s from bucket %s for file %s into %s",
		key, bucketName, gf.Identifier, localPath))
	s3Key, err := restorer.s3Client.GetKey(bucketName, key)
	if err != nil {
		errMsg := fmt.Sprintf("Could not get key info for %s: %v", gf.URI, err)
		return &FetchResult {
			ErrorMessage: errMsg,
		}
	}
	return restorer.s3Client.FetchToFile(bucketName, *s3Key, localPath)
}

// Given an S3 URI, returns the bucket name and key.
func bucketNameAndKey(uri string) (string, string) {
	relativeUri := strings.Replace(uri, S3UriPrefix, "", 1)
	parts := strings.SplitN(relativeUri, "/", 2)
	return parts[0], parts[1]
}

// Deletes a single bag created by Restore()
func (restorer *BagRestorer) cleanup(setNumber int) {
	bagDir := filepath.Join(restorer.workingDir, restorer.bagName(setNumber))
	restorer.debug(fmt.Sprintf("Cleaning up %s", bagDir))
	_ = os.RemoveAll(bagDir)
}

// Deletes all of the bags created by Restore()
func (restorer *BagRestorer) Cleanup() {
	for i := range restorer.fileSets {
		restorer.cleanup(i)
	}
}

// BagName returns the IntelObj identifier, minus the institution name prefix,
// plus a suffix like .b001.of125, if necessary. Param setNumber is the
// index of the fileset whose files should go into the bag.
func (restorer *BagRestorer) bagName(setNumber int) (string) {
	instPrefix := fmt.Sprintf("%s/", restorer.IntellectualObject.InstitutionId)
	bagName := strings.Replace(restorer.IntellectualObject.Identifier, instPrefix, "", 1)
	if len(restorer.fileSets) > 1 {
		partNumber := setNumber + 1
		return fmt.Sprintf("%s.b%04d.of%04d", bagName, partNumber, len(restorer.fileSets))
	}
	return bagName
}
