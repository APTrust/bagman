package bagman

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/diamondap/goamz/aws"
	"os"
	"path/filepath"
	"strings"
)

const (
	// BagSizeLimit is 250GB.
	BagSizeLimit = int64(268435456000)

	// Allox approx 1MB padding for tag files,
	// manifest, and tar file headers.
	BagPadding   = int64(1000000)

	// The max number of bytes files can occupy
	// in a bag.
	FileSetSizeLimit = BagSizeLimit - BagPadding

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
	// if _, err := os.Stat(absWorkingDir); os.IsNotExist(err) {
	// 	return nil, fmt.Errorf("The bag restoration working directory, '%s', does not exist", absWorkingDir)
	// }
	s3Client, err := NewS3Client(aws.USEast)
	if err != nil {
		return nil, err
	}
	// Specify the location & create a new empty bag.
	restorer := BagRestorer {
		IntellectualObject: intelObj,
		s3Client: s3Client,
		workingDir: absWorkingDir,
	}
	return &restorer, nil
}

// Fills restorer.fileSets with lists of files that can be packaged
// into individual bags.
func (restorer *BagRestorer) buildFileSets() {
	bytesInSet := int64(0)
	fileSet := &FileSet{}
	for _, gf := range restorer.IntellectualObject.GenericFiles {
		if bytesInSet + gf.Size > FileSetSizeLimit {
			restorer.fileSets = append(restorer.fileSets, fileSet)
			fileSet = &FileSet{}
			bytesInSet = 0
		}
		fileSet.Files = append(fileSet.Files, gf)
		bytesInSet += gf.Size
	}
	if bytesInSet > 0 {
		restorer.fileSets = append(restorer.fileSets, fileSet)
	}
}

func (restorer *BagRestorer) Restore() (error) {
	restorer.buildFileSets()
	for i := range(restorer.fileSets) {
		bag, err := restorer.buildBag(i)
		if err != nil {
			return err
		}
		fmt.Printf("Bag is at %s\n", bag.Path())
	}
	return nil
}

func (restorer *BagRestorer) buildBag(setNumber int) (*bagins.Bag, error) {
	bagName := restorer.bagName(setNumber)
	err := restorer.makeDirectory(bagName)
	if err != nil {
		return nil, err
	}
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
		fmt.Printf("Adding %s -> %s \n", fileName, filepath.Base(fileName))
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
	if err := bag.AddTagfile("aptrust-info.txt"); err != nil {
		return err
	}
	tagFile, err := bag.TagFile("bagit.txt")
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

func (restorer *BagRestorer) fetchAllFiles(setNumber int) ([]string, error) {
	fileSet := restorer.fileSets[setNumber]
	localFilePaths := make([]string, len(fileSet.Files))
	for i, gf := range fileSet.Files {
		// TODO: Use go-routines to fetch multiple files at once?
		// If we are restoring many bags simultaneously, we could
		// wind up with too many open connections/file handles.
		fetchResult := restorer.fetchFile(gf)
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

func (restorer *BagRestorer) makeDirectory(bagName string) (error){
	localPath := filepath.Join(restorer.workingDir, bagName)
	localDir := filepath.Dir(localPath)
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		err = os.MkdirAll(localDir, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func (restorer *BagRestorer) fetchFile(gf *models.GenericFile) (*FetchResult) {
	localPath := filepath.Join(restorer.workingDir, gf.Identifier)
	bucketName, key := bucketNameAndKey(gf.URI)
	s3Key, err := restorer.s3Client.GetKey(bucketName, key)
	if err != nil {
		errMsg := fmt.Sprintf("Could not get key info for %s: %v", gf.URI, err)
		return &FetchResult {
			ErrorMessage: errMsg,
		}
	}
	return restorer.s3Client.FetchToFile(bucketName, *s3Key, localPath)
}

func bucketNameAndKey(uri string) (string, string) {
	relativeUri := strings.Replace(uri, S3UriPrefix, "", 1)
	parts := strings.SplitN(relativeUri, "/", 2)
	return parts[0], parts[1]
}

func (restorer *BagRestorer) cleanup(setNumber int) {
	bagDir := filepath.Join(restorer.workingDir, restorer.bagName(setNumber))
	_ = os.RemoveAll(bagDir)
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

// Returns the path to the tar file. This is the file that should
// be copied to the S3 restoration bucket.
func (bagRestorer *BagRestorer) TarFiles() ([]string) {
	return bagRestorer.tarFiles
}
