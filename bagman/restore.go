// +build !partners

// Don't include this in the partners build: it's not needed
// in the partner apps, and the syscall.Stat* functions cause
// the build to fail on Windows.
package bagman

import (
	"archive/tar"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/nsqio/go-nsq"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultBagSizeLimit is 250GB.
	DefaultBagSizeLimit = int64(268435456000)

	// Allox approx 1MB padding for tag files,
	// manifest, and tar file headers.
	DefaultBagPadding   = int64(1000000)

	// The default restoration bucket prefix.
	RestorationBucketPrefix = "aptrust.restore"
)

// FileSet is a set of files that will be put into a
// single bag upon restoration. Some large intellectual
// objects will have to be split into multiple bags
// during restoration to accomodate the 250GB bag size limit.
type FileSet struct {
	Files []*GenericFile
}

/*
BagRestorer exposes methods for restoring bags and publishing them to S3
restoration buckets. There are separate methods below for restoring a bag
locally, copying the restored files to S3, and cleaning up.

Generally, you'll want to do all that in one step, which you can do like
this:

    restorer, err := bagman.NewBagRestorer(intellectualObject, outputDir)
    if err != nil {
        return err
    }
    urls, err := RestoreAndPublish()


Here's a fuller example:

    restorer, err := bagman.NewBagRestorer(intellectualObject, outputDir)
    if err != nil {
        return err
    }

    // Optional, if you want to log debug statements.
    // Default is no logging.
    restorer.SetLogger(myCustomLogger)

    // Optional, if you wan to constrain bag size to 50000000 bytes
    // The following aims for total bag sizes of 50000000 bytes
    // that include 100k or so of non-payload data (manifests, tag
    // files, tar file headers). Default is <= 250GB bag size.
    restorer.SetBagSizeLimit(50000000)
    restorer.SetBagPadding(100000)

    // Optional, if you want to restore to a non-standard bucket.
    // Default is aptrust.restore.some.edu
    restorer.SetCustomRestoreBucket("aptrust.test.restore")

    // This creates the bags, copies them to S3, and cleans up.
    // Return value urls is a slice of strings, each of which
    // is a URL pointing to a restored bag on S3.
    urls, err := RestoreAndPublish()

*/
type BagRestorer struct {
	// The intellectual object we'll be restoring.
	IntellectualObject    *IntellectualObject
	// s3Client lets us publish restored bags to S3.
	s3Client              *S3Client
	// workingDir is the root directory under which
	// we build and tar our bags.
	workingDir            string
	// fileSets is a list of FileSet structs. We'll
	// have one for each bag we need to create.
	fileSets              []*FileSet
	// Did we find the bag-info.txt file? For bags
	// ingested before March 29, 2016, we didn't save
	// that file, and we need to rebuild it when we restore.
	foundBagInfo          bool
	// logger is optional. If provided, the functions
	// below will log debug messages to it.
	logger                *logging.Logger
	// The maximum allowed bag size. Default is 250GB,
	// but you can set it smaller to force multiple bags.
	bagSizeLimit          int64
	// The estimated amount of space required by manifest
	// files, tag files and tar file headers in a tarred
	// bag.
	bagPadding            int64
	// The bucket into which restored, tarred bags
	// should be published.
	customRestoreBucket   string
	// Should we restore to the partner's test restoration
	// bucket? This should be true in the demo config only,
	// which runs on test.aptrust.org. Note that
	// customRestoreBucket overrides this.
	restoreToTestBuckets  bool
}

// Creates a new bag restorer from the intellectual object.
// Param working dir is the path to the directory into which
// files should be downloaded and the bag should be built.
func NewBagRestorer(intelObj *IntellectualObject, workingDir string, restoreToTestBuckets bool) (*BagRestorer, error) {
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
		restoreToTestBuckets: restoreToTestBuckets,
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

func (restorer *BagRestorer) SetCustomRestoreBucket (bucketName string) {
	restorer.customRestoreBucket = bucketName
}

func (restorer *BagRestorer) RestorationBucketName () (string) {
	if restorer.customRestoreBucket != "" {
		return restorer.customRestoreBucket
	}
	// Get institution name from bag id. It's the
	// part before the first slash.
	idParts := strings.SplitN(restorer.IntellectualObject.Identifier, "/", 2)
	institution := idParts[0]
	if restorer.restoreToTestBuckets {
		return fmt.Sprintf("%s.test.%s", RestorationBucketPrefix, institution)
	}
	return fmt.Sprintf("%s.%s", RestorationBucketPrefix, institution)
}

// Returns the total number of bytes the files in the data directory
// may occupy for this bag, which is calculated as bagSizeLimit - bagPadding.
func (restorer *BagRestorer) GetFileSetSizeLimit() (int64) {
	return restorer.bagSizeLimit - restorer.bagPadding
}


// Fills restorer.fileSets with lists of files that can be packaged
// into individual bags.
func (restorer *BagRestorer) buildFileSets() {
	totalBytes := int64(0)
	bytesInSet := int64(0)
	fileSet := &FileSet{}
	restorer.debug(fmt.Sprintf("Object %s has %d generic files",
		restorer.IntellectualObject.Identifier,
		len(restorer.IntellectualObject.GenericFiles)))
	for _, gf := range restorer.IntellectualObject.GenericFiles {
		if len(fileSet.Files) > 0 && bytesInSet + gf.Size > restorer.GetFileSetSizeLimit() {
			restorer.fileSets = append(restorer.fileSets, fileSet)
			fileSet = &FileSet{}
			bytesInSet = 0
		}
		origPath, _ := gf.OriginalPath()
		if origPath == "bag-info.txt" {
			restorer.foundBagInfo = true
		}
		fileSet.Files = append(fileSet.Files, gf)
		// Note that total bytes listed in Bag-Size,
		// according to the BagIt spec, can be approximate.
		// That's in section 2.2.2. We calculate this size
		// to figure out whether we should break the bag
		// into parts.
		totalBytes += gf.Size
		bytesInSet += gf.Size
		restorer.debug(fmt.Sprintf("Added %s to fileset %d", gf.Identifier, len(restorer.fileSets) + 1))
	}
	if bytesInSet > 0 {
		restorer.fileSets = append(restorer.fileSets, fileSet)
	}
	restorer.debug(fmt.Sprintf("Built %d file sets with %d bytes", len(restorer.fileSets), totalBytes))
}

/*
Restores an IntellectualObject by downloading all of its files
and assembling them into one or more bags. Returns a slice of
strings, each of which is the path to a bag.

This function restores the entire bag at once, and will use
about 2 * bag_size bytes of disk space. To avoid using so much
disk space, you can use RestoreAndPublish below.
*/
func (restorer *BagRestorer) Restore() ([]string, error) {
	restorer.buildFileSets()
	numberOfBagParts := len(restorer.fileSets)
	paths := make([]string, numberOfBagParts)
	for i := range(restorer.fileSets) {
		bag, err := restorer.buildBag(i, numberOfBagParts)
		if err != nil {
			return nil, err
		}
		paths[i] = bag.Path()
		restorer.debug(fmt.Sprintf("Created local bag %s", bag.Path()))
	}
	return paths, nil
}


// Creates a single bag and returns a reference to the bag object.
func (restorer *BagRestorer) buildBag(setNumber, numberOfBagParts int) (*bagins.Bag, error) {
	bagName := restorer.bagName(setNumber)
	err := restorer.makeDirectory(bagName)
	if err != nil {
		return nil, err
	}
	restorer.debug(fmt.Sprintf("Creating bag %s", bagName))
	bag, err := bagins.NewBag(restorer.workingDir, bagName, []string {"md5", "sha256"}, true)
	if err != nil {
	 	return nil, err
	}
	// Add tag files. See https://github.com/APTrust/bagins/blob/develop/bag.go#L88
	err = restorer.writeAPTrustTagFile(bag)
	if err != nil {
	 	return nil, err
	}

	// We did not save bag-info.txt for bags ingested before March 29, 2016,
	// so we have to reconsititute it here.
	if restorer.foundBagInfo == false {
		restorer.debug(fmt.Sprintf("Rebuilding bag-info.txt for %s", bagName))
		err = restorer.writeBagInfoTagFile(bag, setNumber, numberOfBagParts)
		if err != nil {
			return nil, fmt.Errorf("Could not create bag-info.txt: %v", err)
		}
	}

	// Fetch the generic files
	filesFetched, err := restorer.fetchAllFiles(setNumber)
	if err != nil {
	 	return nil, err
	}

	// Add the fetched files to the bag.
	for _, fileName := range filesFetched {
		pathWithinBag := restorer.PathWithinBag(fileName, bagName)
		if strings.HasPrefix(pathWithinBag, "data/") {
			err = bag.AddFile(fileName, strings.Replace(pathWithinBag, "data/", "", 1))
			if err != nil {
				return nil, err
			}
		} else {
			err = bag.AddCustomTagfile(fileName, pathWithinBag, true)
			if err != nil {
				return nil, err
			}
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

// Notes for fix to PivotalTracker #93237220: data files
// were being put into the wrong directory.
//
// We've already pulled the files down into the proper
// directory structure. The fileName here is an absolute
// path. We need to extract from that the file's path
// within the bag. The call to bag.AddFile says "add the
// file at absolute path x into the bag at relative path y."
// If the two paths wind up being the same (and they will
// be the same here), AddFile does not peform a copy,
// but it does calculate the md5 checksum for the manifest.
//
// The vars below look something like this:
//
// fileName: /Users/apd4n/tmp/restore/test.edu/ncsu.1840.16-1004/data/metadata.xml
// bagName: test.edu/ncsu.1840.16-1004
// workingDir: /Users/apd4n/tmp/restore
// pathWithinBag: data/metadata.xml
func (restorer *BagRestorer) PathWithinBag(fileName, bagName string) (string) {
	index := strings.Index(fileName, bagName)
	endIndex := index + len(bagName) + 1
	return fileName[endIndex:]
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
		tagFile.Data.AddField(*bagins.NewTagField("Description", restorer.IntellectualObject.Description))
	}
	return nil
}

func (restorer *BagRestorer) writeBagInfoTagFile(bag *bagins.Bag, setNumber, numberOfBagParts int) (error) {
	restorer.debug(fmt.Sprintf("Creating bag-info.txt"))
	if err := bag.AddTagfile("bag-info.txt"); err != nil {
		return err
	}
	tagFile, err := bag.TagFile("bag-info.txt")
	if err != nil {
		return err
	}
	slashIndex := strings.Index(restorer.IntellectualObject.Identifier, "/")
	instName := restorer.IntellectualObject.Identifier[0:slashIndex]
	bagNameWithoutInst := restorer.IntellectualObject.Identifier[slashIndex + 1:]

	bagCount := fmt.Sprintf("%d of %d", setNumber + 1, numberOfBagParts)

	tagFile.Data.AddField(*bagins.NewTagField(
		"Source-Organization", instName))
	tagFile.Data.AddField(*bagins.NewTagField(
		"Bagging-Date", time.Now().UTC().Format(time.RFC3339)))
	tagFile.Data.AddField(*bagins.NewTagField(
		"Bag-Count", bagCount))
	tagFile.Data.AddField(*bagins.NewTagField(
		"Bag-Group-Identifier", ""))
	tagFile.Data.AddField(*bagins.NewTagField(
		"Internal-Sender-Description", restorer.IntellectualObject.Description))
	tagFile.Data.AddField(*bagins.NewTagField(
		"Internal-Sender-Identifier", bagNameWithoutInst))
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
		localFilePaths[i] = fetchResult.LocalFile
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
func (restorer *BagRestorer) fetchFile(genericFile *GenericFile, setNumber int) (*FetchResult) {
	subdir := strings.Replace(genericFile.Identifier, restorer.IntellectualObject.Identifier, restorer.bagName(setNumber), 1)
	localPath := filepath.Join(restorer.workingDir, subdir)
	restorer.debug(fmt.Sprintf("Fetching URL %s for file %s into %s",
		genericFile.URI, genericFile.Identifier, localPath))

	// Pivotal #94090170: Retry failed downloads as long as error is not fatal.
	// We get a fair number of "connection reset by peer" errors, which are
	// transient. For fatal errors, Retry will be set to false. We'll break
	// out of this loop if 1) fetch succeeded (no error) or 2) fetch failed
	// on a fatal error (Retry == false).
	var fetchResult *FetchResult
	for i := 0; i < 5; i++ {
		fetchResult = restorer.s3Client.FetchURLToFile(genericFile.URI, localPath)
		if (fetchResult.ErrorMessage == "" ||
			(fetchResult.ErrorMessage != "" && fetchResult.Retry == false)) {
                       break
		}
	}
       return fetchResult
}

// Deletes a single bag created by Restore()
func (restorer *BagRestorer) cleanup(setNumber int) {
	bagDir := filepath.Join(restorer.workingDir, restorer.bagName(setNumber))
	tarFile := fmt.Sprintf("%s.tar", bagDir)

	// Remove the entire bag directory
	restorer.debug(fmt.Sprintf("Cleaning up %s", bagDir))
	_ = os.RemoveAll(bagDir)

	// Remove the tar file, if it exists
	restorer.debug(fmt.Sprintf("Cleaning up %s", tarFile))
	os.Remove(tarFile)
}

// Deletes all of the bags created by Restore()
func (restorer *BagRestorer) Cleanup() {
	for i := range restorer.fileSets {
		restorer.cleanup(i)
	}
}

// BagName returns the IntelObj identifier, plus a suffix like
// .b001.of125, if necessary. Param setNumber is the
// index of the fileset whose files should go into the bag.
func (restorer *BagRestorer) bagName(setNumber int) (string) {
	bagName := restorer.IntellectualObject.Identifier
	if len(restorer.fileSets) > 1 {
		partNumber := setNumber + 1
		return fmt.Sprintf("%s.b%04d.of%04d", bagName, partNumber, len(restorer.fileSets))
	}
	return bagName
}

/*
Tars the bag specified by setNumber, which is zero-based.
Returns the path to the tar file it just created.

Restore() returns a slice of strings, each of which is the
path to a bag. To tar all the bags, you'd do this:

    paths, _ := restorer.Restore()
    for i := range paths {
        pathToTarFile, _ := restorer.TarBag(i)
    }
*/
func (restorer *BagRestorer) TarBag(setNumber int) (string, error) {
	// TODO: Clean up this naming mess in the refactor!!
	bagName := restorer.bagName(setNumber)
	tarFileName := fmt.Sprintf("%s.tar", bagName)
	cleanBagName, err := CleanBagName(tarFileName) // inst.edu/my_bag.b001.of008.tar -> inst.edu/my_bag
	if err != nil {
		return "", err
	}
	slashIndex := strings.Index(cleanBagName, "/") + 1
	bagNameWithoutInstPrefix := cleanBagName[slashIndex:] // inst.edu/my_bag -> my_bag

	tarFilePath := filepath.Join(restorer.workingDir, tarFileName)
	tarFile, err := os.Create(tarFilePath)
	if err != nil {
		return "", fmt.Errorf("Error creating tar file: %v", err)
	}
	tarWriter := tar.NewWriter(tarFile)

	// Add the tag files and the manifest
	bagPath := filepath.Join(restorer.workingDir, bagName)
	textFiles, err := filepath.Glob(filepath.Join(bagPath, "*.txt"))
	for _, textFile := range textFiles {
		textFileBase := filepath.Base(textFile)
		filePath := filepath.Join(restorer.workingDir, bagName, textFileBase)
		pathWithinArchive := filepath.Join(bagNameWithoutInstPrefix, textFileBase)
		err = AddToArchive(tarWriter, filePath, pathWithinArchive)
		if err != nil {
			tarFile.Close()
			os.Remove(tarFilePath)
			return "", err
		}
	}

	// Add all the generic files
	for _, gf := range restorer.fileSets[setNumber].Files {
		gfPath, _ := gf.OriginalPath()
		filePath := filepath.Join(restorer.workingDir, bagName, gfPath)
		pathWithinArchive := filepath.Join(bagNameWithoutInstPrefix, gfPath)
		err = AddToArchive(tarWriter, filePath, pathWithinArchive)
		if err != nil {
			tarFile.Close()
			os.Remove(tarFilePath)
			return "", err
		}
	}
	if err := tarWriter.Close(); err != nil {
		tarFile.Close()
		os.Remove(tarFilePath)
		return "", err
	}
	return tarFilePath, nil
}


/*
Copies a tarred bag file to S3. In most cases, you'll want RestoreAndPublish()
to do this for you. But if you want to do it manually, do something like this
(but don't ignore the errors):

    paths, _ := restorer.Restore()
    for i := range paths {
        pathToTarFile, _ := restorer.TarBag(i)
        s3Url, _ := restorer.CopyToS3(i)
    }
    restorer.Cleanup()
*/
func (restorer *BagRestorer) CopyToS3(setNumber int) (string, error) {
	bagName := restorer.bagName(setNumber)
	tarFileName := fmt.Sprintf("%s.tar", bagName)
	tarFilePath := filepath.Join(restorer.workingDir, tarFileName)
	fileInfo, err := os.Stat(tarFilePath)
	if err != nil {
		return "", nil
	}
	reader, err := os.Open(tarFilePath)
	if err != nil {
		return "", nil
	}
	bucketName := restorer.RestorationBucketName()
	keyName := filepath.Base(bagName) + ".tar"
	defer reader.Close()
	url := ""
	if fileInfo.Size() < S3_LARGE_FILE {
		restorer.debug(fmt.Sprintf("Starting S3 put to %s/%s", bucketName, keyName))
		url, err = restorer.s3Client.SaveToS3(
			bucketName,
			keyName,
			"application/x-tar",
			reader,
			fileInfo.Size(),
			s3.Options{})
	} else {
		restorer.debug(fmt.Sprintf("Starting S3 multipart put to %s/%s", bucketName, keyName))
		url, err = restorer.s3Client.SaveLargeFileToS3(
			bucketName,
			keyName,
			"application/x-tar",
			reader,
			fileInfo.Size(),
			s3.Options{},
			S3_CHUNK_SIZE)
	}
	if err != nil {
		restorer.logger.Error("Error putting key %s into bucket %s: %v",
			keyName, bucketName, err)
		return "", nil
	} else {
		restorer.debug(fmt.Sprintf("Finished putting to S3 %s/%s", bucketName, keyName))
		restorer.debug(fmt.Sprintf("File is stored at %s", url))
	}
	return url, nil
}

// Restores a bag (including multi-part bags), publishes them to the
// restoration bucket, and returns the URLs to access them.
// Param message is an NSQ message and may be nil. In production, we
// want this param, because we need to remind NSQ frequently that
// we're still working on the message. Otherwise, NSQ thinks the
// message timed out.
func (restorer *BagRestorer) RestoreAndPublish(message *nsq.Message) (urls []string, err error) {
	// Make sure we clean up, no matter what happens.
	defer restorer.Cleanup()
	restorer.touch(message)
	restorer.buildFileSets()
	restorer.touch(message)

	// Fully process each bag as we go, including cleanup,
	// so we can preserve disk space.
	numberOfBagParts := len(restorer.fileSets)
	for i := range(restorer.fileSets) {
		restorer.touch(message)
		bag, err := restorer.buildBag(i, numberOfBagParts)
		if err != nil {
			return nil, err
		}
		restorer.debug(fmt.Sprintf("Created local bag %s", bag.Path()))

		restorer.touch(message)
		_, err = restorer.TarBag(i)
		if err != nil {
			return nil, err
		}

		restorer.touch(message)
		s3Url, err := restorer.CopyToS3(i)
		if err != nil {
			return nil, err
		}
		urls = append(urls, s3Url)

		// Cleanup now, so we don't fill up the disk.
		restorer.touch(message)
		restorer.cleanup(i)
	}
	return urls, nil
}

// Try to avoid problem with NSQ timeouts.
func (restorer *BagRestorer) touch(message *nsq.Message) {
	if message != nil {
		message.Touch()
	}
}
