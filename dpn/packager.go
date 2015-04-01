package dpn

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Packager creates DPN bags from APTrust IntellectualObjects
// through the following steps:
//
// 1. Fetch all data files from S3.
// 2. Build the DPN bag with data files, manifests and tag files.
// 3. Tar the bag.
//
// The packager pushes data through the following channels:
//
// 1. LookupChannel retrieves the IntellectualObject (and a list of
//    its files) from Fluctus.
// 2. FetchChannel fetches the bag's data files from S3.
// 3. BuildChannel builds the DPN bag.
// 4. TarChannel tars the DPN bag.
// 5. CleanupChannel deletes the files that went into the
//    tarred bag (but keeps the tar file).
// 6. ResultsChannel logs results, tells NSQ the work is done,
//    and queues items from post-processing.
//
// Steps 5 and 6 are guaranteed to occur, no matter what happens
// in the other steps.

type PackageResult struct {
	BagIdentifier   string
	NsqMessage      *nsq.Message `json:"-"`
	BagBuilder      *BagBuilder
	DPNFetchResults []*DPNFetchResult
	TarFilePath     string
	ErrorMessage    string
	Retry           bool
}

func (result *PackageResult) Errors() ([]string) {
	errors := make([]string, 0)
	if result.ErrorMessage != "" {
		errors = append(errors, result.ErrorMessage)
	}
	if result.BagBuilder.ErrorMessage != "" {
		errors = append(errors, result.BagBuilder.ErrorMessage)
	}
	for _, result := range result.DPNFetchResults {
		if result.FetchResult.ErrorMessage != "" {
			errors = append(errors, result.FetchResult.ErrorMessage)
		}
	}
	return errors
}

func (result *PackageResult) Succeeded() (bool) {
	return result.TarFilePath != "" && len(result.Errors()) == 0
}

type Packager struct {
	LookupChannel       chan *PackageResult
	FetchChannel        chan *PackageResult
	BuildChannel        chan *PackageResult
	TarChannel          chan *PackageResult
	CleanupChannel      chan *PackageResult
	PostProcessChannel  chan *PackageResult
	DefaultMetadata     *DefaultMetadata
	ProcUtil            *bagman.ProcessUtil
}

func NewPackager(procUtil *bagman.ProcessUtil, obj *bagman.IntellectualObject, defaultMetadata *DefaultMetadata) (*Packager) {
	packager := &Packager {
		DefaultMetadata: defaultMetadata,
		ProcUtil: procUtil,
	}
	fetcherBufferSize := procUtil.Config.DPNPackageWorker.NetworkConnections * 4
	workerBufferSize := procUtil.Config.DPNPackageWorker.Workers * 4
	packager.FetchChannel = make(chan *PackageResult, fetcherBufferSize)
	packager.BuildChannel = make(chan *PackageResult, workerBufferSize)
	packager.TarChannel = make(chan *PackageResult, workerBufferSize)
	packager.CleanupChannel = make(chan *PackageResult, workerBufferSize)
	packager.PostProcessChannel = make(chan *PackageResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go packager.doLookup()
		go packager.doBuild()
		go packager.doTar()
		go packager.doCleanup()
		go packager.postProcess()
	}
	for i := 0; i <  procUtil.Config.PrepareWorker.NetworkConnections; i++ {
		go packager.doFetch()
	}
	return packager
}

// MessageHandler handles messages from NSQ, putting each
// item into the pipleline.
func (packager *Packager) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	var packageResult *PackageResult
	err := json.Unmarshal(message.Body, packageResult)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		packager.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Start processing.
	packager.LookupChannel <- packageResult
	packager.ProcUtil.MessageLog.Info("Put %s into lookup channel",
		packageResult.BagIdentifier)
	return nil
}

// doLookup gets information about the intellectual object from
// Fluctus, builds a package result object, and moves the data
// into the FetchChannel.
//
// TODO: Check whether this bag already exists in DPN??
func (packager *Packager) doLookup() {
	for result := range packager.LookupChannel {
		// Get the bag, with a list of GenericFiles
		intelObj, err := packager.ProcUtil.FluctusClient.IntellectualObjectGet(result.BagIdentifier, true)
		if err != nil {
			// FAIL - Can't get intel obj data (HTTP or Fluctus error)
			result.ErrorMessage = fmt.Sprintf("Could not fetch info about IntellectualObject " +
				"'%s' from Fluctus: %s", result.BagIdentifier, err.Error())
			result.Retry = true
			packager.PostProcessChannel <- result
			continue
		}
		if intelObj == nil {
			// FAIL - Can't get intel obj data (Object not found)
			result.ErrorMessage = fmt.Sprintf("Fluctus returned nil for IntellectualObject %s",
				result.BagIdentifier)
			result.Retry = true
			packager.PostProcessChannel <- result
			continue
		}
		err = packager.ProcUtil.Volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
		if err != nil {
			// FAIL - Not enough disk space in staging area to build this bag
			packager.ProcUtil.MessageLog.Warning("Requeueing bag %s, %d bytes - not enough disk space",
				result.BagIdentifier, intelObj.TotalFileSize())
			result.ErrorMessage = err.Error()
			result.Retry = true
			packager.PostProcessChannel <- result
			continue
		} else {
			dir, err := packager.DPNBagDirectory(result)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("Cannot get absolute path for bag directory: %s",
					err.Error())
				result.Retry = true
				packager.PostProcessChannel <- result
				continue
			}
			// Woo-hoo!
			result.BagBuilder = NewBagBuilder(dir, intelObj, packager.DefaultMetadata)
			packager.FetchChannel <- result
		}
	}
}


// doFetch fetches the IntellectualObject's files from S3 and
// stores them locally. Data then goes into the BuildChannel
// so we can build the DPN bag.
func (packager *Packager) doFetch() {
	for result := range packager.FetchChannel {
		targetDirectory, err := packager.DPNBagDirectory(result)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Cannot get abs path for bag directory: %s", err.Error())
			packager.CleanupChannel <- result
			continue
		}
		files, err := packager.FilesToFetch(result)
		if err != nil {
			result.ErrorMessage += err.Error()
			packager.CleanupChannel <- result
			continue
		}
		fetchResults, err := FetchObjectFiles(packager.ProcUtil.S3Client,
			files, targetDirectory)
		if err != nil {
			result.ErrorMessage += err.Error()
			packager.CleanupChannel <- result
		} else if fetchResults.SuccessCount() != len(files) {
			result.ErrorMessage += strings.Join(fetchResults.Errors(), ", ")
			packager.CleanupChannel <- result
		} else {
			packager.BuildChannel <- result
		}
	}
}

// doBuild builds the DPN bag, creating all of the necessary
// tag files, manifests and directories. Data then goes into the
// TarChannel, so the bag can be tarred up.
func (packager *Packager) doBuild() {
	for result := range packager.BuildChannel {
		bag, err := result.BagBuilder.BuildBag()
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Error building bag: %v", err.Error())
			packager.CleanupChannel <- result
			continue
		}
		errors := bag.Write()
		if errors != nil && len(errors) > 0 {
			errMessages := strings.Join(errors, ", ")
			result.ErrorMessage += fmt.Sprintf("Error writing bag: %s", errMessages)
			packager.CleanupChannel <- result
			continue
		}
		packager.TarChannel <- result
	}
}

// doTar tars up the DPN bag and then sends data along to the
// CleanupChannel.
func (packager *Packager) doTar() {
	for result := range packager.TarChannel {
		// Figure out where the files are for this bag
		bagDir, err := packager.DPNBagDirectory(result)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Cannot get abs path for bag directory for bag %s: %s",
				result.BagIdentifier, err.Error())
			packager.CleanupChannel <- result
			continue
		}
		// Get the list of all files (manifests, tag files & payload)
		files, err := bagman.RecursiveFileList(bagDir)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Cannot get list of files in directory %s: %s",
				bagDir, err.Error())
			packager.CleanupChannel <- result
			continue
		}
		// The name of the tar file will be the DPN UUID + .tar
		tarFileName := fmt.Sprintf("%s.tar", result.BagBuilder.UUID)
		tarFilePath := filepath.Join(packager.ProcUtil.Config.DPNStagingDirectory,
			"bags", tarFileName)
		// Make sure the directory exists, then open a new tar file for writing
		err = os.MkdirAll(filepath.Dir(tarFilePath), 0755)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Cannot create directory %s: %s",
				filepath.Dir(tarFilePath), err.Error())
			packager.CleanupChannel <- result
			continue
		}
		tarFile, err := os.Create(tarFilePath)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf("Error creating tar file %s for bag %s: %v",
				tarFilePath, result.BagIdentifier, err)
			packager.CleanupChannel <- result
			continue
		}

		// Set up our tar writer, and put all items from the bag
		// directory into the tar file.
		tarWriter := tar.NewWriter(tarFile)
		pathSeparator := string(os.PathSeparator)
		for _, filePath := range files {
			pathWithinArchive := strings.Replace(filePath, bagDir, "", 1)
			if strings.HasPrefix(pathWithinArchive, pathSeparator) {
				pathWithinArchive = strings.Replace(pathWithinArchive, pathSeparator, "", 1)
			}
			err = bagman.AddToArchive(tarWriter, filePath, pathWithinArchive)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Error adding file %s to archive %s: %v",
					filePath, tarFilePath, err)
				tarFile.Close()
				tarWriter.Close()
				os.Remove(tarFilePath)
				packager.CleanupChannel <- result
				continue
			}
		}
		tarFile.Close()
		packager.CleanupChannel <- result
	}
}

// doCleanup cleans up the the directory containing all of the
// data files, tag files, manifests, etc. that went into the
// DPN bag. When this is done, the tar file will still be around,
// but the directories whose contents went into the tar file will
// be gone. From here, data goes into the PostProcessChannel.
func (packager *Packager) doCleanup() {
	log := packager.ProcUtil.MessageLog
	for result := range packager.CleanupChannel {
		if packager.shouldCleanup(result) {
			packager.cleanup(result)
		}
		result.Retry = packager.shouldRetry(result)
		if result.Succeeded() {
			result.NsqMessage.Finish()
		} else {
			log.Info("Requeuing %s (%s)", result.BagIdentifier, result.BagBuilder.LocalPath)
			result.NsqMessage.Requeue(1 * time.Minute)
		}
		packager.PostProcessChannel <- result
	}
}

// postProcess logs the results of our DPN bagging operation, tells
// NSQ that the worker is done with the job (whether successful or not),
// and sends data to the next NSQ topic for post-processing.
func (packager *Packager) postProcess() {
	for result := range packager.PostProcessChannel {
		if result.Succeeded() {
			packager.ProcUtil.MessageLog.Info("Bag %s successfully packaged at %s",
				result.BagIdentifier, result.TarFilePath)
			packager.ProcUtil.IncrementSucceeded()
			// TODO: Send to DPN storage queue
		} else {
			if packager.reachedMaxAttempts(result) {
				packager.ProcUtil.MessageLog.Error(result.ErrorMessage)
				packager.ProcUtil.IncrementFailed()
				// TODO: Send to DPN trouble queue
			} else {  // Failed, but we can still retry
				packager.ProcUtil.MessageLog.Warning(
					"Bag %s failed, but will retry. %s",
					result.BagIdentifier, result.ErrorMessage)
			}
		}
	}
}


//
// ----- END OF GO ROUTINES. SYNCHRONOUS FUNCTIONS FROM HERE DOWN -----
//


func (packager *Packager) reachedMaxAttempts(result *PackageResult) (bool) {
	return result.NsqMessage.Attempts >= uint16(packager.ProcUtil.Config.DPNPackageWorker.MaxAttempts)
}

// shouldCleanup tells us whether we should delete all of the files,
// except the tar files, for a DPN bag. See inline comments for the logic.
func (packager *Packager) shouldCleanup(result *PackageResult) (cleanItUp bool) {
	log := packager.ProcUtil.MessageLog
	downloadStarted := result.DPNFetchResults != nil && len(result.DPNFetchResults) > 0
	cleanItUp = false

	if result.Succeeded() && result.TarFilePath != "" {
		// We have the tar file & no longer need the untarred files.
		cleanItUp = true
	} else if packager.reachedMaxAttempts(result) {
		// Failed, and we're done with this bag.
		cleanItUp = true
		result.ErrorMessage += fmt.Sprintf("Processing failed after %d attempts.", result.NsqMessage.Attempts)
	} else if downloadStarted == false {
		// No use leaving an empty directory laying around.
		cleanItUp = true
	} else {
		log.Info("Skipping cleanup on bag %s at %s. Leaving files in place for retry.",
			result.BagIdentifier, result.BagBuilder.LocalPath)
	}

	if cleanItUp {
		log.Info("Cleaning up bag %s at %s", result.BagIdentifier, result.BagBuilder.LocalPath)
	}

	// If we get to this point, leave cleanItUp = false, because
	// we've downloaded some files, and we still have some retries
	// left. This may be a bag with 1000 files, and we may already
	// have 900 of them on disk. The next retry will resume dowloading
	// at file #901, which is what we want. The only downside here is
	// that we might be using up a lot of disk space.
	return cleanItUp
}

func (packager *Packager) shouldRetry(result *PackageResult) (retry bool) {
	retry = true
	if result.Succeeded() && result.TarFilePath != "" {
		retry = false
	} else if packager.reachedMaxAttempts(result) {
		retry = false
	}
	return retry
}

func (packager *Packager) cleanup(result *PackageResult) {
	bagDir, err := packager.DPNBagDirectory(result)
	if err != nil {
		result.ErrorMessage += fmt.Sprintf("Cannot get abs path for bag directory: %s", err.Error())
		packager.ProcUtil.MessageLog.Error("Error cleaning up %s: %v", bagDir, err.Error())
		return
	}
	if strings.Index(bagDir, result.BagIdentifier) < 0 {
		packager.ProcUtil.MessageLog.Error("Skipping clean-up because bagDir %s looks suspicious", bagDir)
		return
	}
	err = os.RemoveAll(bagDir)
	if err != nil {
		packager.ProcUtil.MessageLog.Error("Error cleaning up %s: %v", bagDir, err)
	}
	packager.ProcUtil.Volume.Release(uint64(result.BagBuilder.IntellectualObject.TotalFileSize() * 2))
}

// Returns the path to the directory where we will build the DPN bag.
// If the DPN staging dir is at /mnt/dpn, and the bag we're restoring
// has the identifier test.edu/my_bag, this will return
// /mnt/dpn/test.edu/my_bag
func (packager *Packager) DPNBagDirectory(result *PackageResult) (string, error) {
	return filepath.Abs(filepath.Join(
		packager.ProcUtil.Config.DPNStagingDirectory,
		result.BagIdentifier))
}

func (packager *Packager) FilesToFetch(result *PackageResult) ([]*bagman.GenericFile, error) {
	alreadyFetched, err := packager.FilesAlreadyFetched(result)
	if err != nil {
		return nil, err
	}
	filesToFetch := make([]*bagman.GenericFile, 0)
	for _, gf := range result.BagBuilder.IntellectualObject.GenericFiles {
		_, alreadyOnDisk := alreadyFetched[gf.Identifier]
		if !alreadyOnDisk {
			filesToFetch = append(filesToFetch, gf)
		}
	}
	return filesToFetch, nil
}

func (packager *Packager) FilesAlreadyFetched(result *PackageResult) (map[string]bool, error) {
	// Get a list of all files we've already fetched.
	// These would have been fetched in a prior run
	// that eventually errored out. Maybe we have 50
	// of the 100 files we need for a bag.
	files, err := bagman.RecursiveFileList(result.BagBuilder.LocalPath)
	if err != nil {
		return nil, err
	}
	// Convert the absolute paths returned by RecursiveFileList
	// to GenericFile.Identifiers.
	gfIdentifiers := make(map[string]bool, 0)
	for _, f := range files {
		identifier := strings.Replace(f,
			result.BagBuilder.LocalPath,
			result.BagIdentifier, 1)
		fmt.Println(identifier)
		gfIdentifiers[identifier] = true
	}
	return gfIdentifiers, err
}
