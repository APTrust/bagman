package dpn

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

type Packager struct {
	LookupChannel       chan *DPNResult
	FetchChannel        chan *DPNResult
	BuildChannel        chan *DPNResult
	TarChannel          chan *DPNResult
	CleanupChannel      chan *DPNResult
	PostProcessChannel  chan *DPNResult
	DPNConfig           *DPNConfig
	ProcUtil            *bagman.ProcessUtil
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewPackager(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Packager) {
	packager := &Packager {
		DPNConfig: dpnConfig,
		ProcUtil: procUtil,
	}

	workerBufferSize := procUtil.Config.DPNPackageWorker.Workers * 4
	packager.LookupChannel = make(chan *DPNResult, workerBufferSize)
	packager.BuildChannel = make(chan *DPNResult, workerBufferSize)
	packager.TarChannel = make(chan *DPNResult, workerBufferSize)
	packager.CleanupChannel = make(chan *DPNResult, workerBufferSize)
	packager.PostProcessChannel = make(chan *DPNResult, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go packager.doLookup()
		go packager.doBuild()
		go packager.doTar()
		go packager.doCleanup()
		go packager.postProcess()
	}

	fetcherBufferSize := procUtil.Config.DPNPackageWorker.NetworkConnections * 4
	packager.FetchChannel = make(chan *DPNResult, fetcherBufferSize)
	for i := 0; i <  procUtil.Config.DPNPackageWorker.NetworkConnections; i++ {
		go packager.doFetch()
	}
	return packager
}

// MessageHandler handles messages from NSQ, putting each
// item into the pipleline.
func (packager *Packager) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	// TODO: Change this. We'll actually just have the bag identifier in the queue.
	result := &DPNResult{}
	err := json.Unmarshal(message.Body, result)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		packager.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Start processing.
	result.NsqMessage = message
	result.Stage = STAGE_PACKAGE

	// Fluctus housekeeping
	if result.ProcessedItemId != 0 {
		processedItem, err := packager.ProcUtil.FluctusClient.GetBagStatusById(result.ProcessedItemId)
		if err != nil {
			errMessage := fmt.Sprintf("Could not get ProcessedItem with id %d from Fluctus",
				result.ProcessedItemId)
			packager.ProcUtil.MessageLog.Error(errMessage)
			message.Attempts += 1
			message.Requeue(1 * time.Minute)
			return fmt.Errorf(errMessage)
		}
		result.processStatus = processedItem
		result.processStatus.Status = bagman.StatusStarted
		result.processStatus.SetNodePidState(result, packager.ProcUtil.MessageLog)
		err = packager.ProcUtil.FluctusClient.UpdateProcessedItem(result.processStatus)
		if err != nil {
			errorMessage := fmt.Sprintf("Before processing '%s', cannot send status " +
				"back to Fluctus: %v", result.BagIdentifier, err)
			packager.ProcUtil.MessageLog.Error(errorMessage)
			message.Attempts += 1
			message.Requeue(1 * time.Minute)
			return fmt.Errorf(errorMessage)
		}
	}

	packager.LookupChannel <- result
	packager.ProcUtil.MessageLog.Info("Put %s into lookup channel",
		result.BagIdentifier)
	return nil
}

// doLookup gets information about the intellectual object from
// Fluctus, builds a package result object, and moves the data
// into the FetchChannel.
//
// We should not have to check whether this bag already exists in DPN
// because the packager only deals with bags ingested at APTrust, and
// the APTrust web UI won't even show the "Send to DPN" button if the
// item is already in DPN.
func (packager *Packager) doLookup() {
	for result := range packager.LookupChannel {
		// Get the bag, with a list of GenericFiles
		intelObj, err := packager.ProcUtil.FluctusClient.IntellectualObjectGet(result.BagIdentifier, true)
		if err != nil {
			// FAIL - Can't get intel obj data (HTTP or Fluctus error)
			result.ErrorMessage += fmt.Sprintf("Could not fetch info about IntellectualObject " +
				"'%s' from Fluctus: %s", result.BagIdentifier, err.Error())
			result.Retry = true
			packager.PostProcessChannel <- result
			continue
		}
		if intelObj == nil {
			// FAIL - Can't get intel obj data (Object not found)
			result.ErrorMessage += fmt.Sprintf("Fluctus returned nil for IntellectualObject %s",
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
			result.ErrorMessage += err.Error()
			result.Retry = true
			packager.PostProcessChannel <- result
			continue
		} else {
			inst, _ := bagman.GetInstitutionFromBagIdentifier(result.BagIdentifier)
			dir, err := filepath.Abs(filepath.Join(
				packager.ProcUtil.Config.DPNStagingDirectory, inst))
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Cannot get absolute path for bag directory: %s",
					err.Error())
				result.Retry = true
				packager.PostProcessChannel <- result
				continue
			}
			// Woo-hoo!
			builder, err := NewBagBuilder(dir, intelObj, packager.DPNConfig.DefaultMetadata)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Error creating BagBuilder: %s",
					err.Error())
				result.Retry = true
				packager.PostProcessChannel <- result
				continue
			}
			result.PackageResult.BagBuilder = builder
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
		result.FetchResults = fetchResults
		if err != nil {
			result.ErrorMessage += err.Error()
			packager.CleanupChannel <- result
		} else if fetchResults.SuccessCount() != len(files) {
			result.ErrorMessage += strings.Join(fetchResults.Errors(), ", ")
			packager.CleanupChannel <- result
		} else  {
			packager.BuildChannel <- result
		}
	}
}

// doBuild builds the DPN bag, creating all of the necessary
// tag files, manifests and directories. Data then goes into the
// TarChannel, so the bag can be tarred up. This process can take
// quite a while, so we poke NSQ several times to let it know
// we're still here.
func (packager *Packager) doBuild() {
	for result := range packager.BuildChannel {
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		// Add files to bag before saving.
		for i := range result.FetchResults.Items {
			fetchResult := result.FetchResults.Items[i]
			sourcePath := fetchResult.FetchResult.LocalFile

			// Ignoring err return. If this bag has been ingested,
			// we know the identifier is valid.
			pathInBag, _ := fetchResult.GenericFile.OriginalPath()

			if strings.HasPrefix(pathInBag, "data/") {
				// This is in the data dir, so it's a normal payload file.
				pathWithoutDataPrefix := strings.Replace(pathInBag, "data/", "", 1)
				result.PackageResult.BagBuilder.Bag.AddFile(sourcePath, pathWithoutDataPrefix)
			} else if !strings.Contains(pathInBag, "/") {
				// This is in the root dir, so it's a top-level tag file,
				// which the DPN spec does not specifically allow or prohibit,
				// but the DPN bag validators at other nodes will probably reject
				// the bag if we leave this file in the top-level dir.
				// Put this in a custom tag dir.
				pathInBag = fmt.Sprintf("aptrust-tags/%s", pathInBag)
				result.PackageResult.BagBuilder.Bag.AddCustomTagfile(sourcePath, pathInBag, true)
			} else {
				// This is not in the top-level dir or in the data dir.
				// It's a custom tag file in a custom tag dir. Just put
				// it in as-is.
				result.PackageResult.BagBuilder.Bag.AddCustomTagfile(sourcePath, pathInBag, true)
			}
		}

		errors := result.PackageResult.BagBuilder.Bag.Save()
		if errors != nil && len(errors) > 0 {
			errMessages := ""
			for _, e := range errors {
				errMessages = fmt.Sprintf("%s %s ", errMessages, e.Error())
			}
			result.ErrorMessage += fmt.Sprintf("Error writing bag: %s", errMessages)
			packager.CleanupChannel <- result
			continue
		}
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		packager.TarChannel <- result
	}
}

// doTar tars up the DPN bag and then sends data along to the
// CleanupChannel.
func (packager *Packager) doTar() {
	for result := range packager.TarChannel {

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

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
		tarFileName := fmt.Sprintf("%s.tar", result.PackageResult.BagBuilder.UUID)
		tarFilePath := filepath.Join(packager.ProcUtil.Config.DPNStagingDirectory,
			tarFileName)
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
		for _, filePath := range files {
			pathWithinArchive, err := PathWithinArchive(result, filePath, bagDir)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf(
					"Cannot create base folder in tar archive: %v", err)
				tarFile.Close()
				tarWriter.Close()
				os.Remove(tarFilePath)
				packager.CleanupChannel <- result
				break
			}

			// The DPN spec at https://wiki.duraspace.org/display/DPN/BagIt+Specification
			// says the top-level folder within the bag should have the name of the DPN
			// Object Identifier (the UUID). So we replace <bag_name>/ with <uuid>/.
			parts := strings.Split(pathWithinArchive, "/")
			topLevelDirName := parts[0]
			pathWithinArchive = strings.Replace(pathWithinArchive, topLevelDirName,
				result.PackageResult.BagBuilder.UUID, 1)

			err = bagman.AddToArchive(tarWriter, filePath, pathWithinArchive)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Error adding file %s to archive %s: %v",
					filePath, tarFilePath, err)
				tarFile.Close()
				tarWriter.Close()
				os.Remove(tarFilePath)
				packager.CleanupChannel <- result
				break
			}
		}
		tarWriter.Flush()
		tarFile.Close()
		result.PackageResult.TarFilePath = tarFilePath

		// Calculate the checksums. We need the md5 for the put to S3,
		// and the sha256 for the bag record.
		fileDigest, err := bagman.CalculateDigests(result.PackageResult.TarFilePath)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("Could not calculate checksums on '%s': %v",
				result.PackageResult.TarFilePath, err)
			packager.CleanupChannel <- result
			continue
		}
		result.BagMd5Digest = fileDigest.Md5Digest
		result.BagSha256Digest = fileDigest.Sha256Digest
		result.BagSize = fileDigest.Size

		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}

		packager.CleanupChannel <- result
	}
}

// doCleanup cleans up the the directory containing all of the
// data files, tag files, manifests, etc. that went into the
// DPN bag. When this is done, the tar file will still be around,
// but the directories whose contents went into the tar file will
// be gone. From here, data goes into the PostProcessChannel.
func (packager *Packager) doCleanup() {
	for result := range packager.CleanupChannel {
		if packager.shouldCleanup(result) {
			packager.cleanup(result)
		}
		result.Retry = packager.shouldRetry(result)
		packager.PostProcessChannel <- result
	}
}

// postProcess logs the results of our DPN bagging operation, tells
// NSQ that the worker is done with the job (whether successful or not),
// and sends data to the next NSQ topic for post-processing.
func (packager *Packager) postProcess() {
	for result := range packager.PostProcessChannel {

		// Tell Fluctus what's up
		if result.processStatus != nil {
			result.processStatus.Retry = (
				result.PackageResult.Succeeded() == false &&
					packager.reachedMaxAttempts(result) == true)
			result.Retry = result.processStatus.Retry
			result.processStatus.SetNodePidState(result, packager.ProcUtil.MessageLog)
			result.processStatus.Node = ""
			result.processStatus.Pid = 0
			err := packager.ProcUtil.FluctusClient.UpdateProcessedItem(result.processStatus)
			if err != nil {
				errorMessage := fmt.Sprintf("After processing bag '%s', " +
					"cannot send status back to Fluctus: %v", result.BagIdentifier, err)
				packager.ProcUtil.MessageLog.Error(errorMessage)
			}
		}

		if result.PackageResult.Succeeded() {
			packager.ProcUtil.MessageLog.Info("Bag %s successfully packaged at %s",
				result.BagIdentifier, result.PackageResult.TarFilePath)
			packager.ProcUtil.IncrementSucceeded()
			// All's well. Send this into the storage queue, so
			// it will be uploaded to Glacier.
			if result.NsqMessage != nil {
				result.NsqMessage.Finish()
				SendToStorageQueue(result, packager.ProcUtil)
			}
		} else {
			if packager.reachedMaxAttempts(result) {
				packager.ProcUtil.MessageLog.Error(result.ErrorMessage)
				packager.ProcUtil.IncrementFailed()
				// Item failed after max attempts. Put in trouble queue
				// for admin review.
				if result.NsqMessage != nil {
					result.NsqMessage.Finish()
					SendToTroubleQueue(result, packager.ProcUtil)
				}
			} else {  // Failed, but we can still retry
				packager.ProcUtil.MessageLog.Warning(
					"Bag %s failed, but will retry. %s",
					result.BagIdentifier, result.ErrorMessage)
				if result.NsqMessage != nil {
					message := fmt.Sprintf("Requeuing %s",result.BagIdentifier)
					if result.PackageResult != nil && result.PackageResult.BagBuilder != nil {
						message = fmt.Sprintf("%s at path %s", message,
							result.PackageResult.BagBuilder.LocalPath)
					}
					packager.ProcUtil.MessageLog.Info(message)
					result.NsqMessage.Requeue(1 * time.Minute)
				}
			}
		}
		if result.NsqMessage == nil {
			// This is a test message, running outside production.
			packager.WaitGroup.Done()
		}

		packager.ProcUtil.LogStats()
	}
}


//
// ----- END OF GO ROUTINES. SYNCHRONOUS FUNCTIONS FROM HERE DOWN -----
//

func (packager *Packager) reachedMaxAttempts(result *DPNResult) (bool) {
	if result.NsqMessage == nil {
		// If no NSQ message, we're running RunTest() without NSQ
		return true
	}
	return result.NsqMessage.Attempts >= uint16(packager.ProcUtil.Config.DPNPackageWorker.MaxAttempts)
}

// shouldCleanup tells us whether we should delete all of the files,
// except the tar files, for a DPN bag. See inline comments for the logic.
func (packager *Packager) shouldCleanup(result *DPNResult) (cleanItUp bool) {
	log := packager.ProcUtil.MessageLog
	downloadStarted := result.PackageResult.DPNFetchResults != nil && len(result.PackageResult.DPNFetchResults) > 0
	cleanItUp = false

	if result.PackageResult.Succeeded() {
		// We have the tar file & no longer need the untarred files.
		cleanItUp = true
	} else if packager.reachedMaxAttempts(result) {
		// Failed, and we're done with this bag.
		cleanItUp = true
		result.ErrorMessage += " Processing failed after max attempts."
	} else if downloadStarted == false {
		// No use leaving an empty directory laying around.
		cleanItUp = true
	} else {
		log.Info("Skipping cleanup on bag %s at %s. Leaving files in place for retry.",
			result.BagIdentifier, result.PackageResult.BagBuilder.LocalPath)
	}

	if cleanItUp {
		log.Info("Cleaning up bag %s at %s", result.BagIdentifier, result.PackageResult.BagBuilder.LocalPath)
	}

	// If we get to this point, leave cleanItUp = false, because
	// we've downloaded some files, and we still have some retries
	// left. This may be a bag with 1000 files, and we may already
	// have 900 of them on disk. The next retry will resume dowloading
	// at file #901, which is what we want. The only downside here is
	// that we might be using up a lot of disk space.
	return cleanItUp
}

func (packager *Packager) shouldRetry(result *DPNResult) (retry bool) {
	retry = true
	if result.PackageResult.Succeeded() && result.PackageResult.TarFilePath != "" {
		retry = false
	} else if packager.reachedMaxAttempts(result) {
		retry = false
	}
	return retry
}

func (packager *Packager) cleanup(result *DPNResult) {
	stagingDir := packager.ProcUtil.Config.DPNStagingDirectory
	bagDir := result.PackageResult.BagBuilder.LocalPath
	if (!strings.HasPrefix(bagDir, stagingDir) ||
		(strings.HasPrefix(stagingDir, "/") && len(stagingDir) < 5)) {
	 	packager.ProcUtil.MessageLog.Error("Skipping clean-up because bagDir '%s' looks suspicious", bagDir)
		packager.ProcUtil.MessageLog.Error("bagDir should start with '%s', and '%s' should not be == '/'",
			stagingDir, stagingDir)
	 	return
	}
	err := os.RemoveAll(bagDir)
	if err != nil {
		packager.ProcUtil.MessageLog.Error("Error cleaning up %s: %v", bagDir, err)
	}
	packager.ProcUtil.Volume.Release(uint64(result.PackageResult.BagBuilder.IntellectualObject.TotalFileSize() * 2))
}

// Returns the path to the directory where we will build the DPN bag.
// If the DPN staging dir is at /mnt/dpn, and the bag we're restoring
// has the identifier test.edu/my_bag, this will return
// /mnt/dpn/test.edu/my_bag
func (packager *Packager) DPNBagDirectory(result *DPNResult) (string, error) {
	return filepath.Abs(filepath.Join(
		packager.ProcUtil.Config.DPNStagingDirectory,
		result.BagIdentifier))
}

func (packager *Packager) FilesToFetch(result *DPNResult) ([]*bagman.GenericFile, error) {
	alreadyFetched, err := packager.FilesAlreadyFetched(result)
	if err != nil {
		return nil, err
	}
	filesToFetch := make([]*bagman.GenericFile, 0)
	for _, gf := range result.PackageResult.BagBuilder.IntellectualObject.GenericFiles {
		_, alreadyOnDisk := alreadyFetched[gf.Identifier]
		if !alreadyOnDisk {
			filesToFetch = append(filesToFetch, gf)
		}
	}
	return filesToFetch, nil
}

func (packager *Packager) FilesAlreadyFetched(result *DPNResult) (map[string]bool, error) {
	// Get a list of all files we've already fetched.
	// These would have been fetched in a prior run
	// that eventually errored out. Maybe we have 50
	// of the 100 files we need for a bag.
	files, err := bagman.RecursiveFileList(result.PackageResult.BagBuilder.LocalPath)
	if err != nil {
		return nil, err
	}
	// Convert the absolute paths returned by RecursiveFileList
	// to GenericFile.Identifiers.
	gfIdentifiers := make(map[string]bool, 0)
	for _, f := range files {
		identifier := strings.Replace(f,
			result.PackageResult.BagBuilder.LocalPath,
			result.BagIdentifier, 1)
		gfIdentifiers[identifier] = true
	}
	return gfIdentifiers, err
}

// Packages the bag identified by bagIdentifier. This is for local dev
// testing. You still need to have Fluctus running to retrieve bag info,
// and you need to have your S3 environment or config vars set up.
// Run: `dpn_package_devtest -config=dev`
func (packager *Packager) RunTest(bagIdentifier string) (*DPNResult) {
	result := NewDPNResult(bagIdentifier)
	packager.WaitGroup.Add(1)
	packager.ProcUtil.MessageLog.Info("Putting %s into lookup channel",
		result.BagIdentifier)
	packager.LookupChannel <- result
	packager.WaitGroup.Wait()
	fmt.Println("Inspect the tar file output. It's your job to delete the file manually.")
	return result
}

func PathWithinArchive(result *DPNResult, filePath, bagDir string) (string, error) {
	// Figure out this file's base path within the archive.
	// It should be something like data/subdir/file1.pdf
	basePath := strings.Replace(filePath, bagDir, "", 1)
	pathSeparator := string(os.PathSeparator)
	if strings.HasPrefix(basePath, pathSeparator) {
		basePath = strings.Replace(basePath, pathSeparator, "", 1)
	}

	// The top-level folder within the archive should have the
	// same name as the original bag, so that when the bag is
	// restored and untarred, it produces a folder with a
	// meaningful name. So if a bag has identifier
	// "test.edu/ncsu.1840.16-1004", it should untar to a
	// directory called "ncsu.1840.16-1004" so the depositor
	// knows what bag they got back.
	bagName, err := result.OriginalBagName()
	if err != nil {
		return "", err
	}

	// The path within the arcive for data/subdir/file1.pdf
	// will be something like ncsu.1840.16-1004/data/subdir/file1.pdf
	pathWithinArchive := filepath.Join(bagName, basePath)
	return pathWithinArchive, nil
}
