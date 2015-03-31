package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os"
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

type PackageStatus struct {
	BagIdentifier   string
	NsqMessage      *nsq.Message `json:"-"`
	BagBuilder      *BagBuilder
	DPNFetchResults []*DPNFetchResult
	TarFilePath     string
	ErrorMessage    string
	Retry           bool
}

func (status *PackageStatus) Errors() ([]string) {
	errors := make([]string, 0)
	if status.ErrorMessage != "" {
		errors = append(errors, status.ErrorMessage)
	}
	if status.BagBuilder.ErrorMessage != "" {
		errors = append(errors, status.BagBuilder.ErrorMessage)
	}
	for _, result := range status.DPNFetchResults {
		if result.FetchResult.ErrorMessage != "" {
			errors = append(errors, result.FetchResult.ErrorMessage)
		}
	}
	return errors
}

func (status *PackageStatus) Succeeded() (bool) {
	return status.TarFilePath != "" && len(status.Errors()) == 0
}

type Packager struct {
	LookupChannel       chan *PackageStatus
	FetchChannel        chan *PackageStatus
	BuildChannel        chan *PackageStatus
	TarChannel          chan *PackageStatus
	CleanupChannel      chan *PackageStatus
	PostProcessChannel  chan *PackageStatus
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
	packager.FetchChannel = make(chan *PackageStatus, fetcherBufferSize)
	packager.BuildChannel = make(chan *PackageStatus, workerBufferSize)
	packager.TarChannel = make(chan *PackageStatus, workerBufferSize)
	packager.CleanupChannel = make(chan *PackageStatus, workerBufferSize)
	packager.PostProcessChannel = make(chan *PackageStatus, workerBufferSize)
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

	var packageStatus *PackageStatus
	err := json.Unmarshal(message.Body, packageStatus)
	if err != nil {
		detailedError := fmt.Errorf("Could not unmarshal JSON data from nsq:",
			string(message.Body))
		packager.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// Start processing.
	packager.LookupChannel <- packageStatus
	packager.ProcUtil.MessageLog.Info("Put %s into lookup channel",
		packageStatus.BagIdentifier)
	return nil
}

// doLookup gets information about the intellectual object from
// Fluctus, builds a package status object, and moves the data
// into the FetchChannel.
func (packager *Packager) doLookup() {
	for status := range packager.LookupChannel {
		if status.BagBuilder != nil {
			// We started work on this bag before. No need to look it up again.
			packager.FetchChannel <- status
			continue
		}

		// Get the bag, with a list of GenericFiles
		intelObj, err := packager.ProcUtil.FluctusClient.IntellectualObjectGet(status.BagIdentifier, true)
		if err != nil {
			// FAIL - Can't get intel obj data (HTTP or Fluctus error)
			status.ErrorMessage = fmt.Sprintf("Could not fetch info about IntellectualObject " +
				"'%s' from Fluctus: %s", status.BagIdentifier, err.Error())
			status.Retry = true
			packager.PostProcessChannel <- status
			continue
		}
		if intelObj == nil {
			// FAIL - Can't get intel obj data (Object not found)
			status.ErrorMessage = fmt.Sprintf("Fluctus returned nil for IntellectualObject %s",
				status.BagIdentifier)
			status.Retry = true
			packager.PostProcessChannel <- status
			continue
		}
		err = packager.ProcUtil.Volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
		if err != nil {
			// FAIL - Not enough disk space in staging area to build this bag
			packager.ProcUtil.MessageLog.Warning("Requeueing bag %s, %d bytes - not enough disk space",
				status.BagIdentifier, intelObj.TotalFileSize())
			status.ErrorMessage = err.Error()
			status.Retry = true
			packager.PostProcessChannel <- status
			continue
		} else {
			// Woo-hoo!
			status.BagBuilder = NewBagBuilder(packager.ProcUtil.Config.DPNStagingDirectory,
				intelObj, packager.DefaultMetadata)
			packager.FetchChannel <- status
		}
	}
}


// doFetch fetches the IntellectualObject's files from S3 and
// stores them locally. Data then goes into the BuildChannel
// so we can build the DPN bag.
//
// TODO: If we really want to resume fetching, we'll
// need to have a constant UUID for the DPN bag. That
// UUID is assigned in NewBagBuilder. We'll have to assign
// it beforehand. Here's the case:
//
// - We fetched 50 of 100 files for DPN bag X, then we fail & requeue.
// - We have the 50 files we fetched still on the local disk.
// - A new worker picks up the the requeued task. It needs to know
//   where to look to find the 50 files. But the new BagBuilder has
//   assigned this bag UUID Y. We'll look in the wrong directory
//   (Y instead of X). Maybe we should use the DPN bag identifier
//   as the dirname? If we tar it up that way, and change the
//   tar file name at the end to the UUID, then when the bag is
//   untarred, it comes out as test.edu/my_bag. That could be a
//   benefit.
func (packager *Packager) doFetch() {
	// for status := range packager.FetchChannel {

	// }
}

func (packager *Packager) filesAlreadyFetched(status *PackageStatus) ([]string, error) {
	// Get a list of all files we've already fetched.
	// These would have been fetched in a prior run
	// that eventually errored out. Maybe we have 50
	// of the 100 files we need for a bag.
	files, err := bagman.RecursiveFileList(status.BagBuilder.LocalPath)
	if err != nil {
		return nil, err
	}
	// Convert the absolute paths returned by RecursiveFileList
	// to GenericFile.Identifiers.
	gfIdentifiers := make([]string, len(files))
	for i, f := range files {
		gfIdentifiers[i] = strings.Replace(f,
			status.BagBuilder.LocalPath,
			status.BagIdentifier, 1)
	}
	return gfIdentifiers, err
}

// doBuild builds the DPN bag, creating all of the necessary
// tag files, manifests and directories. Data then goes into the
// TarChannel, so the bag can be tarred up.
func (packager *Packager) doBuild() {
	// for status := range packager.BuildChannel {

	// }
}

// doTar tars up the DPN bag and then sends data along to the
// CleanupChannel.
func (packager *Packager) doTar() {
	// for status := range packager.TarChannel {

	// }
}

// doCleanup cleans up the the directory containing all of the
// data files, tag files, manifests, etc. that went into the
// DPN bag. When this is done, the tar file will still be around,
// but the directories whose contents went into the tar file will
// be gone. From here, data goes into the PostProcessChannel.
func (packager *Packager) doCleanup() {
	log := packager.ProcUtil.MessageLog
	for status := range packager.CleanupChannel {
		if packager.shouldCleanup(status) {
			packager.cleanup(status)
		}
		status.Retry = packager.shouldRetry(status)
		if status.Succeeded() {
			status.NsqMessage.Finish()
		} else {
			log.Info("Requeuing %s (%s)", status.BagIdentifier, status.BagBuilder.LocalPath)
			status.NsqMessage.Requeue(1 * time.Minute)
		}
		packager.PostProcessChannel <- status
	}
}

func (packager *Packager) reachedMaxAttempts(status *PackageStatus) (bool) {
	return status.NsqMessage.Attempts >= uint16(packager.ProcUtil.Config.DPNPackageWorker.MaxAttempts)
}

// shouldCleanup tells us whether we should delete all of the files,
// except the tar files, for a DPN bag. See inline comments for the logic.
func (packager *Packager) shouldCleanup(status *PackageStatus) (cleanItUp bool) {
	log := packager.ProcUtil.MessageLog
	downloadStarted := status.DPNFetchResults != nil && len(status.DPNFetchResults) > 0
	cleanItUp = false

	if status.Succeeded() && status.TarFilePath != "" {
		// We have the tar file & no longer need the untarred files.
		cleanItUp = true
	} else if packager.reachedMaxAttempts(status) {
		// Failed, and we're done with this bag.
		cleanItUp = true
		status.ErrorMessage += fmt.Sprintf("Processing failed after %d attempts.", status.NsqMessage.Attempts)
	} else if downloadStarted == false {
		// No use leaving an empty directory laying around.
		cleanItUp = true
	} else {
		log.Info("Skipping cleanup on bag %s at %s. Leaving files in place for retry.",
			status.BagIdentifier, status.BagBuilder.LocalPath)
	}

	if cleanItUp {
		log.Info("Cleaning up bag %s at %s", status.BagIdentifier, status.BagBuilder.LocalPath)
	}

	// If we get to this point, leave cleanItUp = false, because
	// we've downloaded some files, and we still have some retries
	// left. This may be a bag with 1000 files, and we may already
	// have 900 of them on disk. The next retry will resume dowloading
	// at file #901, which is what we want. The only downside here is
	// that we might be using up a lot of disk space.
	return cleanItUp
}

func (packager *Packager) shouldRetry(status *PackageStatus) (retry bool) {
	retry = true
	if status.Succeeded() && status.TarFilePath != "" {
		retry = false
	} else if packager.reachedMaxAttempts(status) {
		retry = false
	}
	return retry
}

func (packager *Packager) cleanup(status *PackageStatus) {
	err := os.RemoveAll(status.BagBuilder.LocalPath)
	if err != nil {
		packager.ProcUtil.MessageLog.Error("Error cleaning up %s: %v",
			status.BagBuilder.LocalPath, err)
	} else {
		packager.ProcUtil.Volume.Release(uint64(status.BagBuilder.IntellectualObject.TotalFileSize() * 2))
	}
}

// postProcess logs the results of our DPN bagging operation, tells
// NSQ that the worker is done with the job (whether successful or not),
// and sends data to the next NSQ topic for post-processing.
func (packager *Packager) postProcess() {
	for status := range packager.PostProcessChannel {
		if status.Succeeded() {
			packager.ProcUtil.MessageLog.Info("Bag %s successfully packaged at %s",
				status.BagIdentifier, status.TarFilePath)
			packager.ProcUtil.IncrementSucceeded()
			// TODO: Send to DPN storage queue
		} else {
			if packager.reachedMaxAttempts(status) {
				packager.ProcUtil.MessageLog.Error(status.ErrorMessage)
				packager.ProcUtil.IncrementFailed()
				// TODO: Send to DPN trouble queue
			} else {  // Failed, but we can still retry
				packager.ProcUtil.MessageLog.Warning(
					"Bag %s failed, but will retry. %s",
					status.BagIdentifier, status.ErrorMessage)
			}
		}
	}
}
