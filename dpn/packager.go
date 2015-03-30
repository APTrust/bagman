package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os"
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
	CleanedUp       bool
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
	return status.CleanedUp == true && len(status.Errors()) == 0
}

type Packager struct {
	LookupChannel   chan *PackageStatus
	FetchChannel    chan *PackageStatus
	BuildChannel    chan *PackageStatus
	TarChannel      chan *PackageStatus
	CleanupChannel  chan *PackageStatus
	ResultsChannel  chan *PackageStatus
	DefaultMetadata *DefaultMetadata
	ProcUtil        *bagman.ProcessUtil
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
	packager.ResultsChannel = make(chan *PackageStatus, workerBufferSize)
	for i := 0; i < procUtil.Config.DPNPackageWorker.Workers; i++ {
		go packager.doLookup()
		go packager.doBuild()
		go packager.doTar()
		go packager.doCleanup()
		go packager.logResults()
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
			packager.ResultsChannel <- status
			continue
		}
		if intelObj == nil {
			// FAIL - Can't get intel obj data (Object not found)
			status.ErrorMessage = fmt.Sprintf("Fluctus returned nil for IntellectualObject %s",
				status.BagIdentifier)
			status.Retry = true
			packager.ResultsChannel <- status
			continue
		}
		err = packager.ProcUtil.Volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
		if err != nil {
			// FAIL - Not enough disk space in staging area to build this bag
			packager.ProcUtil.MessageLog.Warning("Requeueing bag %s, %d bytes - not enough disk space",
				status.BagIdentifier, intelObj.TotalFileSize())
			status.ErrorMessage = err.Error()
			status.Retry = true
			packager.ResultsChannel <- status
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
func (packager *Packager) doFetch() {
	// for status := range packager.FetchChannel {

	// }
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
// be gone. From here, data goes into the ResultsChannel.
func (packager *Packager) doCleanup() {
	for status := range packager.CleanupChannel {
		// BagBuilder.LocalPath is the absolute path to the
		// untarred bag. We want to delete that, but leave the
		// tar file. On success, wipe out that whole working dir.
		if status.Succeeded() && status.TarFilePath != "" {
			packager.cleanup(status)
			status.Retry = false
			status.NsqMessage.Finish()
		} else {
			// TODO: Move this finish/requeue code to logResults...
			if status.NsqMessage.Attempts >= uint16(packager.ProcUtil.Config.DPNPackageWorker.MaxAttempts) {
				status.Retry = true
				status.NsqMessage.Requeue(1 * time.Minute)
				packager.ProcUtil.MessageLog.Warning("Requeuing bag '%s' for attempt #%d",
					status.BagIdentifier, status.NsqMessage.Attempts + 1)
			} else {
				status.Retry = false
				status.NsqMessage.Finish()
				packager.ProcUtil.MessageLog.Error("Giving up on bag '%s' after %d attempts",
					status.BagIdentifier, status.NsqMessage.Attempts)
			}
		}
		// If we get here, something went wrong. If download started,
		// let's keep the downloaded files around so we can resume
		// where we left off on the next run.
		downloadStarted := status.DPNFetchResults != nil && len(status.DPNFetchResults) > 0
		if downloadStarted == false {
			// Clean out the directory, mark space as freed, and
			// maybe we'll start over if this is requeued.
			packager.cleanup(status)
		} else {
			// TODO: No, sucka! Requeu & resume only if we haven't hit MaxAttempts!
			packager.ProcUtil.MessageLog.Warning("Not cleaning up bag '%s': will resume processing later",
				status.BagIdentifier, status.NsqMessage.Attempts + 1)
		}
		packager.ResultsChannel <- status
	}
}

func (packager *Packager) cleanup(status *PackageStatus) {
	err := os.RemoveAll(status.BagBuilder.LocalPath)
	if err != nil {
		packager.ProcUtil.MessageLog.Error("Error cleaning up %s: %v",
			status.BagBuilder.LocalPath, err)
	} else {
		// We have a problem here, since we're reserving 2x bytes
		// and only freeing 1x. Need to have a smarter volume manager.
		packager.ProcUtil.Volume.Release(uint64(status.BagBuilder.IntellectualObject.TotalFileSize()))
	}
}

// logResults logs the results of our DPN bagging operation, tells
// NSQ that the worker is done with the job (whether successful or not),
// and sends data to the next NSQ topic for post-processing.
func (packager *Packager) logResults() {
	// for status := range packager.ResultsChannel {

	// }
}
