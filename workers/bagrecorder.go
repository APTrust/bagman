// bagrecorder records bag metadata in Fluctus.
// That includes metadata for Intellectual Objects,
// Generic Files and Premis Events.
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"os"
	"sync"
	"time"
)

type BagRecorder struct {
	FedoraChannel  chan *bagman.ProcessResult
	CleanupChannel chan *bagman.ProcessResult
	ResultsChannel chan *bagman.ProcessResult
	ProcUtil       *bagman.ProcessUtil
	UsingNsq       bool
	WaitGroup      sync.WaitGroup
}

func NewBagRecorder(procUtil *bagman.ProcessUtil) (*BagRecorder) {
	bagRecorder := &BagRecorder {
		ProcUtil: procUtil,
		UsingNsq: true,
	}
	workerBufferSize := procUtil.Config.RecordWorker.Workers * 10
	bagRecorder.FedoraChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	bagRecorder.CleanupChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	bagRecorder.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	for i := 0; i < procUtil.Config.RecordWorker.Workers; i++ {
		go bagRecorder.recordInFedora()
		go bagRecorder.logResult()
		go bagRecorder.doCleanup()
	}
	return bagRecorder
}

func (bagRecorder *BagRecorder) RunWithoutNsq(result *bagman.ProcessResult) {
	bagRecorder.UsingNsq = false
	bagRecorder.WaitGroup.Add(1) // Marked as done in doCleanup() below
	bagRecorder.FedoraChannel <- result
	bagRecorder.ProcUtil.MessageLog.Debug("Put %s into Fluctus channel", result.S3File.Key.Key)
	bagRecorder.WaitGroup.Wait()
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagRecorder *BagRecorder) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		bagRecorder.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message
	bagRecorder.FedoraChannel <- &result
	bagRecorder.ProcUtil.MessageLog.Debug("Put %s into Fluctus channel", result.S3File.Key.Key)
	return nil
}

func (bagRecorder *BagRecorder) recordInFedora() {
	for result := range bagRecorder.FedoraChannel {
		bagRecorder.ProcUtil.MessageLog.Info("Recording Fedora metadata for %s",
			result.S3File.Key.Key)
		// result.NsqMessage will be nil when the process that uses
		// this library does not deal with NSQ. E.g. apps/apt_retry
		if result.NsqMessage != nil {
			result.NsqMessage.Touch()
		}
		result.Stage = "Record"
		bagRecorder.updateFluctusStatus(result, bagman.StageRecord, bagman.StatusStarted)
		// Save to Fedora only if there are new or updated items in this bag.
		// TODO: What if some items were deleted?
		if result.TarResult.AnyFilesNeedSaving() {
			err := bagRecorder.recordAllFedoraData(result)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf(" %s", err.Error())
			}
			if result.FedoraResult.AllRecordsSucceeded() == false {
				result.ErrorMessage += " When recording IntellectualObject, GenericFiles and " +
					"PremisEvents, one or more calls to Fluctus failed."
			}
			if result.ErrorMessage == "" {
				bagRecorder.ProcUtil.MessageLog.Info("Successfully recorded Fedora metadata for %s",
					result.S3File.Key.Key)
			} else {
				// If any errors in occur while talking to Fluctus,
				// we'll want to requeue and try again. Just leave
				// the result.Retry flag alone, and that will happen.
				bagRecorder.ProcUtil.MessageLog.Error(result.ErrorMessage)
			}
		} else {
			bagRecorder.ProcUtil.MessageLog.Info(
				"Nothing to update for %s: no items changed since last ingest.",
				result.S3File.Key.Key)
		}
		bagRecorder.updateFluctusStatus(result, bagman.StageRecord, bagman.StatusPending)
		bagRecorder.ResultsChannel <- result
	}
}

func (bagRecorder *BagRecorder) logResult() {
	for result := range bagRecorder.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			bagRecorder.ProcUtil.MessageLog.Error(err.Error())
		}
		bagRecorder.ProcUtil.JsonLog.Println(string(json))

		// Add a message to the message log
		if result.ErrorMessage != "" {
			bagRecorder.ProcUtil.IncrementFailed()
			bagRecorder.ProcUtil.MessageLog.Error("%s %s -> %s",
				result.S3File.BucketName,
				result.S3File.Key.Key,
				result.ErrorMessage)
		} else {
			bagRecorder.QueueItemsForReplication(result)
			bagRecorder.ProcUtil.IncrementSucceeded()
			bagRecorder.ProcUtil.MessageLog.Info("%s -> finished OK", result.S3File.Key.Key)
		}

		// Add some stats to the message log
		bagRecorder.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
			bagRecorder.ProcUtil.Succeeded(), bagRecorder.ProcUtil.Failed())

		if result.NsqMessage != nil &&
			result.NsqMessage.Attempts >= uint16(bagRecorder.ProcUtil.Config.RecordWorker.MaxAttempts) &&
			result.ErrorMessage != "" {
			result.Retry = false
			result.ErrorMessage += fmt.Sprintf(" Failure is due to a technical error "+
				"in Fedora. Giving up after %d failed attempts. This item has been "+
				"queued for administrative review. ",
				result.NsqMessage.Attempts)
			err = bagman.Enqueue(bagRecorder.ProcUtil.Config.NsqdHttpAddress,
				bagRecorder.ProcUtil.Config.TroubleWorker.NsqTopic, result)
			if err != nil {
				bagRecorder.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
					result.S3File.Key.Key, err)
			} else {
				bagRecorder.ProcUtil.MessageLog.Warning("Sent '%s' to trouble queue",
					result.S3File.Key.Key)
			}
		}

		// Delete the bag from the receving bucket,
		// and tell NSQ and the fluctopus what happened
		bagRecorder.CleanupChannel <- result
	}
}

func (bagRecorder *BagRecorder) QueueItemsForReplication(result *bagman.ProcessResult) {
	if result.NsqMessage == nil {
		// We're running without NSQ
		return
	}
	bagRecorder.ProcUtil.MessageLog.Info("Queueing %d files for replication",
		len(result.TarResult.Files))
	itemsQueued := 0
	for _, file := range result.TarResult.Files {
		err := bagman.Enqueue(
			bagRecorder.ProcUtil.Config.NsqdHttpAddress,
			bagRecorder.ProcUtil.Config.ReplicationWorker.NsqTopic,
			file)
		if err != nil {
			bagRecorder.ProcUtil.MessageLog.Error(
				"Error queueing %s for replication: %v",
				file.Identifier,
				err)
			result.ErrorMessage += fmt.Sprintf("%s | ", err.Error())
		} else {
			itemsQueued++
		}
	}
	message := fmt.Sprintf(
		"Queued %d of %d files for replication",
		itemsQueued,
		len(result.TarResult.Files))
	if itemsQueued < len(result.TarResult.Files) {
		bagRecorder.ProcUtil.MessageLog.Warning(message)
	} else {
		bagRecorder.ProcUtil.MessageLog.Info(message)
	}
}

func (bagRecorder *BagRecorder) updateFluctusStatus(result *bagman.ProcessResult, stage bagman.StageType, status bagman.StatusType) {
	bagRecorder.ProcUtil.MessageLog.Debug("Setting Ingest status to %s/%s for %s",
		stage, status, result.S3File.Key.Key)
	ingestStatus := result.IngestStatus(bagRecorder.ProcUtil.MessageLog)
	ingestStatus.Stage = stage
	ingestStatus.Status = status
	if status == bagman.StatusFailed || (stage == bagman.StageCleanup && status == bagman.StatusSuccess) {
		ingestStatus.Node = ""
		ingestStatus.Pid = 0
	} else {
		hostname := "hostname?"
		hostname, _ = os.Hostname()
		ingestStatus.Node = hostname
		ingestStatus.Pid = os.Getpid()
	}
	err := bagRecorder.ProcUtil.FluctusClient.SendProcessedItem(ingestStatus)
	if err != nil {
		result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
			"item status returned error %v. ", err)
		bagRecorder.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s",
			err.Error())
	}
}

func (bagRecorder *BagRecorder) doCleanup() {
	for result := range bagRecorder.CleanupChannel {
		if result.ErrorMessage == "" {
			bagRecorder.ProcUtil.MessageLog.Info("Cleaning up %s", result.S3File.Key.Key)
			bagRecorder.DeleteS3File(result)
		} else {
			bagRecorder.ProcUtil.MessageLog.Info("Leaving %s in %s because of error: %s",
				result.S3File.Key.Key, result.S3File.BucketName, result.ErrorMessage)
		}
		ingestStatus := result.IngestStatus(bagRecorder.ProcUtil.MessageLog)
		bagRecorder.updateFluctusStatus(result, ingestStatus.Stage, ingestStatus.Status)

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.NsqMessage != nil {
			if result.ErrorMessage != "" && result.Retry == true {
				bagRecorder.ProcUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
				result.NsqMessage.Requeue(1 * time.Minute)
			} else {
				result.NsqMessage.Finish()
			}
		}
		if bagRecorder.UsingNsq == false {
			bagRecorder.WaitGroup.Done()
		}
	}
}

// Send all metadata about the bag to Fluctus/Fedora. This includes
// the IntellectualObject, the GenericFiles, and all PremisEvents
// related to the object and the files.
func (bagRecorder *BagRecorder) recordAllFedoraData(result *bagman.ProcessResult) (err error) {
	intellectualObject, err := result.IntellectualObject()
	if err != nil {
		return err
	}
	result.FedoraResult = bagman.NewFedoraResult(
		intellectualObject.Identifier,
		result.TarResult.FilePaths())
	existingObj, err := bagRecorder.ProcUtil.FluctusClient.IntellectualObjectGet(
		intellectualObject.Identifier, true)
	if err != nil {
		result.FedoraResult.ErrorMessage = fmt.Sprintf(
			"[ERROR] Error checking Fluctus for existing IntellectualObject '%s': %v",
			intellectualObject.Identifier, err)
		return err
	}
	if existingObj != nil {
		bagRecorder.ProcUtil.MessageLog.Debug("Updating object %s", intellectualObject.Identifier)
		err = bagRecorder.fedoraUpdateObject(result, existingObj, intellectualObject)
		if err != nil {
			return err
		}
	} else if existingObj == nil && len(intellectualObject.GenericFiles) > bagman.MAX_FILES_FOR_CREATE {
		// Create the object with the first 500 files.
		// Call update for the rest.
		bagRecorder.ProcUtil.MessageLog.Debug("Creating new object %s with %d files (multi-step)",
			intellectualObject.Identifier, len(intellectualObject.GenericFiles))
		newObj, err := bagRecorder.fedoraCreateObject(result, intellectualObject, bagman.MAX_FILES_FOR_CREATE)
		if err != nil {
			return err
		}
		err = bagRecorder.fedoraUpdateObject(result, newObj, intellectualObject)
		if err != nil {
			return err
		}
	}else {
		// New IntellectualObject with < 500 files.
		// Do one-step create.
		bagRecorder.ProcUtil.MessageLog.Debug("Creating new object %s with %d files (single-step)",
			intellectualObject.Identifier, len(intellectualObject.GenericFiles))
		_, err = bagRecorder.fedoraCreateObject(result, intellectualObject, bagman.MAX_FILES_FOR_CREATE)
	}
	return err
}

// Creates a new IntellectualObject in Fedora with up to
// maxGenericFiles in a single call.
func (bagRecorder *BagRecorder) fedoraCreateObject(result *bagman.ProcessResult, intellectualObject *bagman.IntellectualObject, maxGenericFiles int) (*bagman.IntellectualObject, error) {
	result.FedoraResult.IsNewObject = true
	newObj, err := bagRecorder.ProcUtil.FluctusClient.IntellectualObjectCreate(intellectualObject, maxGenericFiles)
	if err != nil {
		result.FedoraResult.ErrorMessage = fmt.Sprintf(
			"[ERROR] Error creating new IntellectualObject '%s' in Fluctus: %v",
			intellectualObject.Identifier, err)
		return nil, err
	}
	return newObj, nil
}

// Update generic files, checksums and events in Fedora for an
// existing intellectual object. Param existingObject is the
// record Fluctus already has of this intellectual object.
// Param objectToSave is the record we want to save. We do some
// comparison between the two to make sure we don't save files
// that have not changed, or create new events for files that have
// not changed.
func (bagRecorder *BagRecorder) fedoraUpdateObject(result *bagman.ProcessResult, existingObject, objectToSave *bagman.IntellectualObject) (error) {
	result.FedoraResult.IsNewObject = false
	result.TarResult.MergeExistingFiles(existingObject.GenericFiles)
	if result.TarResult.AnyFilesNeedSaving() {

		err := bagRecorder.fedoraUpdateIntellectualObject(result, objectToSave)
		if err != nil {
			return err
		}

		// -------------------------------------------------------------
		// New save method - up to 200 at a time
		// -------------------------------------------------------------
		file_iterator := bagman.NewFileBatchIterator(result.TarResult.Files, 200)
		totalSaved := 0
		for {
			batch, err := file_iterator.NextBatch()
			if err == bagman.ErrStopIteration {
				bagRecorder.ProcUtil.MessageLog.Info("Finished sending generic files " +
					"from bag %s to Fluctus. %d of %d files needed saving.",
					result.S3File.Key.Key, totalSaved, len(result.TarResult.Files))
				break
			} else if err != nil {
				bagRecorder.ProcUtil.MessageLog.Error("While saving files from bag %s " +
					"to Fluctus, error getting batch: %v", result.S3File.Key.Key, err)
			}
			bagRecorder.ProcUtil.MessageLog.Info("Sending batch of %d generic files " +
				"from bag %s to Fluctus", len(batch), result.S3File.Key.Key)
			err = bagRecorder.ProcUtil.FluctusClient.GenericFileSaveBatch(objectToSave.Identifier, batch)
			if err != nil {
				bagRecorder.handleFedoraError(result, "Error saving generic file batch to Fedora", err)
			} else {
				totalSaved += len(batch)
			}
		}
		// -------------------------------------------------------------
		// End of new save
		// -------------------------------------------------------------
	} else {
		bagRecorder.ProcUtil.MessageLog.Debug(
			"Not saving object, files or events for %s: no change since last ingest",
			existingObject.Identifier)
	}
	return nil
}

func (bagRecorder *BagRecorder) fedoraRecordGenericFile(result *bagman.ProcessResult, objId string, gf *bagman.File) error {
	// Save the GenericFile metadata in Fedora, and add a metadata
	// record so we know whether the call to Fluctus succeeded or failed.
	genericFile, err := gf.ToGenericFile()
	if err != nil {
		return fmt.Errorf("Error converting GenericFile to Fluctus model: %v", err)
	}
	_, err = bagRecorder.ProcUtil.FluctusClient.GenericFileSave(objId, genericFile)
	if err != nil {
		bagRecorder.handleFedoraError(result,
			fmt.Sprintf("Error saving generic file '%s' to Fedora", objId),
			err)
		return err
	}
	bagRecorder.addMetadataRecord(result, "GenericFile", "file_registered", gf.Path, err)

	for _, event := range genericFile.Events {
		_, err = bagRecorder.ProcUtil.FluctusClient.PremisEventSave(genericFile.Identifier,
			"GenericFile", event)
		if err != nil {
			message := fmt.Sprintf("Error saving event '%s' for generic file "+
				"'%s' to Fedora", event, objId)
			bagRecorder.handleFedoraError(result, message, err)
			return err
		}
		bagRecorder.addMetadataRecord(result, "PremisEvent", event.EventType, gf.Path, err)
	}

	return nil
}

// Creates/Updates an IntellectualObject in Fedora, and sends the
// Ingest PremisEvent to Fedora.
func (bagRecorder *BagRecorder) fedoraUpdateIntellectualObject(result *bagman.ProcessResult, intellectualObject *bagman.IntellectualObject) error {
	// Create/Update the IntellectualObject
	savedObj, err := bagRecorder.ProcUtil.FluctusClient.IntellectualObjectUpdate(intellectualObject)
	if err != nil {
		message := fmt.Sprintf("Error saving intellectual object '%s' to Fedora",
			intellectualObject.Identifier)
		bagRecorder.handleFedoraError(result, message, err)
		return err
	}
	bagRecorder.addMetadataRecord(result, "IntellectualObject",
		"object_registered", intellectualObject.Identifier, err)
	if savedObj != nil {
		intellectualObject.Id = savedObj.Id
	}

	// Add PremisEvents for the ingest
	eventId := uuid.NewV4()
	ingestEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "ingest",
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            bagman.StatusSuccess,
		OutcomeDetail:      fmt.Sprintf("%d files copied", len(result.FedoraResult.GenericFilePaths)),
		Object:             "goamz S3 client",
		Agent:              "https://github.com/crowdmob/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}
	_, err = bagRecorder.ProcUtil.FluctusClient.PremisEventSave(intellectualObject.Identifier,
		"IntellectualObject", ingestEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving ingest event for intellectual "+
			"object '%s' to Fedora", intellectualObject.Identifier)
		bagRecorder.handleFedoraError(result, message, err)
		return err
	}
	bagRecorder.addMetadataRecord(result, "PremisEvent", "ingest", intellectualObject.Identifier, err)

	idEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "identifier_assignment",
		DateTime:           time.Now(),
		Detail:             "Assigned bag identifier",
		Outcome:            bagman.StatusSuccess,
		OutcomeDetail:      intellectualObject.Identifier,
		Object:             "APTrust bagman",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Institution domain + tar file name",
	}
	_, err = bagRecorder.ProcUtil.FluctusClient.PremisEventSave(intellectualObject.Identifier,
		"IntellectualObject", idEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving identifier_assignment event for "+
			"intellectual object '%s' to Fedora", intellectualObject.Identifier)
		bagRecorder.handleFedoraError(result, message, err)
		return err
	}
	bagRecorder.addMetadataRecord(result, "PremisEvent",
		"identifier_assignment", intellectualObject.Identifier, err)

	return nil
}

func (bagRecorder *BagRecorder) addMetadataRecord(result *bagman.ProcessResult, eventType, action, eventObject string, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	// Die on bad records. This is entirely within the developer's control
	// and should never happen.
	recError := result.FedoraResult.AddRecord(eventType, action, eventObject, errMsg)
	if recError != nil {
		bagRecorder.ProcUtil.MessageLog.Fatal(recError)
	}
}

func (bagRecorder *BagRecorder) handleFedoraError(result *bagman.ProcessResult, message string, err error) {
	result.FedoraResult.ErrorMessage = fmt.Sprintf("%s: %v", message, err)
	result.ErrorMessage = result.FedoraResult.ErrorMessage
}

// Delete the original tar file from the depositor's S3 receiving bucket.
func (bagRecorder *BagRecorder) DeleteS3File(result *bagman.ProcessResult) {
	result.Stage = bagman.StageCleanup
	if bagRecorder.ProcUtil.Config.DeleteOnSuccess == false {
		// Don't delete the original tar files, because config says
		// not to. (For integration tests, we don't delete our test
		// bags.)
		bagRecorder.ProcUtil.MessageLog.Info("Not deleting %s/%s because " +
			"config.DeleteOnSuccess == false", result.S3File.BucketName,
			result.S3File.Key.Key)
		return
	}
	err := bagRecorder.ProcUtil.S3Client.Delete(result.S3File.BucketName,
		result.S3File.Key.Key)
	if err != nil {
		// TODO: We want to report this error to the admin, but we don't
		// want to stop processing. We need some new mechanism for that.
		errMessage := fmt.Sprintf("Error deleting file '%s' from "+
			"bucket '%s': %v ", result.S3File.Key.Key, result.S3File.BucketName)
		bagRecorder.ProcUtil.MessageLog.Error(errMessage)
	} else {
		result.BagDeletedAt = time.Now().UTC()
		bagRecorder.ProcUtil.MessageLog.Info("Deleted original file '%s' from bucket '%s'",
			result.S3File.Key.Key, result.S3File.BucketName)
	}
}
