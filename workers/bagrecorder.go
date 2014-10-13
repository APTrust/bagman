// bagrecorder records bag metadata in Fluctus.
// That includes metadata for Intellectual Objects,
// Generic Files and Premis Events.
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
	"time"
)

type BagRecorder struct {
	FedoraChannel  chan *bagman.ProcessResult
	CleanUpChannel chan *bagman.ProcessResult
	ResultsChannel chan *bagman.ProcessResult
	StatusChannel  chan *bagman.ProcessResult
	ProcUtil       *bagman.ProcessUtil
}

func NewBagRecorder(procUtil *bagman.ProcessUtil) (*BagRecorder) {
	bagRecorder := &BagRecorder {
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.RecordWorkers * 10
	bagRecorder.FedoraChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	bagRecorder.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	bagRecorder.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	bagRecorder.StatusChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	for i := 0; i < procUtil.Config.RecordWorkers; i++ {
		go bagRecorder.recordInFedora()
		go bagRecorder.logResult()
		go bagRecorder.doCleanUp()
		go bagRecorder.recordStatus()
	}
	return bagRecorder
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
		result.NsqMessage.Touch()
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
			bagRecorder.ProcUtil.IncrementSucceeded()
			bagRecorder.ProcUtil.MessageLog.Info("%s -> finished OK", result.S3File.Key.Key)
		}

		// Add some stats to the message log
		bagRecorder.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
			bagRecorder.ProcUtil.Succeeded(), bagRecorder.ProcUtil.Failed())

		if result.NsqMessage.Attempts >= uint16(bagRecorder.ProcUtil.Config.MaxMetadataAttempts) &&
			result.ErrorMessage != "" {
			result.Retry = false
			result.ErrorMessage += fmt.Sprintf("Failure is due to a technical error "+
				"in Fedora. Giving up after %d failed attempts. This item has been "+
				"queued for administrative review. ",
				result.NsqMessage.Attempts)
			err = bagman.Enqueue(bagRecorder.ProcUtil.Config.NsqdHttpAddress,
				bagRecorder.ProcUtil.Config.TroubleTopic, result)
			if err != nil {
				bagRecorder.ProcUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
					result.S3File.Key.Key, err)
			} else {
				bagRecorder.ProcUtil.MessageLog.Warning("Sent '%s' to trouble queue",
					result.S3File.Key.Key)
			}
		}

		// Tell the fluctopus what happened
		bagRecorder.StatusChannel <- result
	}
}

func (bagRecorder *BagRecorder) recordStatus() {
	for result := range bagRecorder.StatusChannel {
		ingestStatus := result.IngestStatus()
		bagRecorder.updateFluctusStatus(result, ingestStatus.Stage, ingestStatus.Status)
		// Clean up the bag/tar files
		bagRecorder.CleanUpChannel <- result
	}
}

func (bagRecorder *BagRecorder) updateFluctusStatus(result *bagman.ProcessResult, stage bagman.StageType, status bagman.StatusType) {
	bagRecorder.ProcUtil.MessageLog.Debug("Setting Ingest status to %s/%s for %s",
		stage, status, result.S3File.Key.Key)
	ingestStatus := result.IngestStatus()
	ingestStatus.Stage = stage
	ingestStatus.Status = status
	if result.ErrorMessage == "" && stage == bagman.StageRecord && status == bagman.StatusPending {
		// This bag is done. No need to process it again.
		ingestStatus.Retry = false
	}
	err := bagRecorder.ProcUtil.FluctusClient.SendProcessedItem(ingestStatus)
	if err != nil {
		result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
			"item status returned error %v. ", err)
		bagRecorder.ProcUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s",
			err.Error())
	}
}

func (bagRecorder *BagRecorder) doCleanUp() {
	for result := range bagRecorder.CleanUpChannel {
		bagRecorder.ProcUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" && result.Retry == true {
			bagRecorder.ProcUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
			result.NsqMessage.Requeue(1 * time.Minute)
		} else {
			result.NsqMessage.Finish()
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
		bagRecorder.fedoraUpdateObject(result, existingObj, intellectualObject)
	} else if existingObj == nil && len(intellectualObject.GenericFiles) > bagman.MAX_FILES_FOR_CREATE {
		// Create the object with the first 500 files.
		// Call update for the rest.
		bagRecorder.ProcUtil.MessageLog.Debug("Creating new object %s with %d files (multi-step)",
			intellectualObject.Identifier, len(intellectualObject.GenericFiles))
		newObj, err := bagRecorder.fedoraCreateObject(result, intellectualObject, bagman.MAX_FILES_FOR_CREATE)
		if err != nil {
			return err
		}
		bagRecorder.fedoraUpdateObject(result, newObj, intellectualObject)
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
func (bagRecorder *BagRecorder) fedoraUpdateObject(result *bagman.ProcessResult, existingObject, objectToSave *bagman.IntellectualObject) {
	result.FedoraResult.IsNewObject = false
	result.TarResult.MergeExistingFiles(existingObject.GenericFiles)
	if result.TarResult.AnyFilesNeedSaving() {
		bagRecorder.fedoraUpdateIntellectualObject(result, objectToSave)
		for i := range result.TarResult.Files {
			genericFile := result.TarResult.Files[i]
			// Save generic file data to Fedora only if the file is new or changed.
			if genericFile.NeedsSave {
				bagRecorder.fedoraRecordGenericFile(result, objectToSave.Identifier, genericFile)
			} else {
				bagRecorder.ProcUtil.MessageLog.Debug(
					"Nothing to do for %s: no change since last ingest",
					genericFile.Identifier)
			}
		}
	} else {
		bagRecorder.ProcUtil.MessageLog.Debug(
			"Not saving object, files or events for %s: no change since last ingest",
			existingObject.Identifier)
	}
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
	eventId, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	ingestEvent := &bagman.PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "ingest",
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            bagman.StatusSuccess,
		OutcomeDetail:      fmt.Sprintf("%d files copied", len(result.FedoraResult.GenericFilePaths)),
		Object:             "goamz S3 client",
		Agent:              "https://launchpad.net/goamz",
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
