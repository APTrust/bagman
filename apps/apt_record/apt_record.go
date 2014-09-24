package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
	"time"
)

type Channels struct {
	FedoraChannel  chan *bagman.ProcessResult
	CleanUpChannel chan *bagman.ProcessResult
	ResultsChannel chan *bagman.ProcessResult
	StatusChannel  chan *bagman.ProcessResult
}

// Global vars.
var channels *Channels
var procUtil *processutil.ProcessUtil

func main() {
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)

	procUtil.MessageLog.Info("Metarecord started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxMetadataAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
	consumer, err := nsq.NewConsumer(procUtil.Config.MetadataTopic,
		procUtil.Config.MetadataChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &RecordProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

// Set up the channels.
func initChannels() {
	workerBufferSize := procUtil.Config.Workers * 10
	channels = &Channels{}
	channels.FedoraChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.StatusChannel = make(chan *bagman.ProcessResult, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.Workers; i++ {
		go recordInFedora()
		go logResult()
		go doCleanUp()
		go recordStatus()
	}
}

type RecordProcessor struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*RecordProcessor) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message
	channels.FedoraChannel <- &result
	procUtil.MessageLog.Debug("Put %s into Fluctus channel", result.S3File.Key.Key)
	return nil
}

func recordInFedora() {
	for result := range channels.FedoraChannel {
		procUtil.MessageLog.Info("Recording Fedora metadata for %s",
			result.S3File.Key.Key)
		result.NsqMessage.Touch()
		result.Stage = "Record"
		updateFluctusStatus(result, bagman.StageRecord, bagman.StatusStarted)
		// Save to Fedora only if there are new or updated items in this bag.
		// TODO: What if some items were deleted?
		if result.TarResult.AnyFilesNeedSaving() {
			err := recordAllFedoraData(result)
			if err != nil {
				result.ErrorMessage += fmt.Sprintf(" %s", err.Error())
			}
			if result.FedoraResult.AllRecordsSucceeded() == false {
				result.ErrorMessage += " When recording IntellectualObject, GenericFiles and " +
					"PremisEvents, one or more calls to Fluctus failed."
			}
			if result.ErrorMessage == "" {
				procUtil.MessageLog.Info("Successfully recorded Fedora metadata for %s",
					result.S3File.Key.Key)
			} else {
				// If any errors in occur while talking to Fluctus,
				// we'll want to requeue and try again. Just leave
				// the result.Retry flag alone, and that will happen.
				procUtil.MessageLog.Error(result.ErrorMessage)
			}
		} else {
			procUtil.MessageLog.Info("Nothing to update for %s: no items changed since last ingest.",
				result.S3File.Key.Key)
		}
		updateFluctusStatus(result, bagman.StageRecord, bagman.StatusPending)
		channels.ResultsChannel <- result
	}
}

func logResult() {
	for result := range channels.ResultsChannel {
		// Log full results to the JSON log
		json, err := json.Marshal(result)
		if err != nil {
			procUtil.MessageLog.Error(err.Error())
		}
		procUtil.JsonLog.Println(string(json))

		// Add a message to the message log
		if result.ErrorMessage != "" {
			procUtil.IncrementFailed()
			procUtil.MessageLog.Error("%s %s -> %s",
				result.S3File.BucketName,
				result.S3File.Key.Key,
				result.ErrorMessage)
		} else {
			procUtil.IncrementSucceeded()
			procUtil.MessageLog.Info("%s -> finished OK", result.S3File.Key.Key)
		}

		// Add some stats to the message log
		procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
			procUtil.Succeeded(), procUtil.Failed())

		if result.NsqMessage.Attempts >= uint16(procUtil.Config.MaxMetadataAttempts) && result.ErrorMessage != "" {
			result.Retry = false
			result.ErrorMessage += fmt.Sprintf("Failure is due to a technical error "+
				"in Fedora. Giving up after %d failed attempts. This item has been "+
				"queued for administrative review. ",
				result.NsqMessage.Attempts)
			err = bagman.Enqueue(procUtil.Config.NsqdHttpAddress, procUtil.Config.TroubleTopic, result)
			if err != nil {
				procUtil.MessageLog.Error("Could not send '%s' to trouble queue: %v",
					result.S3File.Key.Key, err)
			} else {
				procUtil.MessageLog.Warning("Sent '%s' to trouble queue", result.S3File.Key.Key)
			}
		}

		// Tell the fluctopus what happened
		channels.StatusChannel <- result
	}
}

func recordStatus() {
	for result := range channels.StatusChannel {
		ingestStatus := result.IngestStatus()
		updateFluctusStatus(result, ingestStatus.Stage, ingestStatus.Status)
		// Clean up the bag/tar files
		channels.CleanUpChannel <- result
	}
}

func updateFluctusStatus(result *bagman.ProcessResult, stage bagman.StageType, status bagman.StatusType) {
	procUtil.MessageLog.Debug("Setting Ingest status to %s/%s for %s", stage, status, result.S3File.Key.Key)
	ingestStatus := result.IngestStatus()
	ingestStatus.Stage = stage
	ingestStatus.Status = status
	err := procUtil.FluctusClient.SendProcessedItem(ingestStatus)
	if err != nil {
		result.ErrorMessage += fmt.Sprintf("Attempt to record processed "+
			"item status returned error %v. ", err)
		procUtil.MessageLog.Error("Error sending ProcessedItem to Fluctus: %s",
			err.Error())
	}
}

func doCleanUp() {
	for result := range channels.CleanUpChannel {
		procUtil.MessageLog.Debug("Cleaning up %s", result.S3File.Key.Key)
		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		if result.ErrorMessage != "" && result.Retry == true {
			procUtil.MessageLog.Info("Requeueing %s", result.S3File.Key.Key)
			result.NsqMessage.Requeue(1 * time.Minute)
		} else {
			result.NsqMessage.Finish()
		}
	}
}

// Send all metadata about the bag to Fluctus/Fedora. This includes
// the IntellectualObject, the GenericFiles, and all PremisEvents
// related to the object and the files.
func recordAllFedoraData(result *bagman.ProcessResult) (err error) {
	intellectualObject, err := result.IntellectualObject()
	if err != nil {
		return err
	}
	result.FedoraResult = bagman.NewFedoraResult(
		intellectualObject.Identifier,
		result.TarResult.GenericFilePaths())
	existingObj, err := procUtil.FluctusClient.IntellectualObjectGet(
		intellectualObject.Identifier, true)
	if err != nil {
		result.FedoraResult.ErrorMessage = fmt.Sprintf(
			"[ERROR] Error checking Fluctus for existing IntellectualObject '%s': %v",
			intellectualObject.Identifier, err)
		return err
	}
	if existingObj != nil {
		result.FedoraResult.IsNewObject = false
		result.TarResult.MergeExistingFiles(existingObj.GenericFiles)
		if result.TarResult.AnyFilesNeedSaving() {
			fedoraUpdateIntellectualObject(result, intellectualObject)
			for i := range result.TarResult.GenericFiles {
				genericFile := result.TarResult.GenericFiles[i]
				// Save generic file data to Fedora only if the file is new or changed.
				if genericFile.NeedsSave {
					fedoraRecordGenericFile(result, intellectualObject.Identifier, genericFile)
				} else {
					procUtil.MessageLog.Debug("Nothing to do for %s: no change since last ingest",
						genericFile.Identifier)
				}
			}
		} else {
			procUtil.MessageLog.Debug("Not saving object, files or events for %s: no change since last ingest",
				existingObj.Identifier)
		}
	} else {
		result.FedoraResult.IsNewObject = true
		newObj, err := procUtil.FluctusClient.IntellectualObjectCreate(intellectualObject)
		if err != nil {
			result.FedoraResult.ErrorMessage = fmt.Sprintf(
				"[ERROR] Error creating new IntellectualObject '%s' in Fluctus: %v",
				intellectualObject.Identifier, err)
			return err
		} else {
			intellectualObject.Id = newObj.Id
		}
	}
	return nil
}

func fedoraRecordGenericFile(result *bagman.ProcessResult, objId string, gf *bagman.GenericFile) error {
	// Save the GenericFile metadata in Fedora, and add a metadata
	// record so we know whether the call to Fluctus succeeded or failed.
	fluctusGenericFile, err := gf.ToFluctusModel()
	if err != nil {
		return fmt.Errorf("Error converting GenericFile to Fluctus model: %v", err)
	}
	_, err = procUtil.FluctusClient.GenericFileSave(objId, fluctusGenericFile)
	if err != nil {
		handleFedoraError(result,
			fmt.Sprintf("Error saving generic file '%s' to Fedora", objId),
			err)
		return err
	}
	addMetadataRecord(result, "GenericFile", "file_registered", gf.Path, err)

	for _, event := range fluctusGenericFile.Events {
		_, err = procUtil.FluctusClient.PremisEventSave(fluctusGenericFile.Identifier, "GenericFile", event)
		if err != nil {
			message := fmt.Sprintf("Error saving event '%s' for generic file "+
				"'%s' to Fedora", event, objId)
			handleFedoraError(result, message, err)
			return err
		}
		addMetadataRecord(result, "PremisEvent", event.EventType, gf.Path, err)
	}

	return nil
}

// Creates/Updates an IntellectualObject in Fedora, and sends the
// Ingest PremisEvent to Fedora.
func fedoraUpdateIntellectualObject(result *bagman.ProcessResult, intellectualObject *models.IntellectualObject) error {
	// Create/Update the IntellectualObject
	savedObj, err := procUtil.FluctusClient.IntellectualObjectUpdate(intellectualObject)
	if err != nil {
		message := fmt.Sprintf("Error saving intellectual object '%s' to Fedora",
			intellectualObject.Identifier)
		handleFedoraError(result, message, err)
		return err
	}
	addMetadataRecord(result, "IntellectualObject", "object_registered", intellectualObject.Identifier, err)
	if savedObj != nil {
		intellectualObject.Id = savedObj.Id
	}

	// Add PremisEvents for the ingest
	eventId, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	ingestEvent := &models.PremisEvent{
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
	_, err = procUtil.FluctusClient.PremisEventSave(intellectualObject.Identifier,
		"IntellectualObject", ingestEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving ingest event for intellectual "+
			"object '%s' to Fedora", intellectualObject.Identifier)
		handleFedoraError(result, message, err)
		return err
	}
	addMetadataRecord(result, "PremisEvent", "ingest", intellectualObject.Identifier, err)

	idEvent := &models.PremisEvent{
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
	_, err = procUtil.FluctusClient.PremisEventSave(intellectualObject.Identifier,
		"IntellectualObject", idEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving identifier_assignment event for "+
			"intellectual object '%s' to Fedora", intellectualObject.Identifier)
		handleFedoraError(result, message, err)
		return err
	}
	addMetadataRecord(result, "PremisEvent", "identifier_assignment", intellectualObject.Identifier, err)

	return nil
}

func addMetadataRecord(result *bagman.ProcessResult, eventType, action, eventObject string, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	// Die on bad records. This is entirely within the developer's control
	// and should never happen.
	recError := result.FedoraResult.AddRecord(eventType, action, eventObject, errMsg)
	if recError != nil {
		procUtil.MessageLog.Fatal(recError)
	}
}

func handleFedoraError(result *bagman.ProcessResult, message string, err error) {
	result.FedoraResult.ErrorMessage = fmt.Sprintf("%s: %v", message, err)
	result.ErrorMessage = result.FedoraResult.ErrorMessage
}
