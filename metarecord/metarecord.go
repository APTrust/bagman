package main

import (
    "fmt"
    "flag"
    "encoding/json"
    "os"
    "sync/atomic"
    "log"
	"time"
    "github.com/APTrust/bagman"
    "github.com/APTrust/bagman/fluctus/client"
    "github.com/APTrust/bagman/fluctus/models"
    "github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
	"github.com/crowdmob/goamz/aws"
)

type Channels struct {
	FedoraChannel    chan *bagman.ProcessResult
    CleanUpChannel   chan *bagman.ProcessResult
    ResultsChannel   chan *bagman.ProcessResult
}


// Global vars.
var channels *Channels
var config bagman.Config
var jsonLog *log.Logger
var messageLog *log.Logger
var succeeded = int64(0)
var failed = int64(0)
var s3Client *bagman.S3Client
var fluctusClient *client.Client

func main() {

    loadConfig()
	err := config.EnsureFluctusConfig()
    if err != nil {
        messageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
    }

	fluctusClient, err = client.New(
		config.FluctusURL,
		config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	if err != nil {
		messageLog.Fatalf("Cannot initialize Fluctus Client: %v", err)
	}

    initChannels()
    initGoRoutines()

	err = initS3Client()
    if err != nil {
        messageLog.Fatalf("Cannot initialize S3Client: %v", err)
    }

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxMetadataAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
    consumer, err := nsq.NewConsumer(config.MetadataTopic,
		config.MetadataChannel, nsqConfig)
    if err != nil {
        messageLog.Fatalf(err.Error())
    }

    handler := &RecordProcessor{}
    consumer.SetHandler(handler)
    consumer.ConnectToNSQLookupd(config.NsqLookupd)

    // This reader blocks until we get an interrupt, so our program does not exit.
    <-consumer.StopChan
}

func loadConfig() {
    // Load the config or die.
    requestedConfig := flag.String("config", "", "configuration to run")
    flag.Parse()
    config = bagman.LoadRequestedConfig(requestedConfig)
    jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "metarecord")
}


// Set up the channels.
func initChannels() {
    workerBufferSize := config.Workers * 10
    channels = &Channels{}
	channels.FedoraChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
    for i := 0; i < config.Workers; i++ {
 		go recordInFedora()
        go logResult()
        go doCleanUp()
    }
}

// Initialize the reusable S3 client.
func initS3Client() (err error) {
    s3Client, err = bagman.NewS3Client(aws.USEast)
	return err
}

type RecordProcessor struct {

}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*RecordProcessor) HandleMessage(message *nsq.Message) (error) {
	message.DisableAutoResponse()
    var result bagman.ProcessResult
    err := json.Unmarshal(message.Body, &result)
    if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
            err, string(message.Body))
        messageLog.Println("[ERROR]", detailedError)
        message.Finish()
        return detailedError
    }
	result.NsqMessage = message
    channels.FedoraChannel <- &result
    messageLog.Println("[INFO]", "Put", result.S3File.Key.Key, "into Fluctus channel")
    return nil
}


func recordInFedora() {
    for result := range channels.FedoraChannel {
		messageLog.Println("[INFO] Recording Fedora metadata for",
			result.S3File.Key.Key)
		result.NsqMessage.Touch()
		result.Stage = "Record"
		err := recordAllFedoraData(result)
		if err != nil {
			result.ErrorMessage += fmt.Sprintf(" %s",err.Error())
		}
		if result.FedoraResult.AllRecordsSucceeded() == false {
			result.ErrorMessage += " When recording IntellectualObject, GenericFiles and " +
				"PremisEvents, one or more calls to Fluctus failed."
		}
		if result.ErrorMessage == "" {
			messageLog.Println("[INFO] Successfully recorded Fedora metadata for",
				result.S3File.Key.Key)
		} else {
			// If any errors in occur while talking to Fluctus,
			// we'll want to requeue and try again. Just leave
			// the result.Retry flag alone, and that will happen.
			messageLog.Println("[ERROR]", result.ErrorMessage)
		}
        channels.ResultsChannel <- result
	}
}

// TODO: This code is duplicated in bag_processor.go
func logResult() {
    for result := range channels.ResultsChannel {
        // Log full results to the JSON log
        json, err := json.Marshal(result)
        if err != nil {
            messageLog.Println("[ERROR]", err)
        }
        jsonLog.Println(string(json))

        // Add a message to the message log
        if(result.ErrorMessage != "") {
            atomic.AddInt64(&failed, 1)
            messageLog.Println("[ERROR]",
                result.S3File.BucketName,
                result.S3File.Key.Key,
                "->", result.ErrorMessage)
        } else {
            atomic.AddInt64(&succeeded, 1)
            messageLog.Println("[INFO]", result.S3File.Key.Key, "-> finished OK")
        }

        // Add some stats to the message log
        messageLog.Printf("[STATS] Succeeded: %d, Failed: %d\n", succeeded, failed)

		if result.NsqMessage.Attempts >= uint16(config.MaxMetadataAttempts) && result.ErrorMessage != "" {
			result.Retry = false
			result.ErrorMessage += fmt.Sprintf("Failure is due to a technical error " +
				"in Fedora. Giving up after %d failed attempts. This item has been " +
				"queued for administrative review. ",
				result.NsqMessage.Attempts)
			err = bagman.Enqueue(config.NsqdHttpAddress, config.TroubleTopic, result)
			if err != nil {
				messageLog.Printf("[ERROR] Could not send '%s' to trouble queue: %v\n",
					result.S3File.Key.Key, err)
			} else {
				messageLog.Printf("[WARN] Sent '%s' to trouble queue\n", result.S3File.Key.Key)
			}
		}

		// Tell Fluctus what happened
		go func() {
			err := fluctusClient.SendProcessedItem(result.IngestStatus())
			if err != nil {
				result.ErrorMessage += fmt.Sprintf("Attempt to record processed " +
					"item status returned error %v. ", err)
				messageLog.Println("[ERROR] Error sending ProcessedItem to Fluctus:",
					err)
			}
		}()

        // Clean up the bag/tar files
        channels.CleanUpChannel <- result
    }
}

func doCleanUp() {
    for result := range channels.CleanUpChannel {
        messageLog.Println("[INFO]", "Cleaning up", result.S3File.Key.Key)
        // Build and send message back to NSQ, indicating whether
        // processing succeeded.
        if result.ErrorMessage != "" && result.Retry == true {
			messageLog.Printf("[INFO] Requeueing %s", result.S3File.Key.Key)
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
	existingObj, err := fluctusClient.IntellectualObjectGet(
		intellectualObject.Identifier, false)
	if err != nil {
		result.FedoraResult.ErrorMessage = fmt.Sprintf(
			"[ERROR] Error checking Fluctus for existing IntellectualObject '%s': %v",
			intellectualObject.Identifier, err)
		return err
	}
	if existingObj != nil {
		result.FedoraResult.IsNewObject = false
		fedoraUpdateIntellectualObject(result, intellectualObject)
		for i := range(result.TarResult.GenericFiles) {
			genericFile := result.TarResult.GenericFiles[i]
			// -------------------------------------------------------------------------
			// TEMP - For debugging a specific error
			// -------------------------------------------------------------------------
			if genericFile.MimeType == "" {
				messageLog.Printf("[WARN] Generic file %s of object %s has no mime type",
					genericFile.Path, intellectualObject.Identifier)
			}
			// -------------------------------------------------------------------------
			// END OF TEMP CODE
			// -------------------------------------------------------------------------

			fedoraRecordGenericFile(result, intellectualObject.Identifier, genericFile)
		}
	} else {
		result.FedoraResult.IsNewObject = true

		// -------------------------------------------------------------------------
		// TEMP - For debugging a specific error
		// -------------------------------------------------------------------------
		for i := range(result.TarResult.GenericFiles) {
			genericFile := result.TarResult.GenericFiles[i]
			if genericFile.MimeType == "" {
				messageLog.Printf("[WARN] Generic file %s of object %s has no mime type",
					genericFile.Path, intellectualObject.Identifier)
			}
		}
		// -------------------------------------------------------------------------
		// END OF TEMP CODE
		// -------------------------------------------------------------------------

		newObj, err := fluctusClient.IntellectualObjectCreate(intellectualObject)
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

func fedoraRecordGenericFile(result *bagman.ProcessResult, objId string, gf *bagman.GenericFile) (error) {
	// Save the GenericFile metadata in Fedora, and add a metadata
	// record so we know whether the call to Fluctus succeeded or failed.
	fluctusGenericFile, err := gf.ToFluctusModel()
	if err != nil {
		return fmt.Errorf("Error converting GenericFile to Fluctus model: %v", err)
	}
	_, err = fluctusClient.GenericFileSave(objId, fluctusGenericFile)
	if err != nil {
		handleFedoraError(result,
			fmt.Sprintf("Error saving generic file '%s' to Fedora", objId),
			err)
		return err
	}
	addMetadataRecord(result, "GenericFile", "file_registered", gf.Path, err)

	for _, event := range(fluctusGenericFile.Events) {
		_, err = fluctusClient.PremisEventSave(fluctusGenericFile.Identifier, "GenericFile", event)
		if err != nil {
			message := fmt.Sprintf("Error saving event '%s' for generic file " +
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
func fedoraUpdateIntellectualObject(result *bagman.ProcessResult, intellectualObject *models.IntellectualObject) (error) {
	// Create/Update the IntellectualObject
	savedObj, err := fluctusClient.IntellectualObjectUpdate(intellectualObject)
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
		Identifier: eventId.String(),
		EventType: "ingest",
		DateTime: time.Now(),
		Detail: "Copied all files to perservation bucket",
		Outcome: "Success",
		OutcomeDetail: fmt.Sprintf("%d files copied", len(result.FedoraResult.GenericFilePaths)),
		Object: "goamz S3 client",
		Agent: "https://launchpad.net/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}
	_, err = fluctusClient.PremisEventSave(intellectualObject.Identifier, "IntellectualObject", ingestEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving ingest event for intellectual " +
			"object '%s' to Fedora", intellectualObject.Identifier)
		handleFedoraError(result, message, err)
		return err
	}
	addMetadataRecord(result, "PremisEvent", "ingest", intellectualObject.Identifier, err)

	idEvent := &models.PremisEvent{
		Identifier: eventId.String(),
		EventType: "identifier_assignment",
		DateTime: time.Now(),
		Detail: "Assigned bag identifier",
		Outcome: "Success",
		OutcomeDetail: intellectualObject.Identifier,
		Object: "APTrust bagman",
		Agent: "https://github.com/APTrust/bagman",
		OutcomeInformation: "Institution domain + tar file name",
	}
	_, err = fluctusClient.PremisEventSave(intellectualObject.Identifier, "IntellectualObject", idEvent)
	if err != nil {
		message := fmt.Sprintf("Error saving identifier_assignment event for " +
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
		messageLog.Fatal(recError)
	}
}

func handleFedoraError(result *bagman.ProcessResult, message string, err error) {
	result.FedoraResult.ErrorMessage = fmt.Sprintf("%s: %v", message, err)
	result.ErrorMessage = result.FedoraResult.ErrorMessage
}
