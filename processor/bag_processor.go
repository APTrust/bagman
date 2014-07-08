package main

import (
    "fmt"
    "flag"
    "encoding/json"
    "encoding/base64"
    "encoding/hex"
    "os"
    "regexp"
    "sync/atomic"
    "log"
	"time"
    "path/filepath"
    "github.com/APTrust/bagman"
    "github.com/APTrust/bagman/fluctus/client"
    "github.com/APTrust/bagman/fluctus/models"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
    "github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
)

type Channels struct {
    FetchChannel     chan *bagman.ProcessResult
    UnpackChannel    chan *bagman.ProcessResult
	StorageChannel   chan *bagman.ProcessResult
	FedoraChannel    chan *bagman.ProcessResult
    CleanUpChannel   chan *bagman.ProcessResult
    ResultsChannel   chan *bagman.ProcessResult
}


// Global vars.
var channels *Channels
var config bagman.Config
var jsonLog *log.Logger
var messageLog *log.Logger
var volume *bagman.Volume
var s3Client *bagman.S3Client
var succeeded = int64(0)
var failed = int64(0)
var bytesInS3 = int64(0)
var bytesProcessed = int64(0)
var fluctusClient *client.Client

// bag_processor receives messages from nsqd describing
// items in the S3 receiving buckets. Each item/message
// follows this flow:
//
// 1. Fetch channel: fetches the file from S3.
// 2. Unpack channel: untars the bag files, parses and validates
//    the bag, reads tags, generates checksums and generic file
//    UUIDs.
// 3. Storage channel: copies files to S3 permanent storage.
// 4. Fedora channel: saves intellectual objects, generic files
//    and Premis event metadata to Fedora.
// 5. Results channel: tells the queue whether processing
//    succeeded, and if not, whether the item should be requeued.
//    Also logs results to json and message logs.
// 6. Cleanup channel: cleans up the files after processing
//    completes.
//
// If a failure occurs anywhere in the first three steps,
// processing goes directly to the Results Channel, which
// records the error and the disposition (retry/give up).
//
// As long as the message from nsq contains valid JSON,
// steps 5 and 6 ALWAYS run.
//
// The bag processor has so many responsibilities because
// downloading, untarring and running checksums on
// multi-gigabyte files takes a lot of time. We want to
// avoid having separate processes repeatedly download and
// untar the files, so bag_processor performs all operations
// that require local access to the raw contents of the bags.
func main() {

    loadConfig()
	err := ensureFluctusConfig()
    if err != nil {
        messageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
    }

    initVolume()
    initChannels()
    initGoRoutines(channels)

	err = initS3Client()
    if err != nil {
        messageLog.Fatalf("Cannot initialize S3Client: %v", err)
    }

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 10)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(3))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "30m")
    consumer, err := nsq.NewConsumer(config.BagProcessorTopic, config.BagProcessorChannel, nsqConfig)
    if err != nil {
        messageLog.Fatalf(err.Error())
    }

    handler := &BagProcessor{}
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
    jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "bag_processor")
}

func ensureFluctusConfig() (error) {
    if config.FluctusURL == "" {
        return fmt.Errorf("FluctusUrl is not set in config file")
    }
    if os.Getenv("FLUCTUS_API_USER") == "" {
        return fmt.Errorf("Environment variable FLUCTUS_API_USER is not set")
    }
    if os.Getenv("FLUCTUS_API_KEY") == "" {
        return fmt.Errorf("Environment variable FLUCTUS_API_KEY is not set")
    }
	return nil
}

// Set up the volume to keep track of how much disk space is
// available. We want to avoid downloading large files when
// we know ahead of time that the volume containing the tar
// directory doesn't have enough space to accomodate them.
func initVolume() {
    var err error
    volume, err = bagman.NewVolume(config.TarDirectory, messageLog)
    if err != nil {
        panic(err.Error())
    }
}

// Initialize the reusable S3 client.
func initS3Client() (err error) {
    s3Client, err = bagman.NewS3Client(aws.USEast)
	return err
}

// Set up the channels. It's essential that that the fetchChannel
// be limited to a relatively low number. If we are downloading
// 1GB tar files, we need space to store the tar file AND the
// untarred version. That's about 2 x 1GB. We do not want to pull
// down 1000 files at once, or we'll run out of disk space!
// If config sets fetchers to 10, we can pull down 10 files at a
// time. The fetch queue could hold 10 * 4 = 40 items, so we'd
// have max 40 tar files + untarred directories on disk at once.
// The number of workers should be close to the number of CPU
// cores.
func initChannels() {
    fetcherBufferSize := config.Fetchers * 4
    workerBufferSize := config.Workers * 10

    channels = &Channels{}
    channels.FetchChannel = make(chan *bagman.ProcessResult, fetcherBufferSize)
    channels.UnpackChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.StorageChannel = make(chan *bagman.ProcessResult, workerBufferSize)
	channels.FedoraChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
}

// Set up our go routines. We do NOT want one go routine per
// S3 file. If we do that, the system will run out of file handles,
// as we'll have tens of thousands of open connections to S3
// trying to write data into tens of thousands of local files.
func initGoRoutines(channels *Channels) {
    for i := 0; i < config.Fetchers; i++ {
        go doFetch()
    }

    for i := 0; i < config.Workers; i++ {
        go doUnpack()
		go saveToStorage()
		go recordInFedora()
        go logResult()
        go doCleanUp()
    }
}

type BagProcessor struct {

}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*BagProcessor) HandleMessage(message *nsq.Message) (error) {
    message.Attempts++
    var s3File bagman.S3File
    err := json.Unmarshal(message.Body, &s3File)
    if err != nil {
        messageLog.Println("[ERROR] Could not unmarshal JSON data from nsq:",
            string(message.Body))
        message.Finish()
        return nil
    }

    // Create the result struct and pass it down the pipeline
    result := &bagman.ProcessResult{
        message,         // NsqMessage
        &s3File,         // S3File: tarred bag that was uploaded to receiving bucket
        "",              // ErrorMessage: no processing error yet
        nil,             // FetchResult: could we get the bag?
        nil,             // TarResult: could we untar and validate the bag?
        nil,             // BagReadResult
		nil,             // FedoraResult
        "",              // Current stage of processing
        true}            // Retry if processing fails? Default to yes.
    channels.FetchChannel <- result
    messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
    return nil
}


// -- Step 1 of 6 --
// This runs as a go routine to fetch files from S3.
func doFetch() {
    for result := range channels.FetchChannel {
        result.Stage = "Fetch"
        s3Key := result.S3File.Key
		result.FetchResult = &bagman.FetchResult{}
        // Disk needs filesize * 2 disk space to accomodate tar file & untarred files
        err := volume.Reserve(uint64(s3Key.Size * 2))
        if err != nil {
            // Not enough room on disk
            messageLog.Println("[WARNING]", "Requeueing",
                s3Key.Key, "- not enough disk space")
            result.ErrorMessage = err.Error()
            result.Retry = true
            channels.ResultsChannel <- result
        } else {
            messageLog.Println("[INFO]", "Fetching", s3Key.Key)
            fetchResult := Fetch(result.S3File.BucketName, s3Key)
            result.FetchResult = fetchResult
            result.Retry = fetchResult.Retry
            if fetchResult.ErrorMessage != "" {
                // Fetch from S3 failed. Requeue.
                result.ErrorMessage = fetchResult.ErrorMessage
                channels.ResultsChannel <- result
            } else {
                // Got S3 file. Untar it.
                // And touch the message, so nsqd knows we're making progress.
                result.NsqMessage.Touch()
                channels.UnpackChannel <- result
            }
        }
    }
}

// -- Step 2 of 6 --
// This runs as a go routine to untar files downloaded from S3.
// We calculate checksums and create generic files during the unpack
// stage to avoid having to reprocess large streams of data several times.
func doUnpack() {
    for result := range channels.UnpackChannel {
        if result.ErrorMessage != "" {
            // Unpack failed. Go to end.
            messageLog.Println("[INFO]", "Nothing to unpack for",
                result.S3File.Key.Key)
            channels.ResultsChannel <- result
        } else {
            // Unpacked! Now process the bag and touch message
            // so nsqd knows we're making progress.
            messageLog.Println("[INFO]", "Unpacking", result.S3File.Key.Key)
            result.NsqMessage.Touch()
            ProcessBagFile(result)
			if result.ErrorMessage == "" {
				// Move to permanent storage if bag processing succeeded
				channels.StorageChannel <- result
			} else {
				channels.ResultsChannel <- result
			}
        }
    }
}

// -- Step 3 of 6 --
// This runs as a go routine to save generic files to the permanent
// S3 storage bucket. Unfortunately, there is no concept of transaction
// here. Ideally, either all GenericFiles make to S3 or none do. In
// cases where we're updating an existing bag (i.e. user uploaded a new
// version of it), we may wind up in a state where half of the new files
// make it successfully to S3 and half do not. That would leave us with
// an inconsistent bag, containing half new files and half old files.
// In addition, the failure to copy all files to S3 would result in no
// metadata going to Fedora. So Fedora would not show that any of the
// generic files were overwritten, even though some were. We should alert
// an admin in these cases. The JSON log will have full information about
// the state of all of the files.
func saveToStorage() {
    for result := range channels.StorageChannel {
		errorOccurred := false
		messageLog.Println("[INFO] Storing",result.S3File.Key.Key)
		result.NsqMessage.Touch()
		result.Stage = "Store"
		re := regexp.MustCompile("\\.tar$")
		// Copy each generic file to S3
		for i := range(result.TarResult.GenericFiles) {
			gf := result.TarResult.GenericFiles[i]
			bagDir := re.ReplaceAllString(result.S3File.Key.Key, "")
			file := filepath.Join(
				config.TarDirectory,
				bagDir,
				gf.Path)
			absPath, err := filepath.Abs(file)
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Cannot get absolute " +
					"path to file '%s'. " +
					"File cannot be copied to long-term storage: %v",
					file, err)
				errorOccurred = true
				continue
			}
			reader, err := os.Open(absPath)
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error opening file '%s'" +
					". File cannot be copied to long-term storage: %v",
					absPath, err)
				errorOccurred = true
				continue
			}
			messageLog.Printf("[INFO] Sending %d bytes to S3 for file %s (UUID %s)",
				gf.Size, gf.Path, gf.Uuid)

			// Prepare metadata for save to S3
			bagName := result.S3File.Key.Key[0:len(result.S3File.Key.Key)-4]
			instDomain := bagman.OwnerOf(result.S3File.BucketName)
			s3Metadata := make(map[string][]string)
			s3Metadata["md5"] = []string{ gf.Md5  }
			s3Metadata["institution"] = []string{ instDomain }
			s3Metadata["bag"] = []string{ bagName }
			s3Metadata["bagpath"] = []string{ gf.Path }

			// We'll get error if md5 contains non-hex characters. Catch
			// that below, when S3 tells us our md5 sum is invalid.
			md5Bytes, err := hex.DecodeString(gf.Md5)
			if err != nil {
				msg := fmt.Sprintf("Md5 sum '%s' contains invalid characters. " +
					"S3 will reject this!", gf.Md5)
				result.ErrorMessage += msg
				messageLog.Println("[ERROR]", msg)
				errorOccurred = true
			}

			// Save to S3 with the base64-encoded md5 sum
			base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)
			options := s3Client.MakeOptions(base64md5, s3Metadata)
			url, err := s3Client.SaveToS3(
				config.PreservationBucket,
				gf.Uuid,
				gf.MimeType,
				reader,
				gf.Size,
				options)
			reader.Close()
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error copying file '%s'" +
					"to long-term storage: %v ", absPath, err)
				messageLog.Println("[ERROR]", "Failed to send",
					result.S3File.Key.Key,
					"to long-term storage:",
					err.Error())
				errorOccurred = true
			} else {
				gf.StorageURL = url
				gf.StoredAt = time.Now()
				messageLog.Printf("[INFO] Successfully sent %s (UUID %s)" +
					"to long-term storage bucket.", gf.Path, gf.Uuid)
			}
		}
		if errorOccurred {
			// Don't record Fedora metadata if error occurred.
			// Just make a note in Fluctus' processed items list
			// and then clean up.
			channels.ResultsChannel <- result
		} else {
			// Record metadata in Fluctus/Fedora
			channels.FedoraChannel <- result
		}
    }
}

// -- Step 4 of 6 --
// We have to make several calls to Fluctus/Fedora here, and this will
// likely be the bottleneck in the process. The calls are sequential
// because later calls depend on the object created in earlier calls.
// There's no way around this until we implement a single Fluctus endpoint
// that will take in all the metadata at once and processing everyhing.
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
				"PremisEvents, one or more calls to Fluctus failed. See the JSON log for details."
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

// -- Step 5 of 6 --
// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func logResult() {
    for result := range channels.ResultsChannel {
        // Log full results to the JSON log
        json, err := json.Marshal(result)
        if err != nil {
            messageLog.Println("[ERROR]", err)
        }
        jsonLog.Println(string(json))

        // Add a message to the message log
        atomic.AddInt64(&bytesInS3, int64(result.S3File.Key.Size))
        if(result.ErrorMessage != "") {
            atomic.AddInt64(&failed, 1)
            messageLog.Println("[ERROR]",
                result.S3File.BucketName,
                result.S3File.Key.Key,
                "->", result.ErrorMessage)
        } else {
            atomic.AddInt64(&succeeded, 1)
            atomic.AddInt64(&bytesProcessed, int64(result.S3File.Key.Size))
            messageLog.Println("[INFO]", result.S3File.Key.Key, "-> finished OK")
        }

        // Add some stats to the message log
        messageLog.Printf("[STATS] Succeeded: %d, Failed: %d, Bytes Processed: %d\n",
            succeeded, failed, bytesProcessed)

        // Tell Fluctus what happened
        go func() {
            err := SendProcessedItemToFluctus(result)
            if err != nil {
                messageLog.Println("[ERROR] Error sending ProcessedItem to Fluctus:",
                    err)
            }
        }()

        // Clean up the bag/tar files
        channels.CleanUpChannel <- result
    }
}

// -- Step 6 of 6 --
// This runs as a go routine to remove the files we downloaded
// and untarred.
// THIS STEP ALWAYS RUNS, EVEN IF PRIOR STEPS FAILED.
func doCleanUp() {
    for result := range channels.CleanUpChannel {
        messageLog.Println("[INFO]", "Cleaning up", result.S3File.Key.Key)
        if result.S3File.Key.Key != "" && result.FetchResult.LocalTarFile != "" {
            // Clean up any files we downloaded and unpacked
            errors := CleanUp(result.FetchResult.LocalTarFile)
            if errors != nil && len(errors) > 0 {
                messageLog.Println("[WARNING]", "Errors cleaning up",
                    result.FetchResult.LocalTarFile)
                for _, e := range errors {
                    messageLog.Println("[ERROR]", e)
                }
            }
			// Let our volume tracker know we just freed up some disk space.
			// Free the same amount we reserved.
			volume.Release(uint64(result.S3File.Key.Size * 2))
        }

        // Build and send message back to NSQ, indicating whether
        // processing succeeded.
        if result.ErrorMessage != "" && result.Retry == true {
			messageLog.Printf("[INFO] Requeueing %s", result.S3File.Key.Key)
            result.NsqMessage.Requeue(30000)
        } else {
            result.NsqMessage.Finish()
        }
    }
}



// This fetches a file from S3 and stores it locally.
func Fetch(bucketName string, key s3.Key) (result *bagman.FetchResult) {
    tarFilePath := filepath.Join(config.TarDirectory, key.Key)
    return s3Client.FetchToFile(bucketName, key, tarFilePath)
}

// This deletes the tar file and all of the files that were
// unpacked from it. Param file is the path the tar file.
func CleanUp(file string) (errors []error) {
    errors = make([]error, 0)
    err := os.Remove(file)
    if err != nil {
        errors = append(errors, err)
    }
    // TODO: Don't assume the untarred dir name is the same as
    // the tar file. We have its actual name in the TarResult.
    re := regexp.MustCompile("\\.tar$")
    untarredDir := re.ReplaceAllString(file, "")
    err = os.RemoveAll(untarredDir)
    if err != nil {
        messageLog.Printf("Error deleting dir %s: %s\n", untarredDir, err.Error())
        errors = append(errors, err)
    }
    return errors
}


// Runs tests on the bag file at path and returns information about
// whether it was successfully unpacked, valid and complete.
func ProcessBagFile(result *bagman.ProcessResult) {
    result.Stage = "Unpack"
	instDomain := bagman.OwnerOf(result.S3File.BucketName)
	bagName := result.S3File.Key.Key[0:len(result.S3File.Key.Key)-4]
    result.TarResult = bagman.Untar(result.FetchResult.LocalTarFile,
		instDomain, bagName)
    if result.TarResult.ErrorMessage != "" {
        result.ErrorMessage = result.TarResult.ErrorMessage
        // If we can't untar this, there's no reason to retry...
        // but we'll have to revisit this. There may be cases
        // where we do want to retry, such as if disk was full.
        result.Retry = false
    } else {
        result.Stage = "Validate"
        result.BagReadResult = bagman.ReadBag(result.TarResult.OutputDir)
        if result.BagReadResult.ErrorMessage != "" {
            result.ErrorMessage = result.BagReadResult.ErrorMessage
            // Something was wrong with this bag. Bad checksum,
            // missing file, etc. Don't reprocess it.
            result.Retry = false
        } else {
			for i := range(result.TarResult.GenericFiles) {
				gf := result.TarResult.GenericFiles[i]
				gf.Md5Verified = time.Now()
			}
		}
    }
}

// Returns a reusable HTTP client for communicating with Fluctus.
func getFluctusClient() (fClient *client.Client, err error) {
    if fluctusClient == nil {
        fClient, err := client.New(
            config.FluctusURL,
            os.Getenv("FLUCTUS_API_USER"),
            os.Getenv("FLUCTUS_API_KEY"),
            messageLog)
        if err != nil {
            return nil, err
        }
        fluctusClient = fClient
    }
    return fluctusClient, nil
}

// SendProcessedItemToFluctus sends information about the status of
// processing this item to Fluctus.
func SendProcessedItemToFluctus(result *bagman.ProcessResult) (err error) {
    client, err := getFluctusClient()
    if err != nil {
        return err
    }
    localStatus := result.IngestStatus()
    remoteStatus, err := client.GetBagStatus(
        localStatus.ETag, localStatus.Name, localStatus.BagDate)
    if err != nil {
        return err
    }
    if remoteStatus != nil {
        localStatus.Id = remoteStatus.Id
    }
    err = client.UpdateBagStatus(localStatus)
    if err != nil {
        return err
    }
    messageLog.Printf("[INFO] Updated status in Fluctus for %s: %s/%s\n",
        result.S3File.Key.Key, localStatus.Status, localStatus.Stage)
    return nil
}

// Send all metadata about the bag to Fluctus/Fedora. This includes
// the IntellectualObject, the GenericFiles, and all PremisEvents
// related to the object and the files.
func recordAllFedoraData(result *bagman.ProcessResult) (err error) {
    client, err := getFluctusClient()
    if err != nil {
        return err
    }
	intellectualObject, err := result.IntellectualObject()
    if err != nil {
        return err
    }
	result.FedoraResult = bagman.NewFedoraResult(
		intellectualObject.Identifier,
		result.TarResult.GenericFilePaths())

	fedoraRecordIntellectualObject(result, client, intellectualObject)
	for i := range(result.TarResult.GenericFiles) {
		genericFile := result.TarResult.GenericFiles[i]
		fedoraRecordGenericFile(result, client, intellectualObject.Identifier, genericFile)
	}
	return nil
}

func fedoraRecordGenericFile(result *bagman.ProcessResult, client *client.Client, objId string, gf *bagman.GenericFile) (error) {
	// Save the GenericFile metadata in Fedora, and add a metadata
	// record so we know whether the call to Fluctus succeeded or failed.
	fluctusGenericFile, err := gf.ToFluctusModel()
	if err != nil {
		return fmt.Errorf("Error converting GenericFile to Fluctus model: %v", err)
	}
	_, err = client.GenericFileSave(objId, fluctusGenericFile)
	if err != nil {
		handleFedoraError(result,
			fmt.Sprintf("Error saving generic file '%s' to Fedora", objId),
			err)
		return err
	}
	addMetadataRecord(result, "GenericFile", "file_registered", gf.Path, err)

	for _, event := range(fluctusGenericFile.Events) {
		_, err = client.PremisEventSave(fluctusGenericFile.Identifier, "GenericFile", event)
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
func fedoraRecordIntellectualObject(result *bagman.ProcessResult, client *client.Client, intellectualObject *models.IntellectualObject) (error) {
	// Create/Update the IntellectualObject
	savedObj, err := client.IntellectualObjectSave(intellectualObject)
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
	_, err = client.PremisEventSave(intellectualObject.Identifier, "IntellectualObject", ingestEvent)
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
	_, err = client.PremisEventSave(intellectualObject.Identifier, "IntellectualObject", idEvent)
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
