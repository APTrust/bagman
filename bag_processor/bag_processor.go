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
	"strings"
    "path/filepath"
    "github.com/APTrust/bagman"
    "github.com/APTrust/bagman/fluctus/client"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
    "github.com/bitly/go-nsq"
)

// Constants
const GIGABYTE int64 = int64(1024 * 1024 * 1024)
// Files over 5GB in size must be uploaded via multi-part put.
const S3_LARGE_FILE int64 = int64(5 * GIGABYTE)
// Chunk size for multipart puts to S3.
const S3_CHUNK_SIZE = int64(209715200)


type Channels struct {
    FetchChannel     chan *bagman.ProcessResult
    UnpackChannel    chan *bagman.ProcessResult
	StorageChannel   chan *bagman.ProcessResult
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
// 4. Results channel: tells the queue whether processing
//    succeeded, and if not, whether the item should be requeued.
//    Also logs results to json and message logs.
// 5. Cleanup channel: cleans up the files after processing
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
	err := config.EnsureFluctusConfig()
    if err != nil {
        messageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
    }

	fluctusClient, err = client.New(
		config.FluctusURL,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	if err != nil {
		messageLog.Fatalf("Cannot initialize Fluctus Client: %v", err)
	}

    initVolume()
    initChannels()
    initGoRoutines()

	err = initS3Client()
    if err != nil {
        messageLog.Fatalf("Cannot initialize S3Client: %v", err)
    }

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxBagAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
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
    channels.CleanUpChannel = make(chan *bagman.ProcessResult, workerBufferSize)
    channels.ResultsChannel = make(chan *bagman.ProcessResult, workerBufferSize)
}

// Set up our go routines. We do NOT want one go routine per
// S3 file. If we do that, the system will run out of file handles,
// as we'll have tens of thousands of open connections to S3
// trying to write data into tens of thousands of local files.
func initGoRoutines() {
    for i := 0; i < config.Fetchers; i++ {
        go doFetch()
    }

    for i := 0; i < config.Workers; i++ {
        go doUnpack()
		go saveToStorage()
        go logResult()
        go doCleanUp()
    }
}

type BagProcessor struct {

}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*BagProcessor) HandleMessage(message *nsq.Message) (error) {
	message.DisableAutoResponse()
    var s3File bagman.S3File
    err := json.Unmarshal(message.Body, &s3File)
    if err != nil {
        messageLog.Println("[ERROR] Could not unmarshal JSON data from nsq:",
            string(message.Body))
        message.Finish()
        return nil
    }

	// If we're not reprocessing on purpose, and this item has already
	// been successfully processed, skip it. There are certain timing
	// conditions that can cause the bucket reader to add items to the
	// queue twice. If we get rid of NSQ, we can get rid of this check.
	if config.SkipAlreadyProcessed == true && needsProcessing(&s3File) == false {
		messageLog.Printf("[INFO] Marking %s as complete, without processing because " +
			"this bag was successfully processed previously and Config.SkipAlreadyProcessed " +
			"= true", s3File.Key.Key)
		message.Finish()
		return nil
	}

    // Create the result struct and pass it down the pipeline
    result := &bagman.ProcessResult{
        NsqMessage: message,
        S3File: &s3File,
        ErrorMessage: "",
        FetchResult: nil,
        TarResult: nil,
        BagReadResult: nil,
		FedoraResult: nil,
        Stage: "",
        Retry: true,
	}
    channels.FetchChannel <- result
    messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
    return nil
}

// Returns true if the file needs processing. We check this
// because the bucket reader may add duplicate items to the
// queue when the queue is long and the reader refills it hourly.
// If we get rid of NSQ and read directly from the
// database, we can get rid of this.
func needsProcessing(s3File *bagman.S3File) (bool) {
	bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	if err != nil {
		messageLog.Printf("[ERROR] Cannot parse S3File mod date '%s'. "+
			"File %s will be re-processed.",
			s3File.Key.LastModified, s3File.Key.Key)
		return true
	}
	etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status, err := fluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
	if err != nil {
		messageLog.Printf("[ERROR] Error getting status for file %s. Will reprocess.",
			s3File.Key.Key)
	}
	if status != nil && (status.Stage == "Record" && status.Status == "Success") {
		return false
	}
	return true
}

// -- Step 1 of 5 --
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

// -- Step 2 of 5 --
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

// -- Step 3 of 5 --
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
				continue
			}
			reader, err := os.Open(absPath)
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error opening file '%s'" +
					". File cannot be copied to long-term storage: %v",
					absPath, err)
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
			}

			// Save to S3 with the base64-encoded md5 sum
			base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)
			options := s3Client.MakeOptions(base64md5, s3Metadata)
			var url string = ""
			// Standard put to S3 for files < 5GB
			if gf.Size < S3_LARGE_FILE {
				url, err = s3Client.SaveToS3(
					config.PreservationBucket,
					gf.Uuid,
					gf.MimeType,
					reader,
					gf.Size,
					options)
			} else {
				// Multi-part put for files >= 5GB
				url, err = s3Client.SaveLargeFileToS3(
					config.PreservationBucket,
					gf.Uuid,
					gf.MimeType,
					reader,
					gf.Size,
					options,
				    S3_CHUNK_SIZE)
			}
			reader.Close()
			if err != nil {
				// Consider this error transient. Leave retry = true.
				result.ErrorMessage += fmt.Sprintf("Error copying file '%s'" +
					"to long-term storage: %v ", absPath, err)
				messageLog.Println("[ERROR]", "Failed to send",
					result.S3File.Key.Key,
					"to long-term storage:",
					err.Error())
			} else {
				gf.StorageURL = url
				gf.StoredAt = time.Now()
				messageLog.Printf("[INFO] Successfully sent %s (UUID %s)" +
					"to long-term storage bucket.", gf.Path, gf.Uuid)
			}
		}

		// If there were no errors, put this into the metadata
		// queue, so we can record the events in Fluctus.
		if result.ErrorMessage == "" {
			err := bagman.Enqueue(config.NsqdHttpAddress, config.MetadataTopic, result)
			if err != nil {
				errMsg := fmt.Sprintf("[ERROR] Error adding '%s' to metadata queue: %v ",
					result.S3File.Key.Key, err)
				messageLog.Println(errMsg)
				result.ErrorMessage += errMsg
			} else {
				messageLog.Printf("[INFO] Sent '%s' to metadata queue\n",
					result.S3File.Key.Key)
			}
		}

		// If some but not all files were copied to preservation, add an
		// entry to the trouble queue. We should review the items that
		// were copied and decide whether to delete them.
		if (result.TarResult.AnyFilesCopiedToPreservation() == true &&
			result.TarResult.AllFilesCopiedToPreservation() == false) {
			err := bagman.Enqueue(config.NsqdHttpAddress, config.TroubleTopic, result)
			if err != nil {
				messageLog.Printf("[ERROR] Could not send '%s' to trouble queue: %v\n",
					result.S3File.Key.Key, err)
			} else {
				messageLog.Printf("[WARN] Sent '%s' to trouble queue because some but not " +
					"all generic files were copied to preservation bucket\n", result.S3File.Key.Key)
			}

		}

		// Record the results.
		channels.ResultsChannel <- result
    }
}

// -- Step 4 of 5 --
// TODO: This code is duplicated in metarecord.go
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

// -- Step 5 of 5 --
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
            result.NsqMessage.Requeue(5 * time.Minute)
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