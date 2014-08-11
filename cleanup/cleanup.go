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
    "github.com/bitly/go-nsq"
	"github.com/diamondap/goamz/aws"
)

type Channels struct {
    CleanupChannel   chan *bagman.CleanupResult
    ResultsChannel   chan *bagman.CleanupResult
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
	nsqConfig.Set("max_attempts", uint16(config.MaxCleanupAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
    consumer, err := nsq.NewConsumer(config.CleanupTopic,
		config.CleanupChannel, nsqConfig)
    if err != nil {
        messageLog.Fatalf(err.Error())
    }

    handler := &CleanupProcessor{}
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
    jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "cleanup")
}


// Set up the channels.
func initChannels() {
    workerBufferSize := config.Workers * 10
    channels = &Channels{}
    channels.CleanupChannel = make(chan *bagman.CleanupResult, workerBufferSize)
    channels.ResultsChannel = make(chan *bagman.CleanupResult, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
    for i := 0; i < config.Workers; i++ {
        go logResult()
        go doCleanUp()
    }
}

// Initialize the reusable S3 client.
func initS3Client() (err error) {
    s3Client, err = bagman.NewS3Client(aws.USEast)
	return err
}

type CleanupProcessor struct {

}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*CleanupProcessor) HandleMessage(message *nsq.Message) (error) {
	message.DisableAutoResponse()
    var result bagman.CleanupResult
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
    channels.CleanupChannel <- &result
    messageLog.Println("[INFO]", "Put", result.BagName, "into cleanup channel")
    return nil
}

// TODO: This code is duplicated in bag_processor.go
func logResult() {
    for result := range channels.ResultsChannel {
        // Log full results to the JSON log
        json, err := json.Marshal(result)
        if err != nil {
            messageLog.Println("[ERROR]", err)
			messageLog.Printf("[INFO] Requeueing %s due to error", result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
            atomic.AddInt64(&failed, 1)
			logStats()
			continue
        }
        jsonLog.Println(string(json))

		// Log & requeue if something failed.
        if result.Succeeded() == false {
			messageLog.Printf("[INFO] Requeueing %s because at least one S3 delete failed",
				result.BagName)
            result.NsqMessage.Requeue(1 * time.Minute)
            atomic.AddInt64(&failed, 1)
			logStats()
			continue
        }

		// Mark item as resolved in Fluctus & tell the queue what happened
		err = MarkItemResolved(result)
        if err != nil {
			// TODO: This will just get retried forever, won't it?
			messageLog.Printf("[ERROR] Requeueing %s because we could not update Fluctus",
				result.BagName)
			result.NsqMessage.Requeue(1 * time.Minute)
            atomic.AddInt64(&failed, 1)
        } else {
			messageLog.Printf("[INFO] Cleanup of %s succeeded", result.BagName)
			result.NsqMessage.Finish()
            atomic.AddInt64(&succeeded, 1)
        }
		logStats()
    }
}

func logStats() {
	messageLog.Printf("[STATS] Succeeded: %d, Failed: %d\n", succeeded, failed)
}

func doCleanUp() {
    for result := range channels.CleanupChannel {
        messageLog.Println("[INFO]", "Cleaning up", result.BagName)
		if config.DeleteOnSuccess == true {
			DeleteS3Files(result)
		} else {
			// For testing...
			// result.Files[0].DeletedAt = time.Now()
		}
		channels.ResultsChannel <- result
    }
}

// Deletes each item in result.Files from S3.
func DeleteS3Files(result *bagman.CleanupResult) {
	for i := range result.Files {
		file := result.Files[i]
		err := s3Client.Delete(file.BucketName, file.Key)
		if err != nil {
			file.ErrorMessage += fmt.Sprintf("Error deleting file '%s' from " +
				"bucket '%s': %v ", file.Key, file.BucketName)
			messageLog.Println("[Error]", file.ErrorMessage)
		} else {
			file.DeletedAt = time.Now()
			messageLog.Printf("[INFO] Deleted original file '%s' from bucket '%s'",
				file.Key, file.BucketName)
		}
	}
}

// Tell Fluctus this ProcessedItem is resolved
func MarkItemResolved(result *bagman.CleanupResult) (error) {
	remoteStatus, err := fluctusClient.GetBagStatus(
		result.ETag, result.BagName, result.BagDate)
	if err != nil {
		messageLog.Println("[ERROR] Error getting ProcessedItem to Fluctus:", err)
		return err
	}
	if remoteStatus != nil {
		remoteStatus.Reviewed = false
		remoteStatus.Stage = StageCleanup
		remoteStatus.Status = StatusResolved
	}
	err = fluctusClient.UpdateBagStatus(remoteStatus)
	if err != nil {
		messageLog.Println("[ERROR] Error sending ProcessedItem to Fluctus:", err)
	} else {
		messageLog.Printf("[INFO] Updated status in Fluctus for %s: %s/%s\n",
			remoteStatus.Name, remoteStatus.Status, remoteStatus.Stage)
	}
	return err
}
