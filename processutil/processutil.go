package processutil

import (
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"github.com/bitly/go-nsq"
	"github.com/crowdmob/goamz/aws"
	"github.com/op/go-logging"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
)

/*
ProcessUtil sets up the items common to many of the bag
processing services (bag_processor, bag_restorer, cleanup,
etc.). It also encapsulates some functions common to all of
those services.
*/
type ProcessUtil struct {
	Config          bagman.Config
	JsonLog         *log.Logger
	MessageLog      *logging.Logger
	Volume          *bagman.Volume
	S3Client        *bagman.S3Client
	FluctusClient   *client.Client
	syncMap         *bagman.SynchronizedMap
	succeeded       int64
	failed          int64
}

/*
Creates and returns a new ProcessUtil object. Because some
items are absolutely required by this object and the processes
that use it, this method will panic if it gets an invalid
config param from the command line, or if it cannot set up some
essential services, such as logging.

This object is meant to used as a singleton with any of the
stand-along processing services (bag_processor, bag_restorer,
cleanup, etc.).

Param requestedConfig should be the name of a valid configuration
in the config.json file ("dev", "test", etc.).
*/
func NewProcessUtil(requestedConfig *string) (procUtil *ProcessUtil) {
	procUtil = &ProcessUtil {
		succeeded: int64(0),
		failed: int64(0),
	}
	procUtil.Config = bagman.LoadRequestedConfig(requestedConfig)
	procUtil.initLogging()
	procUtil.initVolume()
	procUtil.initS3Client()
	procUtil.initFluctusClient()
	procUtil.syncMap = bagman.NewSynchronizedMap()
	return procUtil
}

// Loads environment variables from the specified file,
// and assumes vars are in the format
//
// export VAR=VALUE
//
// Values can be quoted. If command-line arg -env names
// a file that does not exist, this causes a fatal error.
func (procUtil *ProcessUtil) LoadCustomEnv(customEnvFile *string) {
	if customEnvFile != nil && *customEnvFile != "" {
		err := bagman.LoadEnv(*customEnvFile)
		if err != nil {
			procUtil.MessageLog.Fatalf("Cannot load custom environment file '%s'. " +
				"Is that an absolute file path? Error: %v",
				*customEnvFile, err)
		}
	}
}

// Initializes the loggers.
func (procUtil *ProcessUtil) initLogging() {
	procUtil.MessageLog = bagman.InitLogger(procUtil.Config)
	procUtil.JsonLog = bagman.InitJsonLogger(procUtil.Config)
}

// Sets up a new Volume object to track estimated disk usage.
func (procUtil *ProcessUtil) initVolume() {
	volume, err := bagman.NewVolume(procUtil.Config.TarDirectory, procUtil.MessageLog)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot init Volume object: %v", err)
		fmt.Fprintln(os.Stderr, message)
		procUtil.MessageLog.Fatal(message)
	}
	procUtil.Volume = volume
}

// Initializes a reusable S3 client.
func (procUtil *ProcessUtil) initS3Client() {
	s3Client, err := bagman.NewS3Client(aws.USEast)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot init S3 client: %v", err)
		fmt.Fprintln(os.Stderr, message)
		procUtil.MessageLog.Fatal(message)
	}
	procUtil.S3Client = s3Client
}

// Initializes a reusable Fluctus client.
func (procUtil *ProcessUtil) initFluctusClient() {
	fluctusClient, err := client.New(
		procUtil.Config.FluctusURL,
		procUtil.Config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		procUtil.MessageLog)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot initialize Fluctus Client: %v", err)
		fmt.Fprintln(os.Stderr, message)
		procUtil.MessageLog.Fatal(message)
	}
	procUtil.FluctusClient = fluctusClient
}

// Returns the number of processed items that succeeded.
func (procUtil *ProcessUtil) Succeeded() (int64) {
	return procUtil.succeeded
}

// Returns the number of processed items that failed.
func (procUtil *ProcessUtil) Failed() (int64) {
	return procUtil.failed
}

// Increases the count of successfully processed items by one.
func (procUtil *ProcessUtil) IncrementSucceeded() (int64) {
	atomic.AddInt64(&procUtil.succeeded, 1)
	return procUtil.succeeded
}

// Increases the count of unsuccessfully processed items by one.
func (procUtil *ProcessUtil) IncrementFailed() (int64) {
	atomic.AddInt64(&procUtil.failed, 1)
	return procUtil.succeeded
}

/*
Registers an item currently being processed so we can keep track
of duplicates. Many requests for ingest, restoration, etc. may be
queued more than once. Register an item here to note that it is
being processed under a specific message id. If they item comes in
again before we're done processing, and you try to register it here,
you'll get an error saying the item is already in process.

The key should be a unique identifier. For intellectual objects,
this can be the IntellectualObject.Identifier. For S3 files, it can
be bucket_name/file_name.
*/
func (procUtil *ProcessUtil) RegisterItem(key string, messageId nsq.MessageID) (error) {
	messageIdString := procUtil.MessageIdString(messageId)
	if procUtil.syncMap.HasKey(key) {
		otherId := procUtil.syncMap.Get(key)
		sameOrDifferent := "a different"
		if otherId == messageIdString {
			sameOrDifferent = "the same"
		}
		return fmt.Errorf("Item is already being processed under %s messageId (%s)",
			sameOrDifferent, otherId)
	}
	// Make a note that we're processing this file.
	procUtil.syncMap.Add(key, messageIdString)
	return nil
}

/*
UnregisterItem removes the item with specified key from the list
of items we are currently processing. Be sure to call this when you're
done processing any item you've registered so we know we're finished
with it and we can reprocess it later, under a different message id.
*/
func (procUtil *ProcessUtil) UnregisterItem(key string) {
	procUtil.syncMap.Delete(key)
}

/*
Returns the NSQ MessageId under which the current item is being
processed, or an empty string if no item with that key is currently
being processed.
*/
func (procUtil *ProcessUtil) MessageIdFor(key string) (string) {
	if procUtil.syncMap.HasKey(key) {
		return procUtil.syncMap.Get(key)
	}
	return ""
}

// Converts an NSQ MessageID to a string.
func (procUtil *ProcessUtil) MessageIdString(messageId nsq.MessageID) (string) {
	messageIdBytes := make([]byte, nsq.MsgIDLength)
	for i := range messageId {
		messageIdBytes[i] = messageId[i]
	}
	return string(messageIdBytes)
}

// Logs info about the number of items that have succeeded and failed.
func (procUtil *ProcessUtil) LogStats() {
	procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		procUtil.Succeeded(), procUtil.Failed())
}


/*
Returns true if the bag is currently being processed. This handles a
special case where a very large bag is in process for a long time,
the NSQ message times out, then NSQ re-sends the same message with
the same ID to this worker. Without these checks, the worker will
accept the message and will be processing it twice. This causes
problems because the first working will be deleting files while the
second working is trying to run checksums on them.
*/
func (procUtil *ProcessUtil) BagAlreadyInProgress(s3File *bagman.S3File, currentMessageId string) (bool) {
	// Bag is in process if it's in the registry.
	messageId := procUtil.MessageIdFor(s3File.BagName())
	if messageId == currentMessageId {
		return true
	}

	re := regexp.MustCompile("\\.tar$")
	bagDir := re.ReplaceAllString(s3File.Key.Key, "")
	tarFilePath := filepath.Join(procUtil.Config.TarDirectory, s3File.Key.Key)
	unpackDir := filepath.Join(procUtil.Config.TarDirectory, bagDir)

	// Bag is in process if we have its files on disk.
	return bagman.FileExists(unpackDir) || bagman.FileExists(tarFilePath)
}
