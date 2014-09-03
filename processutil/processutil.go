package processutil

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"github.com/diamondap/goamz/aws"
	"github.com/op/go-logging"
	"log"
	"os"
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
	Succeeded       int64
	Failed          int64
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
*/
func NewProcessUtil() (procUtil *ProcessUtil) {
	procUtil = &ProcessUtil {
		Succeeded: int64(0),
		Failed: int64(0),
	}
	procUtil.loadConfig()
	procUtil.initLogging()
	procUtil.initVolume()
	procUtil.initS3Client()
	procUtil.initFluctusClient()
	return procUtil
}

// Loads whatever config was requested on the command line.
// WILL DIE IF CONFIG IS MISSING OR INVALID!!
func (procUtil *ProcessUtil) loadConfig() {
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	procUtil.Config = bagman.LoadRequestedConfig(requestedConfig)
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

// TODO: This code is duplicated in bag_processor.go
func (procUtil *ProcessUtil) LogResult(result *bagman.ProcessResult) {
	// Log full results to the JSON log
	json, err := json.Marshal(result)
	if err != nil {
		procUtil.MessageLog.Error(err.Error())
	}
	procUtil.JsonLog.Println(string(json))

	// Add a message to the message log
	if result.ErrorMessage != "" {
		atomic.AddInt64(&procUtil.Failed, 1)
		procUtil.MessageLog.Error("%s %s -> %s",
			result.S3File.BucketName,
			result.S3File.Key.Key,
			result.ErrorMessage)
	} else {
		atomic.AddInt64(&procUtil.Succeeded, 1)
		procUtil.MessageLog.Info("%s -> finished OK", result.S3File.Key.Key)
	}

	// Add some stats to the message log
	procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		procUtil.Succeeded, procUtil.Failed)
}
