package main

import (
	"fmt"
	"flag"
	"encoding/json"
	"os"
	"regexp"
	"sync/atomic"
	"log"
	"time"
	"path/filepath"
	"github.com/APTrust/bagman"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"github.com/bitly/go-nsq"
)

type Channels struct {
	FetchChannel     chan bagman.ProcessResult
	UnpackChannel    chan bagman.ProcessResult
	CleanUpChannel   chan bagman.ProcessResult
	ResultsChannel   chan bagman.ProcessResult
}


// Global vars.
var channels *Channels
var config bagman.Config
var jsonLog *log.Logger
var messageLog *log.Logger
var taskCounter = int64(0)
var volume *bagman.Volume
var succeeded = int64(0)
var failed = int64(0)
var bytesInS3 = int64(0)
var bytesProcessed = int64(0)


func main() {

	loadConfig()
	initVolume()
	initChannels()
	initGoRoutines(channels)

	// ----------------------------------------------------------------
	// START HERE
	// Bind message handler here!!!
	// See https://github.com/APTrust/bagman/blob/develop/nsqtest/nsqtest.go
	// ----------------------------------------------------------------


	waitForAllTasks()
}


func loadConfig() {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "bagman_cli")
}

// Set up the volume to keep track of how much disk space is
// available. We want to avoid downloading large files when
// we know ahead of time that the volume containing the tar
// directory doesn't have enough space to accomodate them.
func initVolume() {
	var err error
	volume, err = bagman.NewVolume(config.TarDirectory)
	if err != nil {
		panic(err.Error())
	}
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
	channels.FetchChannel = make(chan bagman.ProcessResult, fetcherBufferSize)
	channels.UnpackChannel = make(chan bagman.ProcessResult, workerBufferSize)
	channels.CleanUpChannel = make(chan bagman.ProcessResult, workerBufferSize)
	channels.ResultsChannel = make(chan bagman.ProcessResult, workerBufferSize)
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
		go logResult()
		go doCleanUp()
	}
}

func HandleMessage(message *nsq.Message, outputChannel chan *nsq.FinishedMessage) {
	fmt.Println(string(message.Body))
	message.Attempts++
	atomic.AddInt64(&taskCounter, 1)

	var s3File bagman.S3File
	err := json.Unmarshal(message.Body, &s3File)
	if err != nil {
		messageLog.Println("[ERROR] Could not unmarshal JSON data from nsq")
		finishedMessage := &nsq.FinishedMessage{message.Id, 1000, false}
		outputChannel <- finishedMessage
	}

	// Create the result struct and pass it down the pipeline
	result := bagman.ProcessResult{
		message,
		outputChannel,
		&s3File,
		nil,
		nil,
		nil,
		nil,
		true}
	channels.FetchChannel <- result
	messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
}


// This function blocks until all tasks are complete.
// sync.WaitGroup is the standard way of doing this, but
// WaitGroup is really for counting go routines. We have only a handful
// of go routines, and a huge number of tasks. We're counting the tasks.
func waitForAllTasks() {
	for {
		if atomic.LoadInt64(&taskCounter) == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
}

// This runs as a go routine to fetch files from S3.
func doFetch() {
	for result := range channels.FetchChannel {
		s3Key := result.S3File.Key
		// Disk needs filesize * 2 disk space to accomodate tar file & untarred files
		err := volume.Reserve(uint64(s3Key.Size * 2))
		if err != nil {
			// Not enough room on disk
			messageLog.Println("[WARNING]", "Requeueing",
				s3Key.Key, "- not enough disk space")
			result.Error = err
			result.Retry = true
			channels.ResultsChannel <- result
		} else {
			messageLog.Println("[INFO]", "Fetching", s3Key)
			fetchResult := Fetch(result.S3File.BucketName, s3Key)
			result.FetchResult = fetchResult
			result.Retry = fetchResult.Retry
			if fetchResult.Error != nil {
				// Fetch from S3 failed. Requeue.
				result.Error = fetchResult.Error
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

// This runs as a go routine to untar files downloaded from S3.
func doUnpack() {
	for result := range channels.UnpackChannel {
		if result.Error != nil {
			// Unpack failed. Go to end.
			messageLog.Println("[INFO]", "Nothing to unpack for",
				result.S3File.Key.Key)
			channels.ResultsChannel <- result
		} else {
			// Unpacked! Now process the bag and touch message
			// so nsqd knows we're making progress.
			messageLog.Println("[INFO]", "Unpacking", result.S3File.Key.Key)
			result.NsqMessage.Touch()
			ProcessBagFile(&result)
			channels.ResultsChannel <- result
		}
	}
}

// This runs as a go routine to remove the files we downloaded
// and untarred.
func doCleanUp() {
	for result := range channels.CleanUpChannel {
		messageLog.Println("[INFO]", "Cleaning up", result.S3File.Key.Key)
		if result.FetchResult.LocalTarFile != "" {
			// Clean up any files we downloaded and unpacked
			errors := CleanUp(result.FetchResult.LocalTarFile)
			if errors != nil && len(errors) > 0 {
				messageLog.Println("[WARNING]", "Errors cleaning up",
					result.FetchResult.LocalTarFile)
				for _, e := range errors {
					messageLog.Println("[ERROR]", e)
				}
			}
		}
		// Let our volumn tracker know we just freed up some disk space.
		volume.Release(uint64(result.S3File.Key.Size * 2))
		// Reduce outstanding task count.
		atomic.AddInt64(&taskCounter, -1)

		// Build and send message back to NSQ, indicating whether
		// processing succeeded.
		succeeded := true
		requeueDelayMs := 0
		if result.Error != nil {
			succeeded = false
			requeueDelayMs = 60000
		}
		finishedMessage := &nsq.FinishedMessage{
			result.NsqMessage.Id,
			requeueDelayMs,
			succeeded}
		result.NsqOutputChannel <- finishedMessage
	}
}


// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file.
func logResult() {
	for result := range channels.ResultsChannel {
		json, err := json.Marshal(result)
		if err != nil {
			messageLog.Println("[ERROR]", err)
		}
		jsonLog.Println(string(json))
		atomic.AddInt64(&bytesInS3, int64(result.S3File.Key.Size))
		if(result.Error != nil) {
			atomic.AddInt64(&failed, 1)
			messageLog.Println("[ERROR]",
				result.S3File.BucketName,
				result.S3File.Key.Key,
				"->", result.Error)
		} else {
			atomic.AddInt64(&succeeded, 1)
			atomic.AddInt64(&bytesProcessed, int64(result.S3File.Key.Size))
			messageLog.Println("[INFO]", result.S3File.Key.Key, "-> finished OK")
		}
		channels.CleanUpChannel <- result
	}
}


// This fetches a file from S3 and stores it locally.
func Fetch(bucketName string, key s3.Key) (result *bagman.FetchResult) {
	client, err := bagman.GetClient(aws.USEast)
	if err != nil {
		fetchResult := new(bagman.FetchResult)
		fetchResult.Error = err
		return fetchResult
	}
	// TODO: We fetched this bucket before. Do we need to fetch it again?
	bucket := client.Bucket(bucketName)
	tarFilePath := filepath.Join(config.TarDirectory, key.Key)
	return bagman.FetchToFile(bucket, key, tarFilePath)
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
	result.TarResult = bagman.Untar(result.FetchResult.LocalTarFile)
	if result.TarResult.Error != nil {
		result.Error = result.TarResult.Error
		// If we can't untar this, there's no reason to retry...
		// but we'll have to revisit this. There may be cases
		// where we do want to retry, such as if disk was full.
		result.Retry = false
	} else {
		result.BagReadResult = bagman.ReadBag(result.TarResult.OutputDir)
		if result.BagReadResult.Error != nil {
			result.Error = result.BagReadResult.Error
			// Something was wrong with this bag. Bad checksum,
			// missing file, etc. Don't reprocess it.
			result.Retry = false
		}
	}
}
