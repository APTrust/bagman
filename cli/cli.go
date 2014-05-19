package main

import (
	"fmt"
	"flag"
	"encoding/json"
	"os"
	"regexp"
	"sync/atomic"
	"log"
	"strings"
	"time"
	"path/filepath"
	"github.com/APTrust/bagman"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)



// Global vars.
var config bagman.Config
var jsonLog *log.Logger
var messageLog *log.Logger
var taskCounter = int64(0)
var volume *bagman.Volume


// TODO: Move these out of global namespace.
// It's probably not even safe to have multiple
// go routines updated these vars without synchronization.
var succeeded = int64(0)
var failed = int64(0)
var bytesInS3 = int64(0)
var bytesProcessed = int64(0)

// test.go
//
// Downloads and tests tarred bag files from S3.
//
// Usage:
//
// $ go run test.go -config=<some config>
//
// Configuration options are defined in config.json,
// in this project's top-level directory. You can see
// a list of available configurations by running:
//
// $ go run test.go
//
// This program does the following:
//
// 1. Scans all the receiving buckets in S3.
// 2. Downloads all of the tar files in those buckets.
// 3. Untars the files.
// 4. Parses the untarred files, extracting tags and
//    verifying md5 sums.
// 5. Logs a summary for each tar file, which includes
//    information about whether the file was successfully
//    fetched, untarred, parsed and verified.
// 6. Deletes all of the downloaded and untarred files.
//
// You'll find the log in the directory specified in
// the configuration. See config.json for configuration
// options.
func main() {

	// Load the config or die.
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "bagman_cli")
	bagman.PrintConfig(config)

	// Set up the volume to keep track of how much disk space is
	// available. We want to avoid downloading large files when
	// we know ahead of time that the volume containing the tar
	// directory doesn't have enough space to accomodate them.
	var err error
	volume, err = bagman.NewVolume(config.TarDirectory)
	if err != nil {
		panic(err.Error())
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
	fetcherBufferSize := config.Fetchers * 4
	workerBufferSize := config.Workers * 10

	fetchChannel := make(chan bagman.S3File, fetcherBufferSize)
	unpackChannel := make(chan bagman.ProcessResult, workerBufferSize)
	cleanUpChannel := make(chan bagman.ProcessResult, workerBufferSize)
	resultsChannel := make(chan bagman.ProcessResult, workerBufferSize)

	// Now fetch lists from S3 of what's in each bucket.
	messageLog.Println("[INFO]", "Checking S3 bucket lists")
	bucketSummaries, err := bagman.CheckAllBuckets(config.Buckets)
	if err != nil {
		messageLog.Println("[ERROR]", err)
		return
	}
	messageLog.Println("[INFO]", "Got info on", len(bucketSummaries), "buckets")


	// Set up our go routines. We do NOT want one go routine per
	// S3 file. If we do that, the system will run out of file handles,
	// as we'll have tens of thousands of open connections to S3
	// trying to write data into tens of thousands of local files.
	for i := 0; i < config.Fetchers; i++ {
		go doFetch(unpackChannel, resultsChannel, fetchChannel)
	}

	for i := 0; i < config.Workers; i++ {
		go doUnpack(resultsChannel, unpackChannel)
		go printResult(cleanUpChannel, resultsChannel)
		go doCleanUp(cleanUpChannel)
	}

	// Start adding S3 files into the fetch queue. Remember that this
	// queue blocks when it fills up, so we'll never be fetching more
	// than <queue size> files at once. That's what we want.
	for _, bucketSummary := range bucketSummaries {
		messageLog.Println("[INFO]", "Starting bucket", bucketSummary.BucketName,
			"which has", len(bucketSummary.Keys), "items")
		for _, key := range bucketSummary.Keys {
			if key.Size < config.MaxFileSize {
				if strings.HasSuffix(key.Key, ".tar") == false {
					messageLog.Println("[INFO]", "Ignoring non-tar file", key.Key,
						"in", bucketSummary.BucketName)
					continue
				}
				atomic.AddInt64(&taskCounter, 1)
				// TODO: Set attempt number correctly when queue is working.
				fetchChannel <- bagman.S3File{bucketSummary.BucketName, key, 1}
				messageLog.Println("[INFO]", "Put", key.Key, "into fetch queue")
			}
		}
	}

	// Let the go routines run and wait for all tasks to complete.
	// sync.WaitGroup is the standard way of doing this, but it
	// seems to always fail on our longer-running jobs, blocking forever.
	// It works fine on shorting jobs. Perhaps we're using it incorrectly.
	// WaitGroup is really for counting go routines. We have only a handful
	// of go routines, and a huge number of tasks. We're counting the tasks.
	waitForAllTasks()
	printTotals()
}



// This function blocks until all tasks are complete.
func waitForAllTasks() {
	for {
		if atomic.LoadInt64(&taskCounter) == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
}

// This runs as a go routine to fetch files from S3.
func doFetch(unpackChannel chan<- bagman.ProcessResult, resultsChannel chan<- bagman.ProcessResult, fetchChannel <-chan bagman.S3File) {
	for s3File := range fetchChannel {
		// Disk needs filesize * 2 disk space to accomodate tar file & untarred files
		err := volume.Reserve(uint64(s3File.Key.Size * 2))
		if err != nil {
			messageLog.Println("[WARNING]", "Requeueing", s3File.Key.Key, "- not enough disk space")
			resultsChannel <- bagman.ProcessResult{&s3File, err, nil, nil, nil, true}
		} else {
			messageLog.Println("[INFO]", "Fetching", s3File.Key.Key)
			fetchResult := Fetch(s3File.BucketName, s3File.Key)
			if fetchResult.Error != nil {
				resultsChannel <- bagman.ProcessResult{&s3File, fetchResult.Error, fetchResult, nil, nil,
					fetchResult.Retry}
			} else {
				unpackChannel <- bagman.ProcessResult{&s3File, nil, fetchResult, nil, nil, fetchResult.Retry}
			}
		}
	}
}

// This runs as a go routine to untar files downloaded from S3.
func doUnpack(resultsChannel chan<- bagman.ProcessResult, unpackChannel <-chan bagman.ProcessResult) {
	for result := range unpackChannel {
		if result.Error != nil {
			messageLog.Println("[INFO]", "Nothing to unpack for", result.S3File.Key.Key)
			resultsChannel <- result
		} else {
			messageLog.Println("[INFO]", "Unpacking", result.S3File.Key.Key)
			ProcessBagFile(&result)
			resultsChannel <- result
		}
	}
}

// This runs as a go routine to remove the files we downloaded
// and untarred.
func doCleanUp(cleanUpChannel <-chan bagman.ProcessResult) {
	for result := range cleanUpChannel {
		messageLog.Println("[INFO]", "Cleaning up", result.S3File.Key.Key)
		if result.FetchResult.LocalTarFile != "" {
			errors := CleanUp(result.FetchResult.LocalTarFile)
			if errors != nil && len(errors) > 0 {
				messageLog.Println("[WARNING]", "Errors cleaning up", result.FetchResult.LocalTarFile)
				for _, e := range errors {
					messageLog.Println("[ERROR]", e)
				}
			}
		}
		volume.Release(uint64(result.S3File.Key.Size * 2))
		atomic.AddInt64(&taskCounter, -1)
	}
}

// This prints to the log a summary of the total number of bags
// fetched and processed. This is printed at the end of the log.
func printTotals() {
	fmt.Println("-----------------------------------------------------------")
	fmt.Printf("Total Bags:       %d\n", succeeded + failed)
	fmt.Printf("Succeeded:        %d\n", succeeded)
	fmt.Printf("Failed:           %d\n", failed)
	fmt.Printf("Bytes in S3:      %d\n", bytesInS3)
	fmt.Printf("Bytes processed:  %d\n", bytesProcessed)
}

// This prints to the log the result of the program's attempt to fetch,
// untar, unbag and verify an individual S3 tar file.
func printResult(cleanUpChannel chan<- bagman.ProcessResult, resultsChannel <-chan bagman.ProcessResult) {
	for result := range resultsChannel {
		json, err := json.Marshal(result)
		if err != nil {
			messageLog.Println("[ERROR]", err)
		}
		jsonLog.Println(string(json))
		bytesInS3 += result.S3File.Key.Size
		if(result.Error != nil) {
			failed++
			messageLog.Println("[ERROR]",
				result.S3File.BucketName,
				result.S3File.Key.Key,
				"->", result.Error)
		} else {
			succeeded++
			bytesProcessed += result.S3File.Key.Size
			messageLog.Println("[INFO]", result.S3File.Key.Key, "-> OK")
		}
		cleanUpChannel <- result
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
		fmt.Fprintf(os.Stderr, "Error deleting dir %s: %s\n", untarredDir, err.Error())
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
