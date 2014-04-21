package main

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"path/filepath"
	"github.com/APTrust/bagman"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

var allBuckets = []string {
	"aptrust.receiving.columbia.edu",
	"aptrust.receiving.iub.edu",
	"aptrust.receiving.jhu.edu",
	"aptrust.receiving.ncsu.edu",
	"aptrust.receiving.stanford.edu",
	"aptrust.receiving.syr.edu",
	"aptrust.receiving.uchicago.edu",
	"aptrust.receiving.uc.edu",
	"aptrust.receiving.uconn.edu",
	"aptrust.receiving.umd.edu",
	"aptrust.receiving.miami.edu",
	"aptrust.receiving.umich.edu",
	"aptrust.receiving.unc.edu",
	"aptrust.receiving.und.edu",
	"aptrust.receiving.virginia.edu",
	"aptrust.receiving.vt.edu",
}

type S3File struct {
	BucketName     string
	Key            s3.Key
}

type TestResult struct {
	S3File         *S3File
	Error          error
	FetchResult    *bagman.FetchResult
	TarResult      *bagman.TarResult
	BagReadResult  *bagman.BagReadResult
}

type BucketSummary struct {
	BucketName     string
	Keys           []s3.Key
	MaxFileSize    int64
}

// Use these settings when running locally
var outputDir = "/Users/apd4n/tmp/"
var maxFileSize = int64(200000)  // ~200k

// Use these settings on S3
//var outputDir = "/mnt/apt_data"
//var maxFileSize = int64(20000000000)  // ~20GB

// Global vars. Should be moved to closure in results processing function
var waitGroup sync.WaitGroup
var succeeded = int64(0)
var failed = int64(0)
var bytesInS3 = int64(0)
var bytesProcessed = int64(0)

func main() {

	fetchers := 12
	workers := 4
	fetcherBufferSize := fetchers * 4
	workerBufferSize := workers * 2

	fetchChannel := make(chan S3File, fetcherBufferSize)
	unpackChannel := make(chan TestResult, workerBufferSize)
	cleanUpChannel := make(chan TestResult, workerBufferSize)
	resultsChannel := make(chan TestResult, workerBufferSize)

	bagman.LogDebug("Checking S3 bucket lists")
	bucketSummaries, err := CheckAllBuckets()
	if err != nil {
		bagman.LogError(err)
		return
	}
	bagman.LogDebug(fmt.Sprintf("Got info on %d buckets", len(bucketSummaries)))


	for i := 0; i < fetchers; i++ {
		go doFetch(unpackChannel, resultsChannel, fetchChannel)
	}
	for i := 0; i < workers; i++ {
		go doUnpack(resultsChannel, unpackChannel)
		go printResult(cleanUpChannel, resultsChannel)
		go doCleanUp(cleanUpChannel)
	}

	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			if key.Size < maxFileSize {
				fetchChannel <- S3File{bucketSummary.BucketName, key}
				waitGroup.Add(1)
				bagman.LogDebug(">> Put %s into fetch queue\n", key.Key)
			}
		}
	}
	waitGroup.Wait()
	printTotals()
}


// This runs as a go routine to fetch files from S3.
func doFetch(unpackChannel chan<- TestResult, resultsChannel chan<- TestResult, fetchChannel <-chan S3File) {
	for s3File := range fetchChannel {
		bagman.LogDebug(">> Fetching %s\n", s3File.Key.Key)
		fetchResult := Fetch(s3File.BucketName, s3File.Key)
		if fetchResult.Error != nil {
			resultsChannel <- TestResult{&s3File, fetchResult.Error, fetchResult, nil, nil}
		} else {
			unpackChannel <- TestResult{&s3File, nil, fetchResult, nil, nil}
		}
	}
}


// This runs as a go routine to untar files downloaded from S3.
func doUnpack(resultsChannel chan<- TestResult, unpackChannel <-chan TestResult) {
	for result := range unpackChannel {
		bagman.LogDebug(fmt.Sprintf(">> Unpacking %s\n", result.S3File.Key.Key))
		TestBagFile(&result)
		if result.Error != nil {
			resultsChannel <- result
		} else {
			resultsChannel <- result
		}
	}
}

// This runs as a go routine to remove the files we downloaded
// and untarred.
func doCleanUp(cleanUpChannel <-chan TestResult) {
	for result := range cleanUpChannel {
		bagman.LogDebug(fmt.Sprintf(">> Cleaning up %s\n", result.S3File.Key.Key))
		if result.FetchResult.LocalTarFile != "" {
			errors := CleanUp(result.FetchResult.LocalTarFile)
			if errors != nil && len(errors) > 0 {
				bagman.LogWarning("Errors cleaning up", result.FetchResult.LocalTarFile)
				for e := range errors {
					bagman.LogWarning(e)
				}
			}
		}
		waitGroup.Done()
	}
}

func printTotals() {
	bagman.LogInfo("-----------------------------------------------------------")
	bagman.LogInfo(fmt.Sprintf("Total Bags:       %d", succeeded + failed))
	bagman.LogInfo(fmt.Sprintf("Succeeded:        %d", succeeded))
	bagman.LogInfo(fmt.Sprintf("Failed:           %d", failed))
	bagman.LogInfo(fmt.Sprintf("Bytes in S3:      %d", bytesInS3))
	bagman.LogInfo(fmt.Sprintf("Bytes processed:  %d", bytesProcessed))
}

// This prints the result of the program's attempt to fetch, untar, unbag
// and verify an individual S3 tar file.
func printResult(cleanUpChannel chan<- TestResult, resultsChannel <-chan TestResult) {
	for result := range resultsChannel {
		bytesInS3 += result.S3File.Key.Size
		fmt.Println("")
		if(result.Error != nil) {
			failed++
			bagman.LogError(fmt.Sprintf("%s (ERROR) -> %s", result.S3File.Key.Key, result.Error))
		} else {
			succeeded++
			bytesProcessed += result.S3File.Key.Size
			bagman.LogInfo(fmt.Sprintf("%s (OK)", result.S3File.Key.Key))
		}
		printFetchResult(result.FetchResult)
		printTarResult(result.TarResult)
		printBagReadResult(result.BagReadResult)
		bagman.LogInfo("--- End of file", succeeded + failed, "---")
		cleanUpChannel <- result
	}
}

func printFetchResult(result *bagman.FetchResult) {
	if result == nil {
		bagman.LogInfo("  Could not fetch tar file from S3")
	} else {
		bagman.LogInfo("  Results of fetch from S3")
		bagman.LogInfo("    Remote md5:  ", result.RemoteMd5)
		bagman.LogInfo("    Local md5:   ", result.LocalMd5)
		bagman.LogInfo("    Error:       ", result.Error)
		bagman.LogInfo("    Warning:     ", result.Warning)
	}
}

func printTarResult(result *bagman.TarResult) {
	if result == nil {
		bagman.LogInfo("  Could not untar file")
	} else {
		bagman.LogInfo("  Results of untar")
		bagman.LogInfo("    Input file:  ", result.InputFile)
		bagman.LogInfo("    Output dir:   ", result.OutputDir)
		bagman.LogInfo("    Error:       ", result.Error)
		if result.Warnings != nil && len(result.Warnings) > 0 {
			bagman.LogWarning("    Warnings:")
			for _, warning := range result.Warnings {
				bagman.LogWarning("     ", warning)
			}
		}
		if result.FilesUnpacked != nil && len(result.FilesUnpacked) > 0 {
			bagman.LogInfo("    Files:   ")
			for _, file:= range result.FilesUnpacked {
				bagman.LogInfo("     ", file)
			}
		}
	}
}

func printBagReadResult(result *bagman.BagReadResult) {
	if result == nil {
		bagman.LogInfo("  Could not read bag")
	} else {
		bagman.LogInfo("  Results of bag read")
		bagman.LogInfo("    Path:       ", result.Path)
		bagman.LogInfo("    Error:      ", result.Error)
		if result.ChecksumErrors != nil {
			for _, err := range result.ChecksumErrors {
				bagman.LogWarning("    Checksum Error:", err)
			}
		}
		if result.Files != nil {
			bagman.LogInfo("    Files")
			for _, file := range result.Files {
				bagman.LogInfo("      ", file)
			}
		}
		if result.Tags != nil {
			bagman.LogInfo("    Tags")
			for _, tag := range result.Tags {
				bagman.LogInfo(fmt.Sprintf("      %s %s", tag.Label(), tag.Value()))
			}
		}
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
	tarFilePath := filepath.Join(outputDir, key.Key)
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
		fmt.Errorf("Error deleting dir %s: %s", untarredDir, err.Error())
		errors = append(errors, err)
	}
	return errors
}

// Collects info about all of the buckets listed in allBuckets.
func CheckAllBuckets() (bucketSummaries []*BucketSummary, err error) {
	bucketSummaries = make([]*BucketSummary, 0)
	for _, bucketName := range(allBuckets) {
		bucketSummary, err := CheckBucket(bucketName)
		if err != nil {
			return bucketSummaries, err
		}
		bucketSummaries = append(bucketSummaries, bucketSummary)
	}
	return bucketSummaries, nil
}

// Returns info about the contents of the bucket named bucketName.
// BucketSummary contains the bucket name, a list of keys, and the
// size of the largest file in the bucket.
func CheckBucket(bucketName string) (bucketSummary *BucketSummary, err error) {
	client, err := bagman.GetClient(aws.USEast)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(bucketName)
	if bucket == nil {
		err = errors.New(fmt.Sprintf("Cannot retrieve bucket: %s", bucketName))
		return nil, err
	}
	bucketSummary = new(BucketSummary)
	bucketSummary.BucketName = bucketName
	bucketSummary.Keys, err = bagman.ListBucket(bucket, 0)
	bucketSummary.MaxFileSize = GetMaxFileSize(bucketSummary.Keys)
	if err != nil {
		return nil, err
	}
	return bucketSummary, nil
}

// Returns the size in bytes of the largest file in the list of keys.
func GetMaxFileSize(keys []s3.Key) (maxsize int64) {
	for _, k := range keys {
		if k.Size > maxsize {
			maxsize = k.Size
		}
	}
	return maxsize
}

// Runs tests on the bag file at path and prints the
// results to stdout.
func TestBagFile(result *TestResult) {
	result.TarResult = bagman.Untar(result.FetchResult.LocalTarFile)
	if result.TarResult.Error != nil {
		result.Error = result.TarResult.Error
	} else {
		result.BagReadResult = bagman.ReadBag(result.TarResult.OutputDir)
		if result.BagReadResult.Error != nil {
			result.Error = result.BagReadResult.Error
		}
	}
}
