package main

import (
	"errors"
	"fmt"
	"os"
	"regexp"
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

	// completedChannel size is a hack. See TODO comment below.
	// also, worker buffer size should be bigger. The workers are slacking!
	completedChannel := make(chan bool, 1000)

	fmt.Println("Checking S3 bucket lists")
	bucketSummaries, err := CheckAllBuckets()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Got info on", len(bucketSummaries), "buckets")


	for i := 0; i < fetchers; i++ {
		go doFetch(unpackChannel, resultsChannel, fetchChannel)
	}
	for i := 0; i < workers; i++ {
		go doUnpack(resultsChannel, unpackChannel)
		go printResult(cleanUpChannel, resultsChannel)
		go doCleanUp(completedChannel, cleanUpChannel)
	}

	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			if key.Size < maxFileSize {
				fetchChannel <- S3File{bucketSummary.BucketName, key}
				fmt.Printf(">> Put %s into fetch queue\n", key.Key)
			}
		}
	}

	// TODO: Fix this. We don't start reading from the completed channel
	// until the above for loop is done. By then, we have fetched just
	// about everything. The completed channel fills up an N number of items,
	// and then blocks all of the other channels.
	//
	// - completedChannel should have a limited size
	// - it must know the number of items being fetched
	// - its go routine must start BEFORE we start filling the fetchChannel
	itemsToFetch := len(fetchChannel)
	doneCount := 0
	for _ = range completedChannel {
		doneCount++
		if doneCount == itemsToFetch {
			break
		}
	}
	fmt.Println("-----------------------------------------------------------")
	fmt.Printf("Total Bags:       %d\n", succeeded + failed)
	fmt.Printf("Succeeded:        %d\n", succeeded)
	fmt.Printf("Failed:           %d\n", failed)
	fmt.Printf("Bytes in S3:      %d\n", bytesInS3)
	fmt.Printf("Bytes processed:  %d\n", bytesProcessed)
}


// This runs as a go routine to fetch files from S3.
func doFetch(unpackChannel chan<- TestResult, resultsChannel chan<- TestResult, fetchChannel <-chan S3File) {
	for s3File := range fetchChannel {
		fmt.Printf(">> Fetching %s\n", s3File.Key.Key)
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
		fmt.Printf(">> Unpacking %s\n", result.S3File.Key.Key)
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
func doCleanUp(completedChannel chan<- bool, cleanUpChannel <-chan TestResult) {
	for result := range cleanUpChannel {
		fmt.Printf(">> Cleaning up %s\n", result.S3File.Key.Key)
		if result.FetchResult.LocalTarFile == "" {
			completedChannel <- true
			return
		}
		errors := CleanUp(result.FetchResult.LocalTarFile)
		if errors != nil && len(errors) > 0 {
			fmt.Println("Errors cleaning up", result.FetchResult.LocalTarFile)
			for e := range errors {
				fmt.Println(e)
			}
		}
		completedChannel <- true
	}
}

// This prints the result of the program's attempt to fetch, untar, unbag
// and verify an individual S3 tar file.
func printResult(cleanUpChannel chan<- TestResult, resultsChannel <-chan TestResult) {
	for result := range resultsChannel {
		bytesInS3 += result.S3File.Key.Size
		fmt.Println("")
		if(result.Error != nil) {
			failed++
			fmt.Printf("%s [ERROR] -> %s\n", result.S3File.Key.Key, result.Error)
		} else {
			succeeded++
			bytesProcessed += result.S3File.Key.Size
			fmt.Printf("%s [OK]\n", result.S3File.Key.Key)
		}
		printFetchResult(result.FetchResult)
		printTarResult(result.TarResult)
		printBagReadResult(result.BagReadResult)
		fmt.Println("--- End of file", succeeded + failed, "---")
		cleanUpChannel <- result
	}
}

func printFetchResult(result *bagman.FetchResult) {
	if result == nil {
		fmt.Println("  Could not fetch tar file from S3")
	} else {
		fmt.Println("  Results of fetch from S3")
		fmt.Println("    Remote md5:  ", result.RemoteMd5)
		fmt.Println("    Local md5:   ", result.LocalMd5)
		fmt.Println("    Error:       ", result.Error)
		fmt.Println("    Warning:     ", result.Warning)
	}
}

func printTarResult(result *bagman.TarResult) {
	if result == nil {
		fmt.Println("  Could not untar file")
	} else {
		fmt.Println("  Results of untar")
		fmt.Println("    Input file:  ", result.InputFile)
		fmt.Println("    Output dir:   ", result.OutputDir)
		fmt.Println("    Error:       ", result.Error)
		if result.Warnings != nil && len(result.Warnings) > 0 {
			fmt.Println("    Warnings:")
			for _, warning := range result.Warnings {
				fmt.Println("     ", warning)
			}
		}
		if result.FilesUnpacked != nil && len(result.FilesUnpacked) > 0 {
			fmt.Println("    Files:   ")
			for _, file:= range result.FilesUnpacked {
				fmt.Println("     ", file)
			}
		}
	}
}

func printBagReadResult(result *bagman.BagReadResult) {
	if result == nil {
		fmt.Println("  Could not read bag")
	} else {
		fmt.Println("  Results of bag read")
		fmt.Println("    Path:       ", result.Path)
		fmt.Println("    Error:      ", result.Error)
		if result.ChecksumErrors != nil {
			for _, err := range result.ChecksumErrors {
				fmt.Println("    Checksum Error:", err)
			}
		}
		if result.Files != nil {
			fmt.Println("    Files")
			for _, file := range result.Files {
				fmt.Println("      ", file)
			}
		}
		if result.Tags != nil {
			fmt.Println("    Tags")
			for _, tag := range result.Tags {
				fmt.Printf("      %s %s\n", tag.Label(), tag.Value())
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
