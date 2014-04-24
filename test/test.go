package main

import (
	"errors"
	"fmt"
	"flag"
	"encoding/json"
	"os"
	"regexp"
	"sync"
	"io/ioutil"
	"log"
	"path/filepath"
	"github.com/APTrust/bagman"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

// These are all the buckets we want to check.
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

type Config struct {
	TarDirectory   string
	LogDirectory   string
	MaxFileSize    int64
	LogLevel       bagman.LogLevel
	Fetchers       int
	Workers        int
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


// Global vars.
var config Config
var waitGroup sync.WaitGroup
var jsonLog *log.Logger
var messageLog *log.Logger

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

	config = loadRequestedConfig()
	jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory)
	printConfig(config)

	fetcherBufferSize := config.Fetchers * 4
	workerBufferSize := config.Workers * 2

	fetchChannel := make(chan S3File, fetcherBufferSize)
	unpackChannel := make(chan TestResult, workerBufferSize)
	cleanUpChannel := make(chan TestResult, workerBufferSize)
	resultsChannel := make(chan TestResult, workerBufferSize)

	messageLog.Println("[INFO]", "Checking S3 bucket lists")
	bucketSummaries, err := CheckAllBuckets()
	if err != nil {
		messageLog.Println("[ERROR]", err)
		return
	}
	messageLog.Println("[INFO]", "Got info on", len(bucketSummaries), "buckets")


	for i := 0; i < config.Fetchers; i++ {
		go doFetch(unpackChannel, resultsChannel, fetchChannel)
	}
	for i := 0; i < config.Workers; i++ {
		go doUnpack(resultsChannel, unpackChannel)
		go printResult(cleanUpChannel, resultsChannel)
		go doCleanUp(cleanUpChannel)
	}

	for _, bucketSummary := range bucketSummaries {
		for _, key := range bucketSummary.Keys {
			if key.Size < config.MaxFileSize {
				fetchChannel <- S3File{bucketSummary.BucketName, key}
				waitGroup.Add(1)
				messageLog.Println("[INFO]", "Put", key.Key, "into fetch queue")
			}
		}
	}
	waitGroup.Wait()
	printTotals()
}

// This returns the configuration that the user requested.
// If the user did not specify any configuration (using the
// -config flag), or if the specified configuration cannot
// be found, this prints a help message and terminates the
// program.
func loadRequestedConfig() (config Config){
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	configurations := loadConfigFile()
	config, configExists := configurations[*requestedConfig]
	if requestedConfig == nil || !configExists  {
		printConfigHelp(*requestedConfig, configurations)
		os.Exit(1)
	}
	return config
}

// This prints the current configuration to stdout.
func printConfig(config Config) {
	fmt.Println("Running with the following configuration:")
	fmt.Printf("    Tar Directory: %s\n", config.TarDirectory)
	fmt.Printf("    Log Directory: %s\n", config.LogDirectory)
	fmt.Printf("    Log Level:     %d\n", config.LogLevel)
	fmt.Printf("    Max File Size: %d\n", config.MaxFileSize)
	fmt.Printf("    Fetchers:      %d\n", config.Fetchers)
	fmt.Printf("    Workers:       %d\n", config.Workers)
	fmt.Printf("Output will be logged to bagman_json and bagman_messages in %s\n", config.LogDirectory)
}

// This prints a message to stdout describing how to specify
// a valid configuration.
func printConfigHelp(requestedConfig string, configurations map[string]Config) {
	fmt.Fprintf(os.Stderr, "Unrecognized config '%s'\n", requestedConfig)
	fmt.Fprintln(os.Stderr, "Please specify one of the following configurations:")
	for name, _ := range configurations {
		fmt.Println(name)
	}
	os.Exit(1)
}

// This function reads the config.json file and returns a list of
// available configurations.
func loadConfigFile() (configurations map[string]Config) {
	file, err := ioutil.ReadFile("../config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	}
	err = json.Unmarshal(file, &configurations)
	if err != nil{
		fmt.Fprint(os.Stderr, "Error parsing JSON from config file:", err)
		os.Exit(1)
	}
	return configurations
}

// This runs as a go routine to fetch files from S3.
func doFetch(unpackChannel chan<- TestResult, resultsChannel chan<- TestResult, fetchChannel <-chan S3File) {
	for s3File := range fetchChannel {
		messageLog.Println("[INFO]", "Fetching", s3File.Key.Key)
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
		messageLog.Println("[INFO]", "Unpacking", result.S3File.Key.Key)
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
		messageLog.Println("[INFO]", "Cleaning up", result.S3File.Key.Key)
		if result.FetchResult.LocalTarFile != "" {
			errors := CleanUp(result.FetchResult.LocalTarFile)
			if errors != nil && len(errors) > 0 {
				messageLog.Println("[WARNING]", "Errors cleaning up", result.FetchResult.LocalTarFile)
				for e := range errors {
					messageLog.Println("[ERROR]", e)
				}
			}
		}
		waitGroup.Done()
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
func printResult(cleanUpChannel chan<- TestResult, resultsChannel <-chan TestResult) {
	for result := range resultsChannel {
		json, err := json.Marshal(result)
		if err != nil {
			messageLog.Println("[ERROR]", err)
		}
		jsonLog.Println(string(json))
		bytesInS3 += result.S3File.Key.Size
		if(result.Error != nil) {
			failed++
			messageLog.Println("[ERROR]", result.S3File.Key.Key, "->", result.Error)
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

// Runs tests on the bag file at path and returns information about
// whether it was successfully unpacked, valid and complete.
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
