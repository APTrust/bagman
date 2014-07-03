// BucketReader gets a list of files awaiting processing in the S3
// intake buckets and adds each item to the queue for processing.
package main

import (
    "bytes"
    "flag"
    "log"
    "fmt"
    "strings"
    "time"
    "os"
    "encoding/json"
    "net/http"
	"github.com/crowdmob/goamz/aws"
    "github.com/APTrust/bagman"
    "github.com/APTrust/bagman/fluctus/client"
)

// Send S3 files to queue in batches of 500.
// Wait X milliseconds between batches. The wait time is really
// only necessary when the queue is running on an AWS small EC2
// instance, where the number of open network connections is
// severely restricted.
const (
    batchSize = 500
    waitMilliseconds = 5000
)
var (
    config bagman.Config
    jsonLog *log.Logger
    messageLog *log.Logger
    fluctusClient *client.Client
)

func main() {
    err := initialize()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Initialization failed for bucket_reader: %v", err)
        os.Exit(1)
    }
    run()
}

func initialize() (err error) {
    // Load the config or die.
    requestedConfig := flag.String("config", "", "configuration to run")
    flag.Parse()
    config = bagman.LoadRequestedConfig(requestedConfig)
    jsonLog, messageLog = bagman.InitLoggers(config.LogDirectory, "bucket_reader")
    fluctusClient, err = client.New(
        config.FluctusURL,
        os.Getenv("FLUCTUS_API_USER"),
        os.Getenv("FLUCTUS_API_KEY"),
        messageLog)
    return err
}


func run() {
    s3Client, err := bagman.NewS3Client(aws.USEast)
    if err != nil {
        messageLog.Println("[ERROR]", err)
        return
    }
    bucketSummaries, err := s3Client.CheckAllBuckets(config.Buckets)
    if err != nil {
        messageLog.Println("[ERROR]", err)
        return
    }
    url := fmt.Sprintf("%s/mput?topic=%s", config.NsqdHttpAddress,
        config.BagProcessorTopic)
    messageLog.Printf("[INFO] Sending S3 file info to %s \n", url)
    s3Files := filterLargeFiles(bucketSummaries)
    messageLog.Printf("[INFO] %d S3 Files are within our size limit\n",
        len(s3Files))
    filesToProcess := s3Files
    // SkipAlreadyProcessed will almost always be true.
    // The exception is when we want to reprocess items to test new code.
    if config.SkipAlreadyProcessed == true {
		messageLog.Println("[INFO] Skipping already processed files, because config says so")
        filesToProcess = filterProcessedFiles(s3Files)
    } else {
		messageLog.Println("[INFO] Reprocessing already processed files, because config says so")
	}
    start := 0
    end := min(len(filesToProcess), batchSize)
    messageLog.Printf("[INFO] %d Unprocessed files\n", len(filesToProcess))
    for start <= end {
        batch := filesToProcess[start:end]
        messageLog.Printf("[INFO] Queuing batch of %d items\n", len(batch))
        enqueue(url, batch)
        start = end + 1
        if start < len(filesToProcess) {
            end = min(len(filesToProcess), start + batchSize)
        }
        // Sleep so we don't max out connections on EC2 small.
        // The utility server is running a lot of other network I/O
        // in addition to our queue.
        time.Sleep(time.Millisecond * waitMilliseconds)
    }
}

// min returns the minimum of x or y. The Math package has this function
// but you have to cast to floats.
func min (x, y int) (int) {
    if x < y {
        return x
    } else {
        return y
    }
}


// filterLargeFiles returns only those S3 files that are not larger
// than config.MaxFileSize. This is useful when running tests and
// demos on your local machine, so that you can limit your test
// runs to files under 100k (or whatever you set in config.json).
// You don't want to pull down lots of multi-gig files when you're
// just running local tests. In production, set maxFileSize to
// zero, or to some huge value to get all files.
func filterLargeFiles (bucketSummaries []*bagman.BucketSummary) (s3Files []*bagman.S3File) {
    for _, bucketSummary := range bucketSummaries {
        for _, key := range bucketSummary.Keys {
            if config.MaxFileSize == 0 || key.Size < config.MaxFileSize {
                s3Files = append(s3Files, &bagman.S3File{bucketSummary.BucketName, key})
            }
        }
    }
    return s3Files
}

// Remove S3 files that have been processed successfully.
// No need to reprocess those!
func filterProcessedFiles (s3Files []*bagman.S3File) (filesToProcess []*bagman.S3File) {
    for _, s3File := range s3Files {
        bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
        if err != nil {
            messageLog.Printf("[ERROR] Cannot parse S3File mod date '%s'. " +
                "File %s will be re-processed.",
                s3File.Key.LastModified, s3File.Key.Key)
            filesToProcess = append(filesToProcess, s3File)
            continue
        }
        etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
        status, err := fluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
        if err != nil {
            messageLog.Printf("[ERROR] Cannot get Fluctus bag status for %s. " +
                "Will re-process bag. Error was %v", s3File.Key.Key, err)
            filesToProcess = append(filesToProcess, s3File)
        } else if status == nil || (status.Status == "Failed" && status.Retry == true) {
			reason := "Bag has never been processed."
            if status != nil {
                reason = "Bag failed prior processing attempt, and retry flag is true."
            }
            messageLog.Printf("[INFO] Will process bag %s: %s",s3File.Key.Key, reason)
            filesToProcess = append(filesToProcess, s3File)
        } else if status.Status != "Failed" {
            messageLog.Printf("[INFO] Skipping %s: already processed successfully.", s3File.Key.Key)
        } else if status.Retry == false {
            messageLog.Printf("[INFO] Skipping %s: retry flag is set to false.", s3File.Key.Key)
		}
    }
    return filesToProcess
}


// enqueue adds a batch of items to the nsqd work queue
func enqueue(url string, s3Files []*bagman.S3File) {
    jsonData := make([]string, len(s3Files))
    for i, s3File := range s3Files {
        json, err := json.Marshal(s3File)
        if err != nil {
            messageLog.Printf("[ERROR] Error marshalling s3 file to JSON: %v", err)
        } else {
            jsonData[i] = string(json)
            messageLog.Println("[INFO]", "Put", s3File.Key.Key, "into fetch queue")
            //fmt.Println(s3File.Key.Key)
        }
    }
    batch := strings.Join(jsonData, "\n")
    resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
    if err != nil {
        messageLog.Printf("[ERROR] nsqd returned an error: %v", err)
    }
    if resp == nil {
        msg := "[ERROR] No response from nsqd. Is it running? bucket_reader is quitting."
        messageLog.Printf(msg)
        fmt.Println(msg)
        os.Exit(1)
    } else if  resp.StatusCode != 200 {
        messageLog.Printf("[ERROR] nsqd returned status code %d on last mput", resp.StatusCode)
    }
}
