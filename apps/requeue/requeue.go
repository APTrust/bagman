// requeue puts a blob of json into the specified queue
// so that we can reprocess it. This is particularly useful
// for items in the trouble queue, whose JSON status is
// written into /mnt/apt/logs/ingest_failures on the ingest
// server and /mnt/apt/logs/replication_failures on the
// restore server. After fixing the bug that caused the failure,
// you can put the item back in the queue, and processing will
// pick up where it left off.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"io/ioutil"
	"os"
	"strings"
	"time"
)


var config string
var queueName string
var jsonFile string
var procUtil *bagman.ProcessUtil
var statusCache map[string]*bagman.ProcessStatus

var configs = []string{ "dev", "test", "demo", "production", }
var queues = []string{
	"bag_delete_topic",
	"dpn_copy_topic",
	"dpn_package_topic",
	"dpn_store_topic",
	"dpn_record_topic",
	"dpn_validation_topic",
	"file_delete_topic",
	"fixity_topic",
	"prepare_topic",
	"record_topic",
	"replication_topic",
	"restore_topic",
	"store_topic",
}

func main() {
	var err error = nil
	parseCommandLine()
	procUtil = bagman.NewProcessUtil(&config)
	err = procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}
	procUtil.MessageLog.Info("requeue started")
	if err != nil {
		procUtil.MessageLog.Info("Initialization failed for requeue: %v", err)
		os.Exit(1)
	}
	result := readResult()
	procUtil.MessageLog.Info("Setting retry to true for %s", result.S3File.Key.Key)
	result.Retry = true

	// statusRecord, err := getStatusRecord(result.S3File)
	// if err != nil {
	// 	procUtil.MessageLog.Fatalf("Error retrieving ProcessedItem from Fluctus: %v", err)
	// }
	// statusRecord.Retry = true

	err = bagman.Enqueue(procUtil.Config.NsqdHttpAddress, queueName, result)
	if err != nil {
		procUtil.MessageLog.Fatalf("Error sending to %s at %s: %v", 
			queueName, procUtil.Config.NsqdHttpAddress, err)
	}
}

type DateParseError struct {
    message   string
}
func (e DateParseError) Error() string { return e.message }

// Reset the retry flag(s) and return the name of the queue this should go into.
func readResult() (*bagman.ProcessResult) {
	file, err := os.Open(jsonFile)
	if err != nil {
		procUtil.MessageLog.Fatal(err)
	}
	defer file.Close()
	jsonBytes, err := ioutil.ReadAll(file)
	if err != nil {
		procUtil.MessageLog.Fatal(err)
	}
	result := bagman.ProcessResult{}
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		procUtil.MessageLog.Fatal(err)
	}
	return &result
}

func sliceContains(slice []string, item string) (bool) {
	for _, value := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// CHANGE: This should retrieve the status record, set retry to true, then save it.
// Use bagman.FluctusClient.SendProcessedItem to update the status record.
func getStatusRecord(s3File *bagman.S3File) (status *bagman.ProcessStatus, err error) {
	bagDate, err := time.Parse(bagman.S3DateFormat, s3File.Key.LastModified)
	if err != nil {
		msg := fmt.Sprintf("Cannot parse S3File mod date '%s'. "+
			"File %s will be re-processed.",
			s3File.Key.LastModified, s3File.Key.Key)
		return nil, DateParseError { message: msg, }
	}
	etag := strings.Replace(s3File.Key.ETag, "\"", "", 2)
	status, err = procUtil.FluctusClient.GetBagStatus(etag, s3File.Key.Key, bagDate)
	return status, err
}

func parseCommandLine() {
	flag.StringVar(&queueName, "q", "", "Queue name")
	flag.StringVar(&config, "config", "", "APTrust config file")
	flag.Parse()
	if !sliceContains(configs, config) {
		printUsage()
		fmt.Println("Option -config is required and must be one of the options above.")
		os.Exit(0)
	}
	if !sliceContains(queues, queueName) {
		printUsage()
		fmt.Println("Option -q is required and must be one of the options above.")
		os.Exit(0)
	}
	if len(os.Args) < 4 {
		printUsage()
		fmt.Printf("Please specify one or more json files to requeue.\n")		
		os.Exit(1)
	} else {
		jsonFile = strings.TrimSpace(os.Args[3])
	}
}

func printUsage() {
	message := `
Usage:

  requeue -config=<config> -q=<queue name> <filename.json>

Sends the data in filename.json back into the queue specified
in the -q option. This will set the retry flag to true before
requeueing, so that the item will be reprocessed.

Depending on the config value, the item will requeued in the
dev, test, demo or production environment.

Options:

  -config <dev|test|demo|production>
  -q      bag_delete_topic
          dpn_copy_topic
          dpn_package_topic
          dpn_store_topic
          dpn_record_topic
          dpn_validation_topic
          file_delete_topic
          fixity_topic
          prepare_topic
          record_topic
          replication_topic
          restore_topic
          store_topic
          
`
	fmt.Println(message)
}