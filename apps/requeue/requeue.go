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
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"io/ioutil"
	"os"
	"strings"
	"time"
)


var config string
var queueName string
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

type DateParseError struct {
    message   string
}

func (e DateParseError) Error() string { return e.message }


func main() {
	jsonFiles := parseCommandLine()
	procUtil = bagman.NewProcessUtil(&config, "aptrust")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}
	procUtil.MessageLog.Info("requeue started")
	if err != nil {
		procUtil.MessageLog.Info("Initialization failed for requeue: %v", err)
		os.Exit(1)
	}
	if confirm(jsonFiles) == false {
		procUtil.MessageLog.Info("Nothing requeued. User cancelled request.")
		fmt.Println("OK. Bye.")
		return
	}
	succeeded := 0
	failed := 0
	for _, jsonFile := range(jsonFiles) {
		err = nil
		if strings.HasPrefix(queueName, "dpn_") {
			err = requeueDPNFile(jsonFile)
		} else {
			err = requeueFile(jsonFile)
		}
		if err != nil {
			procUtil.MessageLog.Error(err.Error())
			failed++
		} else {
			succeeded++
		}
	}
	message := fmt.Sprintf("%d Succeeded, %d Failed", succeeded, failed)
	fmt.Println(message)
	procUtil.MessageLog.Info(message)
}

func confirm(jsonFiles []string) bool {
	for _, f := range(jsonFiles) {
		fmt.Println(f)
	}
	reader := bufio.NewReader(os.Stdin)
	response := ""
	for response == "" {
		fmt.Printf("Requeue %d files? [y/N]: ", len(jsonFiles))
		response, _ = reader.ReadString('\n')
		if len(response) > 0 {
			if response[0] == 'y' || response[0] == 'Y' {
				return true
			}
		}
	}
	return false
}

// TODO: Merge requeueFile and requeueDPNFile into one.
func requeueFile(jsonFile string) (error) {
	result, err := readResult(jsonFile)
	if err != nil {
		return err
	}
	procUtil.MessageLog.Info("Setting retry to true for %s", result.S3File.Key.Key)
	result.Retry = true

	result.ErrorMessage = ""
	if result.FedoraResult != nil {
		result.FedoraResult.ErrorMessage = ""
	}

	if result.FedoraResult == nil && queueName == "record_topic" {
		return fmt.Errorf("File %s has no FedoraResult, " +
			"so it's not going into the record_topic.", jsonFile)
	}

	err = bagman.Enqueue(procUtil.Config.NsqdHttpAddress, queueName, result)
	return fmt.Errorf("Error sending to %s at %s: %v",
		queueName, procUtil.Config.NsqdHttpAddress, err)
}

// TODO: Merge readResult and readDPNResult into one.
func readResult(jsonFile string) (*bagman.ProcessResult, error) {
	file, err := os.Open(jsonFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	result := bagman.ProcessResult{}
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func requeueDPNFile(jsonFile string) (error) {
	result, err := readDPNResult(jsonFile)
	if err != nil {
		return err
	}
	result.ErrorMessage = ""
	err = bagman.Enqueue(procUtil.Config.NsqdHttpAddress, queueName, result)
	return fmt.Errorf("Error sending to %s at %s: %v",
		queueName, procUtil.Config.NsqdHttpAddress, err)
}

func readDPNResult(jsonFile string) (*dpn.DPNResult, error) {
	file, err := os.Open(jsonFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	result := dpn.DPNResult{}
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
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

func parseCommandLine() ([]string) {
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
	}
	return os.Args[3:]
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
