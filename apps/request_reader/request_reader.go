// request_reader periodically checks the processed items list
// in Fluctus for the following:
//
// 1. Intellectual objects that users want to restore.
// 2. Generic files that users want to delete.
//
// It queues those items in nsqd.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"github.com/op/go-logging"
	"net/http"
	"os"
	"strings"
	"time"
)

// Queue delete requests in batches of 50.
// Wait X milliseconds between batches.
const (
	batchSize        = 50
	waitMilliseconds = 1000
)

var (
	config        bagman.Config
	messageLog    *logging.Logger
	fluctusClient *client.Client
	statusCache   map[string]*bagman.ProcessStatus
)

func main() {
	err := initialize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for request_reader: %v", err)
		os.Exit(1)
	}
	queueAllRestorationItems()
	queueAllDeletionItems()
}

func initialize() (err error) {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	messageLog = bagman.InitLogger(config)
	bagman.LoadCustomEnvOrDie(customEnvFile, messageLog)
	messageLog.Info("Request reader started")
	fluctusClient, err = client.New(
		config.FluctusURL,
		config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	return err
}

// Find all the Intellectual Objects that need to be restored & add them to
// the NSQ restoration queue.
func queueAllRestorationItems() {
	messageLog.Info("Asking Fluctus for objects to restore")
	results, err := fluctusClient.RestorationItemsGet("")
	if err != nil {
		messageLog.Fatalf("Error getting items for restoration: %v", err)
	}
	messageLog.Info("Found %d items to restore", len(results))
	queueBatch(results, "restoration")
}

// Find all the Generic Files that need to be deleted & add them to
// the NSQ restoration queue.
func queueAllDeletionItems() {
	messageLog.Info("Asking Fluctus for files to delete")
	results, err := fluctusClient.DeletionItemsGet("")
	if err != nil {
		messageLog.Fatalf("Error getting items for deletion: %v", err)
	}
	messageLog.Info("Found %d items to delete", len(results))
	queueBatch(results, "delete")
}


// Queues a batch of items for restoration or deletion,
// depending on the url param.
func queueBatch(results []*bagman.ProcessStatus, queueName string) {
	start := 0
	end := min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		messageLog.Info("Queuing batch of %d items", len(batch))
	enqueue(batch, queueName)
		start = end + 1
		if start < len(results) {
			end = min(len(results), start+batchSize)
		}
		time.Sleep(time.Millisecond * waitMilliseconds)
	}
}

// min returns the minimum of x or y. The Math package has this function
// but you have to cast to floats.
func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// enqueue adds a batch of items to the nsqd work queue
func enqueue(statusList []*bagman.ProcessStatus, queueName string) {
	url := fmt.Sprintf("%s/mput?topic=%s", config.NsqdHttpAddress,
		config.RestoreTopic)
	if queueName == "delete" {
		url = fmt.Sprintf("%s/mput?topic=%s", config.NsqdHttpAddress,
			config.DeleteTopic)
	}

	jsonData := make([]string, len(statusList))
	for i, processStatus := range statusList {
		json, err := json.Marshal(processStatus)
		if err != nil {
			messageLog.Error("Error marshalling ProcessStatus to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			messageLog.Info("Put %s/%s into %s queue",
				processStatus.Institution, processStatus.Name, queueName)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		messageLog.Error("nsqd returned an error: %s", err.Error())
	}
	if resp == nil {
		messageLog.Fatal("No response from nsqd. Is it running? restoration_reader is quitting.")
	} else if resp.StatusCode != 200 {
		messageLog.Error("nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
