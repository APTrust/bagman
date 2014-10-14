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
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
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

var workReader *bagman.WorkReader

func main() {
	var err error
	workReader, err = workers.InitializeReader()
	workReader.MessageLog.Info("request_reader started")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization failed for request_reader: %v", err)
		os.Exit(1)
	}
	queueAllRestorationItems()
	queueAllDeletionItems()
}

// Find all the Intellectual Objects that need to be restored & add them to
// the NSQ restoration queue.
func queueAllRestorationItems() {
	workReader.MessageLog.Info("Asking Fluctus for objects to restore")
	results, err := workReader.FluctusClient.RestorationItemsGet("")
	if err != nil {
		workReader.MessageLog.Fatalf("Error getting items for restoration: %v", err)
	}
	workReader.MessageLog.Info("Found %d items to restore", len(results))
	queueBatch(results, "restoration")
}

// Find all the Generic Files that need to be deleted & add them to
// the NSQ restoration queue.
func queueAllDeletionItems() {
	workReader.MessageLog.Info("Asking Fluctus for files to delete")
	results, err := workReader.FluctusClient.DeletionItemsGet("")
	if err != nil {
		workReader.MessageLog.Fatalf("Error getting items for deletion: %v", err)
	}
	workReader.MessageLog.Info("Found %d items to delete", len(results))
	queueBatch(results, "delete")
}


// Queues a batch of items for restoration or deletion,
// depending on the url param.
func queueBatch(results []*bagman.ProcessStatus, queueName string) {
	start := 0
	end := min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		workReader.MessageLog.Info("Queuing batch of %d items", len(batch))
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
	url := fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
		workReader.Config.RestoreWorker.NsqTopic)
	if queueName == "delete" {
		url = fmt.Sprintf("%s/mput?topic=%s", workReader.Config.NsqdHttpAddress,
			workReader.Config.FileDeleteWorker.NsqTopic)
	}

	jsonData := make([]string, len(statusList))
	for i, processStatus := range statusList {
		json, err := json.Marshal(processStatus)
		if err != nil {
			workReader.MessageLog.Error("Error marshalling ProcessStatus to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			workReader.MessageLog.Info("Put %s/%s into %s queue",
				processStatus.Institution, processStatus.Name, queueName)
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		workReader.MessageLog.Error("nsqd returned an error: %s", err.Error())
	}
	if resp == nil {
		workReader.MessageLog.Fatal("No response from nsqd. Is it running? restoration_reader is quitting.")
	} else if resp.StatusCode != 200 {
		workReader.MessageLog.Error("nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
