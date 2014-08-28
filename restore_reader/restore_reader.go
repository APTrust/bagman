// restore_reader periodically checks the processed items list
// in Fluctus for intellectual objects that users want to restore.
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
		fmt.Fprintf(os.Stderr, "Initialization failed for restore_reader: %v", err)
		os.Exit(1)
	}
	run()
}

func initialize() (err error) {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	messageLog = bagman.InitLogger(config)
	messageLog.Info("Restore reader started")
	fluctusClient, err = client.New(
		config.FluctusURL,
		config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	return err
}

func run() {
	url := fmt.Sprintf("%s/mput?topic=%s", config.NsqdHttpAddress,
		config.RestoreTopic)
	messageLog.Info("Asking Fluctus for objects to restore to %s", url)

	results, err := fluctusClient.RestorationItemsGet("")
	if err != nil {
		messageLog.Fatalf("Error getting items for restoration: %v", err)
	}

	messageLog.Info("Found %d items to restore", len(results))

	start := 0
	end := min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		messageLog.Info("Queuing batch of %d items", len(batch))
		enqueue(url, batch)
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
func enqueue(url string, statusList []*bagman.ProcessStatus) {
	jsonData := make([]string, len(statusList))
	for i, processStatus := range statusList {
		json, err := json.Marshal(processStatus)
		if err != nil {
			messageLog.Error("Error marshalling ProcessStatus to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			messageLog.Info("Put %s/%s into restoration queue",
				processStatus.Institution, processStatus.Name)
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
