// cleanup_reader periodically checks the processed items list
// in Fluctus for items that should be deleted. It queues those
// items for cleanup in nsqd.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"log"
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
	messageLog    *log.Logger
	fluctusClient *client.Client
	statusCache   map[string]*bagman.ProcessStatus
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
    messageLog = bagman.InitLogger(config)
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
		config.CleanupTopic)
	messageLog.Printf("[INFO] Sending files to clean up to %s \n", url)

	results, err := fluctusClient.GetReviewedItems()
	if err != nil {
		messageLog.Fatal("Error getting reviewed items: %v", err)
	}

	messageLog.Printf("[INFO] Found %d items to clean up\n", len(results))

	start := 0
	end := min(len(results), batchSize)
	for start <= end {
		batch := results[start:end]
		messageLog.Printf("[INFO] Queuing batch of %d items\n", len(batch))
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
func enqueue(url string, results []*bagman.CleanupResult) {
	jsonData := make([]string, len(results))
	for i, result := range results {
		json, err := json.Marshal(result)
		if err != nil {
			messageLog.Printf("[ERROR] Error marshalling cleanup result to JSON: %v", err)
		} else {
			jsonData[i] = string(json)
			messageLog.Println("[INFO]", "Put", result.BagName, "into cleanup queue")
		}
	}
	batch := strings.Join(jsonData, "\n")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(batch)))
	if err != nil {
		messageLog.Printf("[ERROR] nsqd returned an error: %v", err)
	}
	if resp == nil {
		msg := "[ERROR] No response from nsqd. Is it running? cleanup_reader is quitting."
		messageLog.Printf(msg)
		fmt.Println(msg)
		os.Exit(1)
	} else if resp.StatusCode != 200 {
		messageLog.Printf("[ERROR] nsqd returned status code %d on last mput", resp.StatusCode)
	}
}
