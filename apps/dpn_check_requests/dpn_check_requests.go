package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var timestampFile, _ = bagman.RelativeToAbsPath(filepath.Join("bin", "dpnLastRequestCheck.txt"))
var dummyTime, _ = time.Parse(time.RFC3339, "1999-12-31T23:59:59Z")

// dpn_check_requests checks our local DPN node for outstanding
// replication requests and adds them into NSQ.
func main() {
	configPath := parseCommandLine()
	dpnConfig, err := dpn.LoadConfig(configPath)
	if err != nil {
		msg := fmt.Sprintf("Error loading dpn config file '%s': %v\n",
			configPath, err)
		fmt.Fprintf(os.Stderr, msg)
		os.Exit(1)
	}
	configName := "test"
	procUtil := bagman.NewProcessUtil(&configName)
	client, err := dpn.NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		procUtil.MessageLog)
	if err != nil {
		msg := fmt.Sprintf("Error creating DPN REST client: %v", err.Error())
		fmt.Fprintf(os.Stderr, msg)
		procUtil.MessageLog.Fatal(msg)
		os.Exit(2)
	}
	err = queueReplicationRequests(client, procUtil)
	if err != nil {
		procUtil.MessageLog.Error(err.Error())
		fmt.Println(err.Error())
		os.Exit(3)
	}
}

func queueReplicationRequests(client *dpn.DPNRestClient, procUtil *bagman.ProcessUtil) (error) {
	lastCheck := readLastTimestampFile(procUtil)
	nsqUrl := fmt.Sprintf("%s/mput?topic=%s",
		procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNCopyWorker.NsqTopic)
	pageNum := 1
	params := url.Values{}
	params.Set("to_node", "aptrust")
	params.Set("status", "Requested")
	params.Set("after", lastCheck.Format(time.RFC3339))
	params.Set("page", fmt.Sprintf("%d", pageNum))
	for {
		procUtil.MessageLog.Info("Getting replication requests with the following params: " +
			"to_node=%s, status=%s, after=%s, page=%s",
			params.Get("to_node"), params.Get("status"),
			params.Get("after"), params.Get("page"))
		xferList, err := client.DPNReplicationListGet(&params)
		if err != nil {
			return err
		}
		if len(xferList.Results) == 0 {
			//fmt.Println("No replication requests for aptrust")
			return nil
		}
		procUtil.MessageLog.Info("Queuing batch of %d items", len(xferList.Results))
		genericSlice := make([]interface{}, len(xferList.Results))
		for i := range xferList.Results {
			xfer := xferList.Results[i]
			bag, err := client.DPNBagGet(xfer.UUID)
			if err != nil {
				return err
			}
			dpnResult := dpn.NewDPNResult("")
			dpnResult.TransferRequest = xfer
			dpnResult.DPNBag = bag
			dpnResult.Stage = dpn.STAGE_COPY
			genericSlice[i] = dpnResult
		}
		bagman.QueueToNSQ(nsqUrl, genericSlice)
		if err != nil {
			return err
		}
		for _, xfer := range xferList.Results {
			message := fmt.Sprintf("Queued %s - %s", xfer.ReplicationId, xfer.UUID)
			procUtil.MessageLog.Info(message)
			if xfer.UpdatedAt.After(lastCheck) {
				lastCheck = xfer.UpdatedAt
			}
			//fmt.Println(message)
		}
		if xferList.Next == "" {
			message := fmt.Sprintf("No more results after page %d", pageNum)
			procUtil.MessageLog.Info(message)
			//fmt.Println(message)
			break
		} else {
			nextPageNum := pageNum + 1
			params.Set("page", fmt.Sprintf("%d", nextPageNum))
		}

	}
	procUtil.MessageLog.Info("Attempting to write last check timestamp %s to file '%s'",
		lastCheck, timestampFile)
	err := writeLastTimestampFile(lastCheck)
	if err != nil {
		procUtil.MessageLog.Warning("Could not write last check timestamp to '%s': %v",
			timestampFile, err)
	}
	return nil
}

func parseCommandLine() (string) {
	configFile := flag.String("config", "", "DPN config file")
	flag.Parse()
	if configFile == nil || *configFile == "" {
		printUsage()
		fmt.Fprintln(os.Stderr, "You must specify a DPN config file.")
		os.Exit(1)
	}
	return *configFile
}

func printUsage() {
	fmt.Println("Usage: dpn_check_requests -config=pathToDPNConfigFile")
	fmt.Println("Checks the local DPN node for replication requests and adds them to NSQ.")
}

func readLastTimestampFile(procUtil *bagman.ProcessUtil) (time.Time) {
	lastTime := dummyTime
	var f *os.File
	var err error
	if bagman.FileExists(timestampFile) {
		f, err = os.Open(timestampFile)
		if err != nil {
			procUtil.MessageLog.Warning("Cannot read timestamp file '%s'. " +
				"Will load all entries since %s. Error was %v",
				timestampFile, dummyTime, err)
			return dummyTime
		}
	} else {
		procUtil.MessageLog.Info("Timestamp file '%s' does not exist. " +
			"Will load all entries since %s. Error was %v",
			timestampFile, dummyTime, err)
		return dummyTime
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			procUtil.MessageLog.Warning("Error while reading timestamp file '%s'. " +
				"Will load all entries since %s. Error was %v",
				timestampFile, dummyTime, err)
			return dummyTime
		}
		cleanLine := strings.TrimSpace(line)
		if !strings.HasPrefix(cleanLine, "#") {
			lastCheck, err := time.Parse(time.RFC3339, cleanLine)
			if err != nil {
				procUtil.MessageLog.Warning("Error parsing timestamp in file '%s'. " +
					"Will load all entries since %s. Timestamp was '%s'. Error was %v",
					timestampFile, cleanLine, err)
				return dummyTime
			} else {
				lastTime = lastCheck
				break
			}
		}
	}
	return lastTime
}

func writeLastTimestampFile(lastCheck time.Time) (error) {
	fileText := "# Timestamp of last check for outstanding replication requests.\n"
	fileText += "# Used by dpn_check_requests cron job.\n"
	fileText += lastCheck.Format(time.RFC3339) + "\n"
	return ioutil.WriteFile(timestampFile, []byte(fileText), 0644)
}
