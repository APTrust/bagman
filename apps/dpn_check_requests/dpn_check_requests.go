package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"net/url"
	"os"
)

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
	nsqUrl := fmt.Sprintf("%s/mput?topic=%s",
		procUtil.Config.NsqdHttpAddress,
		procUtil.Config.DPNCopyWorker.NsqTopic)
	pageNum := 1
	params := url.Values{}
	params.Set("to_node", "aptrust")
	params.Set("status", "Requested")
	params.Set("page", fmt.Sprintf("%d", pageNum))
	for {
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
			message := fmt.Sprintf("Added %s - %s to NSQ", xfer.ReplicationId, xfer.UUID)
			procUtil.MessageLog.Info(message)
			//fmt.Println(message)
		}
		if xferList.Next == "" {
			message := fmt.Sprintf("No more results after page %d", pageNum)
			procUtil.MessageLog.Info(message)
			//fmt.Println(message)
			return nil
		} else {
			nextPageNum := pageNum + 1
			params.Set("page", fmt.Sprintf("%d", nextPageNum))
		}

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
