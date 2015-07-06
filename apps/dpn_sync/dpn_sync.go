package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"os"
)

// dpn_sync copies updated records from the registries
// at other nodes into our local DPN registry.
func main() {
	configPath := "dpn/dpn_config.json"
	configName := parseCommandLine()
	dpnConfig, err := dpn.LoadConfig(configPath, configName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading dpn config file '%s': %v\n",
			configPath, err)
		os.Exit(2)
	}
	dpnSync, err := dpn.NewDPNSync(dpnConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dpn_sync failed to start: %v\n", err)
		os.Exit(3)
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dpn_sync could not get node list: %v\n", err)
		os.Exit(4)
	}
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		// The sync code logs to a file called dpn_sync.log
		// in the log directory specified in the config file.
		syncResult := dpnSync.SyncEverythingFromNode(node)
		printErrors(syncResult, node)
	}

}

func parseCommandLine() (string) {
	config := flag.String("config", "", "DPN config name [dev|test|production]")
	flag.Parse()
	if config == nil || *config == "" {
		printUsage()
		fmt.Fprintln(os.Stderr, "You must specify a DPN config name (dev|test|production)")
		os.Exit(1)
	}
	return *config
}

func printUsage() {
	fmt.Println("Usage: dpn_sync -config=pathToDPNConfigFile")
	fmt.Println("Sync bag and transfer entries from remote nodes to our local registry.")
}

func printErrors(syncResult *dpn.SyncResult, node *dpn.DPNNode) {
	if syncResult.BagSyncError != nil {
		fmt.Fprintf(os.Stderr, "Error synching bags from %s: %v\n",
			node.Namespace, syncResult.BagSyncError)
	}
	if syncResult.ReplicationSyncError != nil {
		fmt.Fprintf(os.Stderr, "Error synching Replication Transfers from %s: %v\n",
			node.Namespace, syncResult.ReplicationSyncError)
	}
	if syncResult.RestoreSyncError != nil {
		fmt.Fprintf(os.Stderr, "Error synching Restore Transfers from %s: %v\n",
			node.Namespace, syncResult.RestoreSyncError)
	}
	if syncResult.TimestampError != nil {
		fmt.Fprintf(os.Stderr, "Error updating LastPullDate for %s: %v\n",
			node.Namespace, syncResult.TimestampError)
	}
}
