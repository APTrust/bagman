package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/op/go-logging"
	"net/url"
	"time"
)

type DPNSync struct {
	LocalClient    *DPNRestClient
	RemoteClients  map[string]*DPNRestClient
	Logger         *logging.Logger
	Config         *DPNConfig
}

func NewDPNSync(config *DPNConfig) (*DPNSync, error) {
	logger := initLogger(config)
	localClient, err := NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := initRemoteClients(localClient, config, logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating remote DPN REST client: %v", err)
	}
	sync := DPNSync{
		LocalClient: localClient,
		RemoteClients: remoteClients,
		Logger: logger,
		Config: config,
	}
	return &sync, nil
}

func initLogger(config *DPNConfig) (*logging.Logger) {
	// bagman has a nice InitLogger function, but it
	// needs a bagman.Config object, which does a lot
	// of handy internal work. Create a throw-away
	// bagman config so we can get our logger.
	bagmanConfig := bagman.Config{
		LogDirectory: config.LogDirectory,
		LogLevel: config.LogLevel,
		LogToStderr: config.LogToStderr,
	}
	return bagman.InitLogger(bagmanConfig)
}

func initRemoteClients(localClient *DPNRestClient, config *DPNConfig, logger *logging.Logger) (map[string]*DPNRestClient, error) {
	remoteClients := make(map[string]*DPNRestClient)
	for namespace, _ := range config.RemoteNodeTokens {
		remoteClient, err := localClient.GetRemoteClient(namespace, config, logger)
		if err != nil {
			return nil, fmt.Errorf("Error creating remote client for node %s: %v", namespace, err)
		}
		remoteClients[namespace] = remoteClient
	}
	return remoteClients, nil
}

// Returns a list of all the nodes that our node knows about.
func (dpnSync *DPNSync) GetAllNodes()([]*DPNNode, error) {
	result, err := dpnSync.LocalClient.DPNNodeListGet(nil)
	if err != nil {
		return nil, err
	}
	return result.Results, nil
}

func (dpnSync *DPNSync) UpdateLastPullDate(node *DPNNode, lastPullDate time.Time) (*DPNNode, error) {
	dpnSync.Logger.Debug("Setting last pull date on %s to %s", node.Namespace, lastPullDate)
	node.LastPullDate = lastPullDate  // Don't set this until you're ready to send!
	return dpnSync.LocalClient.DPNNodeUpdate(node)
}

// Syncs bags from the specified node to our own local DPN registry
// if the bags match these critieria:
//
// 1. The node we are querying is the admin node for the bag.
// 2. The bag was updated since the last time we queried the node.
//
// Returns a list of the bags that were successfully updated.
// Even on error, this may still return a list with whatever bags
// were updated before the error occurred.
func (dpnSync *DPNSync) SyncBags(remoteNode *DPNNode) ([]*DPNBag, error) {
	nextTimeStamp := time.Now().UTC()
	pageNumber := 1
	bagsUpdated := make([]*DPNBag, 0)
	defer dpnSync.UpdateLastPullDate(remoteNode, nextTimeStamp)

	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	for {
		dpnSync.Logger.Debug("Getting page %d of bags from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getBags(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return bagsUpdated, err
		}
		dpnSync.Logger.Debug("Got %d bags from %s", result.Count, remoteNode.Namespace)
		updated, err := dpnSync.syncBags(result.Results)
		if err != nil {
			return bagsUpdated, err
		}
		bagsUpdated = append(bagsUpdated, updated...)
		if result.Next == "" {
			dpnSync.Logger.Debug("No more bags to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Updated %d bags in local registry", len(bagsUpdated))
	return bagsUpdated, nil
}

func (dpnSync *DPNSync) syncBags(bags []*DPNBag) ([]*DPNBag, error) {
	bagsUpdated := make([]*DPNBag, 0)
	for _, bag := range(bags) {
		dpnSync.Logger.Debug("Updating bag %s in local registry", bag.UUID)
		updatedBag, err := dpnSync.LocalClient.DPNBagUpdate(bag)
		if err != nil {
			dpnSync.Logger.Debug("Oops! Bag %s: %v", bag.UUID, err)
			return bagsUpdated, err
		}
		bagsUpdated = append(bagsUpdated, updatedBag)
	}
	return bagsUpdated, nil
}

func (dpnSync *DPNSync) getBags(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*BagListResult, error) {
	// We want to get all bags updated since the last time we pulled
	// from this node, and only those bags for which the node we're
	// querying is the admin node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("admin_node", remoteNode.Namespace)
	return remoteClient.DPNBagListGet(&params)
}

//
// Get all nodes
// Get bags
// Sync bags to local
// Get replication transfers
// Sync replication to local
// Get restore transfers
// Sync restore to local
// Update last sync time for each node
