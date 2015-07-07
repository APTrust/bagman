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

// SyncResult describes the result of an operation where we pull
// info about all updated bags, replication requests and restore
// requests from a remote node and copy that data into our own
// local DPN registry.
type SyncResult struct {
	// Node is the node we are pulling information from.
	RemoteNode            *DPNNode
	// Bags is a list of bags successfully synched.
	Bags                  []*DPNBag
	// ReplicationTransfers successfully synched.
	ReplicationTransfers  []*DPNReplicationTransfer
	// RestoreTransfers successfully synched.
	RestoreTransfers      []*DPNRestoreTransfer
	// BagSyncError contains the error (if any) that occurred
	// during the bag sync process. The first error will stop
	// the synching of all subsquent bags.
	BagSyncError          error
	// ReplicationSyncError contains the error (if any) that occurred
	// during the synching of Replication Transfers. The first error
	// will stop the synching of all subsquent replication requests.
	ReplicationSyncError  error
	// RestoreSyncError contains the error (if any) that occurred
	// during the synching of Restore Transfers. The first error
	// will stop the synching of all subsquent restore requests.
	RestoreSyncError      error
	// TimestampError contains the error (if any) that occurred when
	// trying to update the LastPullDate timestamp for the node.
	TimestampError        error
}

func (syncResult *SyncResult) HasSyncErrors() (bool) {
	return (syncResult.BagSyncError != nil ||
		syncResult.ReplicationSyncError != nil ||
		syncResult.RestoreSyncError != nil)
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
	remoteClients, err := GetRemoteClients(localClient, config, logger)
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

// Sync all bags, replication requests and restore requests from
// the specified remote node. Note that this is a pull-only sync.
// We are not writing any data to other nodes, just reading what
// they have and updating our own registry with their info.
func (dpnSync *DPNSync) SyncEverythingFromNode(remoteNode *DPNNode) (*SyncResult) {
	nextTimeStamp := time.Now().UTC()
	syncResult := &SyncResult {
		RemoteNode: remoteNode,
	}

	bags, err := dpnSync.SyncBags(remoteNode)
	syncResult.Bags = bags
	syncResult.BagSyncError = err

	replXfers, err := dpnSync.SyncReplicationRequests(remoteNode)
	syncResult.ReplicationTransfers = replXfers
	syncResult.ReplicationSyncError = err

	restoreXfers, err := dpnSync.SyncRestoreRequests(remoteNode)
	syncResult.RestoreTransfers = restoreXfers
	syncResult.RestoreSyncError = err


	if !syncResult.HasSyncErrors() {
		updatedNode, err := dpnSync.UpdateLastPullDate(remoteNode, nextTimeStamp)
		syncResult.RemoteNode = updatedNode
		syncResult.TimestampError = err
	} else {
		syncResult.TimestampError = fmt.Errorf(
			"LastPullDate was not updated because of errors during the synchronization process.")
	}

	return syncResult
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
	pageNumber := 1
	bagsUpdated := make([]*DPNBag, 0)
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping bag sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return bagsUpdated, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of bags from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getBags(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return bagsUpdated, err
		}
		dpnSync.Logger.Debug("Got %d bags from %s", len(result.Results), remoteNode.Namespace)
		updated, err := dpnSync.syncBags(result.Results)
		if err != nil {
			return bagsUpdated, err
		}
		bagsUpdated = append(bagsUpdated, updated...)
		if result.Next == nil || *result.Next == "" {
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
		existingBag, _ := dpnSync.LocalClient.DPNBagGet(bag.UUID)
		var err error
		var updatedBag *DPNBag
		if existingBag != nil {
			dpnSync.Logger.Debug("Bag %s exists... updating", bag.UUID)
			updatedBag, err = dpnSync.LocalClient.DPNBagUpdate(bag)
		} else {
			dpnSync.Logger.Debug("Bag %s not in local registry... creating", bag.UUID)
			updatedBag, err = dpnSync.LocalClient.DPNBagCreate(bag)
		}
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
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNBagListGet(&params)
}

func (dpnSync *DPNSync) SyncReplicationRequests(remoteNode *DPNNode) ([]*DPNReplicationTransfer, error) {
	xfersUpdated := make([]*DPNReplicationTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping replication sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersUpdated, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of replication requests from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getReplicationRequests(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return xfersUpdated, err
		}
		dpnSync.Logger.Debug("Got %d replication requests from %s", len(result.Results), remoteNode.Namespace)
		updated, err := dpnSync.syncReplicationRequests(result.Results)
		if err != nil {
			return xfersUpdated, err
		}
		xfersUpdated = append(xfersUpdated, updated...)
		if result.Next == nil || *result.Next == "" {
			dpnSync.Logger.Debug("No more replication requests to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Updated %d replication requests in local registry", len(xfersUpdated))
	return xfersUpdated, nil
}

func (dpnSync *DPNSync) syncReplicationRequests(xfers []*DPNReplicationTransfer) ([]*DPNReplicationTransfer, error) {
	xfersUpdated := make([]*DPNReplicationTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Logger.Debug("Updating transfer %s in local registry", xfer.ReplicationId)
		existingXfer, _ := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
		var err error
		var updatedXfer *DPNReplicationTransfer
		if existingXfer != nil {
			dpnSync.Logger.Debug("Replication request %s exists... updating", xfer.ReplicationId)
			updatedXfer, err = dpnSync.LocalClient.ReplicationTransferUpdate(xfer)
		} else {
			dpnSync.Logger.Debug("Replication request %s not in local registry... creating", xfer.ReplicationId)
			updatedXfer, err = dpnSync.LocalClient.ReplicationTransferCreate(xfer)
		}
		if err != nil {
			dpnSync.Logger.Debug("Oops! Replication request %s: %v", xfer.ReplicationId, err)
			return xfersUpdated, err
		}
		xfersUpdated = append(xfersUpdated, updatedXfer)
	}
	return xfersUpdated, nil
}

func (dpnSync *DPNSync) getReplicationRequests(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*ReplicationListResult, error) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the from_node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("from_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNReplicationListGet(&params)
}


func (dpnSync *DPNSync) SyncRestoreRequests(remoteNode *DPNNode) ([]*DPNRestoreTransfer, error) {
	xfersUpdated := make([]*DPNRestoreTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping restore sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersUpdated, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of restore requests from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getRestoreRequests(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return xfersUpdated, err
		}
		dpnSync.Logger.Debug("Got %d restore requests from %s", len(result.Results), remoteNode.Namespace)
		updated, err := dpnSync.syncRestoreRequests(result.Results)
		if err != nil {
			return xfersUpdated, err
		}
		xfersUpdated = append(xfersUpdated, updated...)
		if result.Next == nil || *result.Next == "" {
			dpnSync.Logger.Debug("No more restore requests to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Updated %d restore requests in local registry", len(xfersUpdated))
	return xfersUpdated, nil
}

func (dpnSync *DPNSync) syncRestoreRequests(xfers []*DPNRestoreTransfer) ([]*DPNRestoreTransfer, error) {
	xfersUpdated := make([]*DPNRestoreTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Logger.Debug("Updating transfer %s in local registry", xfer.RestoreId)
		existingXfer, _ := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
		var err error
		var updatedXfer *DPNRestoreTransfer
		if existingXfer != nil {
			dpnSync.Logger.Debug("Restore request %s exists... updating", xfer.RestoreId)
			updatedXfer, err = dpnSync.LocalClient.RestoreTransferUpdate(xfer)
		} else {
			dpnSync.Logger.Debug("Restore request %s not in local registry... creating", xfer.RestoreId)
			updatedXfer, err = dpnSync.LocalClient.RestoreTransferCreate(xfer)
		}
		if err != nil {
			dpnSync.Logger.Debug("Oops! Restore request %s: %v", xfer.RestoreId, err)
			return xfersUpdated, err
		}
		xfersUpdated = append(xfersUpdated, updatedXfer)
	}
	return xfersUpdated, nil
}

func (dpnSync *DPNSync) getRestoreRequests(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*RestoreListResult, error) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the to_node.
	// E.g. We ask TDR for restore requests going TO TDR.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("to_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNRestoreListGet(&params)
}
